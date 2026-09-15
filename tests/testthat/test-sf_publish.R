# The write path: collision detection, PUT response validation, and the
# full pin_store() sequence.
#
# Each pin_store() test mocks only sf_stage_cmd(), the one SQL dispatch
# point; every stage helper and validation function runs for real against
# the mock transport. Request counts come from grepping the recorded SQL.

V <- "20240101T000002Z-bbb"

# Metadata of the shape pins produces, for a two-payload write.
sf_publish_meta <- function(file = c("cars.rds", "wheels.rds"),
                            created = "20240102T000000Z",
                            pin_hash = "abcdef0123456789") {
  list(
    api_version = 1L, file = file, file_size = 12L,
    created = created, pin_hash = pin_hash, type = "rds"
  )
}

# Real local files for an upload set, in the order named.
sf_publish_paths <- function(files = c("cars.rds", "wheels.rds"),
                             envir = parent.frame()) {
  dir <- withr::local_tempdir(.local_envir = envir)
  paths <- file.path(dir, files)
  for (i in seq_along(paths)) {
    writeLines(letters[[i]], paths[[i]])
  }
  paths
}

# ---- sf_check_version_collision ----------------------------------------

test_that("sf_check_version_collision reads the raw listing, not the index", {
  # The index hides payload-only directories, so only the raw listing can
  # see a half-finished write into the version we are about to use.
  cases <- list(
    list(
      name = "payload-only version directory",
      listed = paste0("cars/", V, "/cars.rds"), prefix = "", collides = TRUE
    ),
    list(
      name = "data.txt already present",
      listed = paste0("cars/", V, "/data.txt"), prefix = "", collides = TRUE
    ),
    list(
      # The exact-match arm: sf_stage_list() keeps a row naming the
      # version directory itself, with no file under it.
      name = "the version directory itself",
      listed = paste0("cars/", V), prefix = "", collides = TRUE
    ),
    list(
      # The board path must not blind the check: without the prefix the
      # target is never reached.
      name = "under a board path",
      listed = paste0("team-data/cars/", V, "/cars.rds"),
      prefix = "team-data", collides = TRUE
    ),
    list(
      name = "sibling pin sharing a prefix",
      listed = paste0("cars_extra/", V, "/data.txt"), prefix = "",
      collides = FALSE
    ),
    list(
      name = "different version of the same pin",
      listed = "cars/20240101T000001Z-aaa/data.txt", prefix = "",
      collides = FALSE
    ),
    list(
      name = "zero-row listing",
      listed = character(), prefix = "", collides = FALSE
    )
  )
  for (case in cases) {
    listing <- sf_fixture_listing(case$listed)
    if (case$collides) {
      cond <- expect_error(
        pinsExtras:::sf_check_version_collision(
          listing, "cars", V, prefix = case$prefix
        ),
        class = "pinsExtras_version_collision",
        info = case$name
      )
      msg <- cli::ansi_strip(conditionMessage(cond))
      expect_true(grepl(V, msg, fixed = TRUE), info = case$name)
      expect_true(grepl("of pin", msg, fixed = TRUE), info = case$name)
    } else {
      expect_invisible(
        pinsExtras:::sf_check_version_collision(
          listing, "cars", V, prefix = case$prefix
        ),
        label = case$name
      )
    }
  }
})

# ---- PUT response validation (U8) --------------------------------------
#
# sf_check_put_result() and sf_check_meta_put_result() run the same six
# checks over the same response shape, but split their aborts across two
# classes: for data.txt, the publication marker, an UNINTERPRETABLE
# response is publication uncertainty, because whether the file landed
# decides whether the version is visible at all. That split is what lets
# pin_store() skip cleanup when it cannot tell.

test_that("both PUT validators accept an interpretable UPLOADED response", {
  cases <- list(
    list(name = "UPLOADED", response = sf_fixture_put_response()),
    list(
      name = "lower-case status",
      response = sf_fixture_put_response(status = "uploaded")
    ),
    list(
      name = "upper-case column names",
      response = sf_fixture_put_response(casing = "upper")
    )
  )
  validators <- list(
    put = pinsExtras:::sf_check_put_result,
    meta = pinsExtras:::sf_check_meta_put_result
  )
  for (case in cases) {
    for (which in names(validators)) {
      expect_true(
        validators[[which]](case$response, "cars.rds", "cars/v/cars.rds"),
        info = paste(which, case$name)
      )
    }
  }
})

test_that("the PUT validators split uninterpretable from failed responses", {
  cases <- list(
    # --- uninterpretable: the metadata variant calls these uncertain ---
    list(
      name = "NULL response", response = NULL,
      put = "no upload result", meta_uncertain = TRUE
    ),
    list(
      name = "zero rows", response = sf_fixture_put_response(n = 0L),
      put = "no upload result", meta_uncertain = TRUE
    ),
    list(
      name = "two rows", response = sf_fixture_put_response(n = 2L),
      put = "2 results", meta_uncertain = TRUE
    ),
    list(
      name = "no status column",
      response = sf_fixture_put_response(drop = "status"),
      put = "could not be interpreted", meta_uncertain = TRUE
    ),
    list(
      # A response carrying target_size but no target must not slip past
      # a partial-matching `$` lookup.
      name = "target_size but no target",
      response = sf_fixture_put_response(drop = "target"),
      put = "could not be interpreted", meta_uncertain = TRUE
    ),
    list(
      name = "NA status",
      response = sf_fixture_put_response(status = NA_character_),
      put = "could not be interpreted", meta_uncertain = TRUE
    ),
    list(
      name = "NA target",
      response = sf_fixture_put_response(target = NA_character_),
      put = "could not be interpreted", meta_uncertain = TRUE
    ),
    # --- interpretable failures: both variants call these upload_failed ---
    list(
      name = "SKIPPED",
      response = sf_fixture_put_response(
        status = "SKIPPED", message = "File already exists"
      ),
      put = "skipped", meta = "skipped", meta_uncertain = FALSE
    ),
    list(
      name = "some other status",
      response = sf_fixture_put_response(status = "ERROR", message = "boom"),
      put = "reported status", meta = "reported status",
      meta_uncertain = FALSE
    ),
    list(
      name = "wrong target name",
      response = sf_fixture_put_response(target = "@~/cars/v/other.rds"),
      put = "instead", meta = "instead", meta_uncertain = FALSE
    )
  )
  for (case in cases) {
    expect_error(
      pinsExtras:::sf_check_put_result(
        case$response, "cars.rds", "cars/v/cars.rds"
      ),
      class = "pinsExtras_upload_failed",
      regexp = case$put,
      fixed = TRUE,
      info = paste("sf_check_put_result:", case$name)
    )
    meta_label <- paste("sf_check_meta_put_result:", case$name)
    if (case$meta_uncertain) {
      # No regexp: every uncertain row shares one message, and the class
      # is the contract.
      expect_error(
        pinsExtras:::sf_check_meta_put_result(
          case$response, "cars.rds", "cars/v/cars.rds"
        ),
        class = "pinsExtras_publication_uncertain",
        info = meta_label
      )
    } else {
      # An interpretable failure reuses sf_check_put_result()'s wording
      # verbatim, so the regexp is asserted here too.
      expect_error(
        pinsExtras:::sf_check_meta_put_result(
          case$response, "cars.rds", "cars/v/cars.rds"
        ),
        class = "pinsExtras_upload_failed",
        regexp = case$meta,
        fixed = TRUE,
        info = meta_label
      )
    }
  }
})

# ---- sf_stage_upload end to end through the transport -------------------

test_that("the PUT verb never silently replaces what is already there", {
  cases <- list(
    list(
      name = "default upload",
      run = function(board, src) {
        pinsExtras:::sf_stage_upload(board, src, "cars/v/cars.rds")
      },
      file = "cars.rds",
      sql = "'@~/cars/v' AUTO_COMPRESS=FALSE OVERWRITE=FALSE"
    ),
    list(
      name = "explicit overwrite",
      run = function(board, src) {
        pinsExtras:::sf_stage_upload(
          board, src, "cars/v/cars.rds", overwrite = TRUE
        )
      },
      file = "cars.rds",
      sql = "'@~/cars/v' AUTO_COMPRESS=FALSE OVERWRITE=TRUE"
    ),
    list(
      # sf_stage_upload_meta() has no overwrite parameter at all: the
      # publication marker is fixed to OVERWRITE=FALSE.
      name = "metadata upload",
      run = function(board, src) {
        pinsExtras:::sf_stage_upload_meta(board, src, "cars/v/data.txt")
      },
      file = "data.txt",
      sql = "'@~/cars/v' AUTO_COMPRESS=FALSE OVERWRITE=FALSE"
    ),
    list(
      # fs::path_dir() gives "." for a bare name, which maps to the board
      # root, whose location carries no trailing slash.
      name = "board root",
      run = function(board, src) {
        pinsExtras:::sf_stage_upload(board, src, "_pins.yaml")
      },
      file = "_pins.yaml",
      sql = "'@~' AUTO_COMPRESS=FALSE OVERWRITE=FALSE"
    )
  )
  for (case in cases) {
    board <- sf_mock_board()
    d <- withr::local_tempdir()
    src <- fs::path(d, case$file)
    writeLines("x", src)
    rec <- sf_mock_bind()

    expect_true(case$run(board, src), info = case$name)
    expect_identical(length(rec$calls), 1L, info = case$name)
    expect_match(rec$calls[[1]], case$sql, fixed = TRUE, info = case$name)
  }
})

test_that("sf_stage_upload renames a mismatched source basename", {
  # Every metadata PUT relies on this: the source is a random temp file
  # and the destination must still be data.txt.
  board <- sf_mock_board()
  d <- withr::local_tempdir()
  src <- fs::path(d, "payload.rds")
  writeLines("x", src)
  rec <- sf_mock_bind()

  expect_true(pinsExtras:::sf_stage_upload(board, src, "cars/v/cars.rds"))
  expect_match(rec$calls[[1]], "/cars.rds'", fixed = TRUE)
  expect_false(grepl("payload.rds", rec$calls[[1]], fixed = TRUE))
  expect_match(
    rec$calls[[1]], "AUTO_COMPRESS=FALSE OVERWRITE=FALSE", fixed = TRUE
  )
})

test_that("sf_stage_upload routes its response through the validator", {
  board <- sf_mock_board()
  d <- withr::local_tempdir()
  src <- fs::path(d, "cars.rds")
  writeLines("x", src)
  rec <- sf_mock_bind(
    put = sf_fixture_put_response(
      status = "SKIPPED", message = "File already exists"
    )
  )

  expect_error(
    pinsExtras:::sf_stage_upload(board, src, "cars/v/cars.rds"),
    class = "pinsExtras_upload_failed",
    regexp = "skipped",
    fixed = TRUE
  )
})

# ======================================================================
# pin_store.pins_board_sf_stage -- the full write sequence (U11-store)
# ======================================================================

test_that("a versioned write costs one LIST and one PUT per file", {
  # A board path must not cost an extra request, and an already-published
  # version must not either.
  shapes <- list(
    list(name = "user stage", args = list(),
         sql = "LIST '@~/cars/'"),
    list(name = "named stage",
         args = list(path = "team-data", stage = "@mystage"),
         sql = "LIST '@mystage/team-data/cars/'")
  )
  fixtures <- list(
    list(name = "brand-new pin", listed = character()),
    list(name = "already-published pin",
         listed = "cars/20240101T000001Z-aaa/data.txt")
  )
  paths <- sf_publish_paths()
  withr::local_options(pins.quiet = TRUE)

  for (shape in shapes) {
    for (fixture in fixtures) {
      board <- do.call(sf_mock_board, shape$args)
      rec <- sf_mock_bind(
        list = sf_fixture_listing(fixture$listed, board = board)
      )
      label <- paste(fixture$name, "on the", shape$name)

      out <- pins::pin_store(
        board, "cars", paths, sf_publish_meta(), versioned = TRUE, x = NULL
      )

      expect_identical(out, "cars", info = label)
      expect_identical(
        grep("^LIST ", rec$calls, value = TRUE), shape$sql, info = label
      )
      # two payloads plus the metadata marker
      expect_identical(length(grep("^PUT ", rec$calls)), 3L, info = label)
      expect_identical(length(grep("^REMOVE ", rec$calls)), 0L, info = label)
    }
  }
})

test_that("bad local input aborts the write before a single command", {
  paths <- sf_publish_paths()
  withr::local_options(pins.quiet = TRUE)
  cases <- list(
    list(
      name = "reserved pin name",
      pin = "data.txt", paths = paths, meta = sf_publish_meta(),
      says = "Can't pin file called", class = NULL
    ),
    list(
      name = "upload set disagrees with the metadata",
      pin = "cars", paths = paths[[1]], meta = sf_publish_meta(),
      says = "Metadata lists", class = "pinsExtras_invalid_upload_set"
    )
  )
  for (case in cases) {
    board <- sf_mock_board()
    rec <- sf_mock_bind(list = sf_fixture_listing())
    cond <- expect_error(
      pinsExtras:::pin_store.pins_board_sf_stage(
        board, case$pin, case$paths, case$meta, versioned = TRUE, x = NULL
      ),
      class = case$class,
      info = case$name
    )
    expect_true(
      grepl(case$says, cli::ansi_strip(conditionMessage(cond)), fixed = TRUE),
      info = case$name
    )
    expect_identical(length(rec$calls), 0L, info = case$name)
  }
})

test_that("a preflight failure aborts after one LIST and before any PUT", {
  paths <- sf_publish_paths()
  meta <- sf_publish_meta()
  v <- paste0(meta$created, "-", substr(meta$pin_hash, 1, 5))
  withr::local_options(pins.quiet = TRUE)

  cases <- list(
    list(
      name = "the new version is already published",
      listed = paste0("cars/", v, "/data.txt"),
      versioned = TRUE,
      regexp = "the most recent version", class = NULL
    ),
    list(
      name = "a payload-only directory is in the way",
      listed = paste0("cars/", v, "/cars.rds"),
      versioned = TRUE,
      regexp = NULL, class = "pinsExtras_version_collision"
    ),
    list(
      name = "an unversioned write against a versioned pin",
      listed = c(
        "cars/20240101T000001Z-aaa/data.txt",
        "cars/20240101T000002Z-bbb/data.txt"
      ),
      versioned = FALSE,
      regexp = NULL, class = "pins_pin_versioned"
    )
  )
  for (case in cases) {
    board <- sf_mock_board(versioned = TRUE)
    rec <- sf_mock_bind(list = sf_fixture_listing(case$listed))
    expect_error(
      pinsExtras:::pin_store.pins_board_sf_stage(
        board, "cars", paths, meta, versioned = case$versioned, x = NULL
      ),
      regexp = case$regexp,
      class = case$class,
      info = case$name
    )
    expect_identical(length(grep("^LIST ", rec$calls)), 1L, info = case$name)
    expect_identical(length(grep("^PUT ", rec$calls)), 0L, info = case$name)
  }
})

test_that("a failing payload PUT stops the sequence where it is", {
  # The remaining payload and the metadata marker are never sent, so the
  # version stays unpublished, and nothing is removed.
  board <- sf_mock_board()
  paths <- sf_publish_paths()
  rec <- sf_mock_bind(
    put = function(sql, calls) {
      if (sum(grepl("^PUT ", calls)) == 2L) {
        sf_fixture_put_response(
          target = paste0(sf_mock_sql_args(sql)[[2]], "/wheels.rds"),
          status = "ERROR", message = "boom"
        )
      } else {
        sf_mock_put_response(sql)
      }
    }
  )
  withr::local_options(pins.quiet = TRUE)

  expect_error(
    pinsExtras:::pin_store.pins_board_sf_stage(
      board, "cars", paths, sf_publish_meta(), versioned = TRUE, x = NULL
    ),
    class = "pinsExtras_upload_failed"
  )
  expect_length(grep("^LIST ", rec$calls), 1L)
  expect_length(grep("^PUT ", rec$calls), 2L)
  expect_length(grep("^REMOVE ", rec$calls), 0L)
})

test_that("an explicit metadata PUT failure is a plain upload failure", {
  # The interpretable half of the class split: a clear failure must not
  # surface as publication uncertainty.
  board <- sf_mock_board()
  paths <- sf_publish_paths()
  rec <- sf_mock_bind(
    put = function(sql) {
      if (grepl("data.txt", sql, fixed = TRUE)) {
        sf_fixture_put_response(
          target = paste0(sf_mock_sql_args(sql)[[2]], "/data.txt"),
          status = "ERROR", message = "boom"
        )
      } else {
        sf_mock_put_response(sql)
      }
    }
  )
  withr::local_options(pins.quiet = TRUE)

  expect_error(
    pinsExtras:::pin_store.pins_board_sf_stage(
      board, "cars", paths, sf_publish_meta(), versioned = TRUE, x = NULL
    ),
    class = "pinsExtras_upload_failed"
  )
  expect_length(grep("^LIST ", rec$calls), 1L)
  expect_length(grep("^PUT ", rec$calls), 3L)
  expect_length(grep("^REMOVE ", rec$calls), 0L)
})

test_that("an uninterpretable metadata PUT deletes nothing", {
  # The other half of the split, and the whole reason the uncertain class
  # exists: when we cannot tell whether data.txt landed, the previous
  # version must survive. On the create path zero REMOVE is vacuous, so
  # the replace path is where the claim is actually tested.
  paths <- sf_publish_paths()
  withr::local_options(pins.quiet = TRUE)
  cases <- list(
    list(name = "create path", listed = character(), versioned = TRUE),
    list(
      name = "unversioned replace of a published version",
      listed = "cars/20240101T000001Z-oldv/data.txt", versioned = FALSE
    )
  )
  for (case in cases) {
    board <- sf_mock_board(versioned = TRUE)
    rec <- sf_mock_bind(
      list = sf_fixture_listing(case$listed),
      put = function(sql, calls) {
        if (sum(grepl("^PUT ", calls)) == 3L) NULL else sf_mock_put_response(sql)
      }
    )

    expect_error(
      pinsExtras:::pin_store.pins_board_sf_stage(
        board, "cars", paths, sf_publish_meta(),
        versioned = case$versioned, x = NULL
      ),
      class = "pinsExtras_publication_uncertain",
      info = case$name
    )
    expect_identical(length(grep("^LIST ", rec$calls)), 1L, info = case$name)
    expect_identical(length(grep("^PUT ", rec$calls)), 3L, info = case$name)
    expect_identical(length(grep("^REMOVE ", rec$calls)), 0L, info = case$name)
  }
})

test_that("an unversioned replace uploads everything before it removes", {
  # The U11 ordering invariant: the pin is never absent, because every
  # PUT lands before the first REMOVE.
  shapes <- list(
    list(name = "user stage", args = list(versioned = TRUE)),
    list(name = "named stage",
         args = list(versioned = TRUE, path = "team-data", stage = "@mystage"))
  )
  oldv <- "20240101T000001Z-oldv"
  paths <- sf_publish_paths()
  withr::local_options(pins.quiet = TRUE)

  for (shape in shapes) {
    board <- do.call(sf_mock_board, shape$args)
    rec <- sf_mock_bind(
      list = function(sql, calls) {
        if (endsWith(sql, "cars/'")) {
          # The preflight listing shows the old version published; the
          # final pin-scoped listing shows it gone.
          if (sum(grepl("^LIST ", calls)) == 1L) {
            sf_fixture_listing(paste0("cars/", oldv, "/data.txt"), board = board)
          } else {
            sf_fixture_listing(board = board)
          }
        } else {
          # the confirm LIST for the old version directory
          sf_fixture_listing(board = board)
        }
      }
    )

    out <- expect_no_warning(
      pinsExtras:::pin_store.pins_board_sf_stage(
        board, "cars", paths, sf_publish_meta(), versioned = FALSE, x = NULL
      )
    )

    expect_identical(out, "cars", info = shape$name)
    expect_identical(length(grep("^LIST ", rec$calls)), 3L, info = shape$name)
    expect_identical(length(grep("^PUT ", rec$calls)), 3L, info = shape$name)
    expect_identical(length(grep("^REMOVE ", rec$calls)), 2L, info = shape$name)
    expect_lt(
      max(grep("^PUT ", rec$calls)),
      min(grep("^REMOVE ", rec$calls)),
      label = paste("last PUT on the", shape$name)
    )
  }
})

test_that("an unconfirmed cleanup warns and still returns the pin", {
  # A cleanup failure must not fail an otherwise successful write, and the
  # warning has to name the version the caller can now read.
  board <- sf_mock_board(versioned = TRUE)
  oldv <- "20240101T000001Z-oldv"
  meta <- sf_publish_meta(pin_hash = "zzz990000")
  new_version <- paste0(meta$created, "-", substr(meta$pin_hash, 1, 5))
  paths <- sf_publish_paths()
  rec <- sf_mock_bind(
    # Every listing still shows the old version, so the confirm LIST
    # cannot clear the data.txt and cleanup reports it as remaining.
    list = function(sql) {
      sf_fixture_listing(paste0("cars/", oldv, "/data.txt"), board = board)
    }
  )
  # Wide enough that cli does not wrap the suggested call across lines.
  withr::local_options(pins.quiet = TRUE, cli.width = 300)

  w <- expect_warning(
    out <- pinsExtras:::pin_store.pins_board_sf_stage(
      board, "cars", paths, meta, versioned = FALSE, x = NULL
    ),
    regexp = "cleanup is incomplete",
    class = "pinsExtras_cleanup_incomplete"
  )

  expect_identical(out, "cars")
  msg <- cli::ansi_strip(conditionMessage(w))
  expect_true(grepl(oldv, msg, fixed = TRUE))
  expect_true(grepl("pin_read(", msg, fixed = TRUE))
  expect_true(grepl(new_version, msg, fixed = TRUE))
  # preflight, the failed confirm LIST, and the final pin listing
  expect_length(grep("^LIST ", rec$calls), 3L)
  expect_length(grep("^PUT ", rec$calls), 3L)
  # only the first data.txt REMOVE, before the loop broke
  expect_length(grep("^REMOVE ", rec$calls), 1L)
})

test_that("a version is invisible to discovery until its data.txt lands", {
  board <- sf_mock_board()
  meta <- sf_publish_meta(file = "cars.rds", pin_hash = "zzz990000")
  v <- pinsExtras:::sf_version_name(meta)
  # A pre-existing payload-only directory of a different version: not
  # published, not the version being written, so it neither counts as a
  # version nor collides with the new write.
  oldp <- "20240101T000001Z-oldp"
  paths <- sf_publish_paths("cars.rds")
  # The LIST responder reads the record of issued commands: until the
  # metadata PUT goes out, discovery sees only the old payload.
  rec <- sf_mock_bind(
    list = function(sql, calls) {
      if (any(grepl("data.txt", calls, fixed = TRUE))) {
        sf_fixture_listing(paste0("cars/", v, "/data.txt"))
      } else {
        sf_fixture_listing(paste0("cars/", oldp, "/cars.rds"))
      }
    }
  )
  withr::local_options(pins.quiet = TRUE)

  expect_false(pins::pin_exists(board, "cars"))

  out <- pins::pin_store(board, "cars", paths, meta, versioned = FALSE, x = NULL)
  expect_identical(out, "cars")

  expect_true(pins::pin_exists(board, "cars"))
})

# ======================================================================
# pin_write() -- the budget through pins' own entry point
# ======================================================================
#
# These are the only offline tests that go through pins::pin_write(), so
# they are the only place the extra listing and the hash-check GET that
# upstream pins performs are measured.

# Run one pin_write() and return the metadata it uploaded as data.txt.
#
# The hash pins computes is derived from the serialized object, so this is
# how a test gets hold of the exact pin_hash a later identical write will
# produce, without reaching into pins' internals.
sf_publish_captured_meta <- function(board, x, name) {
  captured <- new.env(parent = emptyenv())
  rec <- sf_mock_transport(
    list = sf_fixture_listing(board = board),
    put = function(sql) {
      src <- sub("^file://", "", sf_mock_sql_args(sql)[[1]])
      if (fs::path_file(src) == "data.txt") {
        captured$meta <- yaml::read_yaml(src, eval.expr = FALSE)
      }
      sf_mock_put_response(sql)
    }
  )
  testthat::local_mocked_bindings(
    sf_stage_cmd = rec$responder, .package = "pinsExtras"
  )
  withr::local_options(pins.quiet = TRUE)
  pins::pin_write(board, x, name)
  captured$meta
}

test_that("pin_write() on a new pin adds only pins' own lookup listing", {
  # Two LIST: pins looks the pin up to compare hashes, then pin_store()
  # does its own preflight. The second one is pins' and is measured here
  # so a change in that cost is visible rather than silent. A board path
  # must not cost an extra request either.
  shapes <- list(
    list(name = "user stage", args = list()),
    list(name = "named stage",
         args = list(path = "team-data", stage = "@mystage"))
  )
  withr::local_options(pins.quiet = TRUE)

  for (shape in shapes) {
    board <- do.call(sf_mock_board, shape$args)
    rec <- sf_mock_bind(list = sf_fixture_listing(board = board))

    out <- pins::pin_write(board, data.frame(x = 1), "cars")

    expect_identical(out, "cars", info = shape$name)
    expect_identical(length(grep("^LIST ", rec$calls)), 2L, info = shape$name)
    expect_identical(length(grep("^GET ", rec$calls)), 0L, info = shape$name)
    expect_identical(length(grep("^PUT ", rec$calls)), 2L, info = shape$name)
    expect_identical(length(grep("^REMOVE ", rec$calls)), 0L, info = shape$name)
  }
})

test_that("pin_write() on an existing pin reads its metadata and rewrites", {
  # The GET here is pins' hash check, and it only counts for the right
  # reason if the served metadata actually parses: an empty data.txt
  # makes pin_meta() abort inside pins' possibly_pin_meta(), which
  # swallows the failure and returns NULL.
  board <- sf_mock_board()
  old_meta <- list(
    api_version = 1L, file = "cars.rds", file_size = 12,
    created = "20240101T000001Z", pin_hash = "0000000000", type = "rds"
  )
  rec <- sf_mock_bind(
    list = sf_fixture_listing(
      "cars/20240101T000001Z-00000/data.txt", board = board
    ),
    get = sf_mock_get_files("data.txt" = yaml::as.yaml(old_meta))
  )
  withr::local_options(pins.quiet = TRUE)

  out <- pins::pin_write(board, data.frame(x = 1), "cars")

  expect_identical(out, "cars")
  expect_length(grep("^LIST ", rec$calls), 2L)
  expect_length(grep("^GET ", rec$calls), 1L)
  expect_length(grep("^PUT ", rec$calls), 2L)
  expect_length(grep("^REMOVE ", rec$calls), 0L)
})

test_that("pin_write() skips the write when the hash has not changed", {
  # The other branch of the same comparison, and the one the old budget
  # test never reached. The metadata served here is the metadata a real
  # write of this object produced, so the hash really does match.
  x <- data.frame(x = 1)
  board <- sf_mock_board()
  written <- sf_publish_captured_meta(board, x, "cars")
  expect_false(is.null(written$pin_hash))
  version <- paste0(written$created, "-", substr(written$pin_hash, 1, 5))

  board2 <- sf_mock_board()
  rec <- sf_mock_bind(
    list = sf_fixture_listing(
      paste0("cars/", version, "/data.txt"), board = board2
    ),
    get = sf_mock_get_files("data.txt" = yaml::as.yaml(written))
  )

  msgs <- testthat::capture_messages(
    out <- pins::pin_write(board2, x, "cars")
  )

  expect_identical(out, "cars")
  expect_true(any(grepl("has not changed", msgs, fixed = TRUE)))
  expect_length(grep("^LIST ", rec$calls), 1L)
  expect_length(grep("^GET ", rec$calls), 1L)
  expect_length(grep("^PUT ", rec$calls), 0L)
  expect_length(grep("^REMOVE ", rec$calls), 0L)
})

test_that("pin_write(force_identical_write = TRUE) skips pins' own lookup", {
  board <- sf_mock_board()
  rec <- sf_mock_bind(list = sf_fixture_listing(board = board))
  withr::local_options(pins.quiet = TRUE)

  out <- pins::pin_write(
    board, data.frame(x = 1), "cars", force_identical_write = TRUE
  )

  expect_identical(out, "cars")
  expect_length(grep("^LIST ", rec$calls), 1L)
  expect_length(grep("^GET ", rec$calls), 0L)
  expect_length(grep("^PUT ", rec$calls), 2L)
  expect_length(grep("^REMOVE ", rec$calls), 0L)
})
