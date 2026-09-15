# The pure write decision, the progress helper, and post-publication
# cleanup. sf_version_plan() executes nothing: pin_store() carries the plan
# out, and test-sf_publish.R drives that.

# ---- sf_version_plan ----------------------------------------------------

# An index holding `n` published versions of "cars", oldest first.
sf_plan_index <- function(n) {
  versions <- c(
    "20240101T000001Z-aaa",
    "20240101T000002Z-bbb",
    "20240101T000003Z-ccc"
  )[seq_len(n)]
  pinsExtras:::sf_published_index(
    sf_fixture_listing(paste0("cars/", versions, "/data.txt"))
  )
}

test_that("sf_version_plan decides create, replace or abort", {
  new_version <- "20240101T000009Z-zzz"
  cases <- list(
    list(
      name = "nothing published, board versioned",
      n = 0L, versioned = NULL, board_versioned = TRUE,
      action = "create", old = character()
    ),
    list(
      # n == 0 always creates, whatever the caller asks for: there is
      # nothing to replace.
      name = "nothing published, unversioned write",
      n = 0L, versioned = FALSE, board_versioned = FALSE,
      action = "create", old = character()
    ),
    list(
      name = "one version, board versioned",
      n = 1L, versioned = NULL, board_versioned = TRUE,
      action = "create", old = character()
    ),
    list(
      # The load-bearing branch: the per-write override beats the
      # board's own flag, and the single old version is handed back for
      # cleanup.
      name = "one version, unversioned write",
      n = 1L, versioned = FALSE, board_versioned = TRUE,
      action = "replace", old = "20240101T000001Z-aaa"
    ),
    list(
      name = "one version, unversioned board",
      n = 1L, versioned = NULL, board_versioned = FALSE,
      action = "replace", old = "20240101T000001Z-aaa"
    ),
    list(
      # With more than one version published and no override, pins
      # forces versioning on even for an unversioned board: the board's
      # own flag is never consulted on this branch.
      name = "three versions, unversioned board",
      n = 3L, versioned = NULL, board_versioned = FALSE,
      action = "create", old = character()
    ),
    list(
      # An explicit versioned = TRUE reaches the effective-flag branch
      # rather than the n > 1 shortcut above.
      name = "three versions, versioned write",
      n = 3L, versioned = TRUE, board_versioned = FALSE,
      action = "create", old = character()
    )
  )
  for (case in cases) {
    plan <- pinsExtras:::sf_version_plan(
      sf_plan_index(case$n), "cars", new_version,
      versioned = case$versioned,
      board_versioned = case$board_versioned
    )
    expect_identical(plan$version, new_version, info = case$name)
    expect_identical(plan$action, case$action, info = case$name)
    expect_identical(plan$old_versions, case$old, info = case$name)
  }

  # An existing versioned pin cannot be rewritten without versions.
  expect_error(
    pinsExtras:::sf_version_plan(
      sf_plan_index(3L), "cars", new_version,
      versioned = FALSE, board_versioned = TRUE
    ),
    class = "pins_pin_versioned"
  )
})

test_that("sf_version_plan rejects a new version that is already published", {
  # The duplicate check runs before `versioned` is read, so the abort is
  # the same whatever the caller asked for.
  for (versioned in list(NULL, TRUE, FALSE)) {
    expect_error(
      pinsExtras:::sf_version_plan(
        sf_plan_index(2L), "cars", "20240101T000002Z-bbb",
        versioned = versioned
      ),
      "same as the most recent version",
      fixed = TRUE,
      info = paste("versioned =", format(versioned))
    )
  }
})

# ---- sf_inform ----------------------------------------------------------

test_that("sf_inform interpolates a caller's local into the message", {
  # .envir is threaded through to cli so it can find the CALLER's locals.
  # Dropping it would break interpolation silently, and every pin_store()
  # test sets pins.quiet = TRUE, so this is the only guard.
  f <- function() {
    v <- "20240101T000002Z-bbb"
    pinsExtras:::sf_inform("Creating new version {.val {v}}")
  }
  expect_message(f(), "20240101T000002Z-bbb")
})

test_that("sf_inform is silent when pins.quiet is set", {
  # The documented user-facing switch. `v` is undefined here, so the fact
  # that nothing errors also proves the early return happens before any
  # interpolation.
  withr::local_options(pins.quiet = TRUE)
  expect_no_message(
    pinsExtras:::sf_inform("Creating new version {.val {v}}")
  )
})

# ---- sf_cleanup_old_versions --------------------------------------------
#
# An unversioned write publishes the new version and only then removes the
# old one. This function reports the versions whose removal could not be
# confirmed; it never aborts and never warns, whatever the transport does.

V1 <- "20240101T000001Z-aaa"
V2 <- "20240101T000002Z-bbb"
V3 <- "20240101T000003Z-ccc"

# A LIST responder that answers the final pin-scoped listing with
# `remaining` and every confirm listing with nothing.
sf_cleanup_listing <- function(board, remaining) {
  function(sql) {
    if (endsWith(sql, "cars/'")) {
      sf_fixture_listing(
        paste0("cars/", remaining, "/cars.rds"),
        board = board
      )
    } else {
      sf_fixture_listing()
    }
  }
}

# A REMOVE responder that fails the marker delete and succeeds otherwise.
sf_cleanup_marker_fails <- function(sql) {
  if (grepl("PATTERN", sql, fixed = TRUE)) {
    stop("403 denied")
  }
  data.frame(name = character(), result = character(), stringsAsFactors = FALSE)
}

test_that("an empty request issues no commands at all", {
  # Step 0 returns before even the final listing, so a listing responder
  # that would fail is never reached.
  board <- sf_mock_board()
  rec <- sf_mock_bind(list = function(sql) stop("must not be reached"))

  out <- pinsExtras:::sf_cleanup_old_versions(board, "cars", character())

  expect_identical(out, character())
  expect_length(rec$calls, 0L)
})

test_that("a clean version is removed marker first, then directory", {
  # The order is what makes cleanup safe: drop the publication marker,
  # confirm it is gone, only then delete the directory.
  board <- sf_mock_board()
  rec <- sf_mock_bind()

  out <- pinsExtras:::sf_cleanup_old_versions(board, "cars", V1)

  expect_identical(out, character())
  expect_length(rec$calls, 4L)
  expect_length(grep("^LIST ", rec$calls), 2L)
  expect_identical(
    rec$calls[[1]],
    paste0(
      "REMOVE '@~/cars/", V1, "/' PATTERN = '^(cars/", V1, "/)?data\\\\.txt$'"
    )
  )

  # Two versions run the same three steps each, then one final listing.
  board2 <- sf_mock_board()
  rec2 <- sf_mock_bind()

  out2 <- pinsExtras:::sf_cleanup_old_versions(board2, "cars", c(V1, V2))

  expect_identical(out2, character())
  expect_length(rec2$calls, 7L)
  expect_length(grep("^LIST ", rec2$calls), 3L)
  expect_identical(
    rec2$calls[[1]],
    paste0(
      "REMOVE '@~/cars/", V1, "/' PATTERN = '^(cars/", V1, "/)?data\\\\.txt$'"
    )
  )
  expect_identical(rec2$calls[[3]], paste0("REMOVE '@~/cars/", V1, "/'"))
  expect_identical(
    rec2$calls[[4]],
    paste0(
      "REMOVE '@~/cars/", V2, "/' PATTERN = '^(cars/", V2, "/)?data\\\\.txt$'"
    )
  )
  expect_identical(rec2$calls[[7]], "LIST '@~/cars/'")
})

test_that("a failing marker REMOVE stops the loop and reports both versions", {
  # A later version is never attempted after an earlier one fails, and the
  # failure is never raised or warned about: a cleanup problem must not
  # turn a successful write into a failed one.
  board <- sf_mock_board()
  rec <- sf_mock_bind(
    remove = sf_cleanup_marker_fails,
    list = sf_cleanup_listing(board, c(V1, V2))
  )

  out <- expect_no_warning(
    pinsExtras:::sf_cleanup_old_versions(board, "cars", c(V1, V2))
  )

  expect_identical(out, c(V1, V2))
  # the failing REMOVE and the final listing only; V2 is never attempted
  expect_length(rec$calls, 2L)
  expect_length(grep("^REMOVE ", rec$calls), 1L)
})

test_that("a confirming LIST that still shows data.txt deletes no directory", {
  # An unconfirmed marker deletion must never be followed by a directory
  # delete: that is what stops a half-deleted version being wiped.
  board <- sf_mock_board()
  rec <- sf_mock_bind(
    list = function(sql) {
      if (endsWith(sql, "cars/'")) {
        sf_fixture_listing(
          paste0("cars/", V1, "/cars.rds"),
          paste0("cars/", V2, "/cars.rds")
        )
      } else {
        sf_fixture_listing(paste0("cars/", V1, "/data.txt"))
      }
    }
  )

  out <- pinsExtras:::sf_cleanup_old_versions(board, "cars", c(V1, V2))

  expect_identical(out, c(V1, V2))
  # REMOVE, confirming LIST, final LIST: zero directory deletes
  expect_length(rec$calls, 3L)
  expect_length(grep("REMOVE ", rec$calls), 1L)
  expect_length(grep("^LIST ", rec$calls), 2L)
})

test_that("a final listing that raises reports every version asked about", {
  # Nothing could be confirmed, so the caller is told about all of them.
  board <- sf_mock_board()
  rec <- sf_mock_bind(
    list = function(sql) {
      if (endsWith(sql, "cars/'")) {
        stop("dead connection")
      }
      sf_fixture_listing()
    }
  )

  out <- expect_no_warning(
    pinsExtras:::sf_cleanup_old_versions(board, "cars", c(V1, V2))
  )

  expect_identical(out, c(V1, V2))
  expect_length(rec$calls, 7L)
})

test_that("the final listing decides which versions are reported", {
  # The listing is the authority, not the sequence of REMOVEs: every
  # REMOVE below reports success.
  cases <- list(
    list(
      # V1 is gone and V3 was never asked about, so only V2 is reported.
      name = "one asked-about version remains",
      remaining = c(V2, V3), want = V2
    ),
    list(
      name = "both asked-about versions remain",
      remaining = c(V1, V2), want = c(V1, V2)
    )
  )
  for (case in cases) {
    board <- sf_mock_board()
    rec <- sf_mock_bind(list = sf_cleanup_listing(board, case$remaining))

    out <- pinsExtras:::sf_cleanup_old_versions(board, "cars", c(V1, V2))

    expect_identical(out, case$want, info = case$name)
    expect_identical(length(rec$calls), 7L, info = case$name)
  }
})

test_that("the board path is honoured when deciding what remains", {
  # Without the sf_board_relative() strip the "team-data/" prefix would
  # never match and every other cleanup test would still pass.
  board <- sf_mock_board(path = "team-data")
  rec <- sf_mock_bind(list = sf_cleanup_listing(board, V1))

  out <- pinsExtras:::sf_cleanup_old_versions(board, "cars", V1)

  expect_identical(out, V1)
  expect_length(rec$calls, 4L)
})
