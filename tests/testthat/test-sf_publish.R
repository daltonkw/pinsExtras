# Collision detection against the raw pin-scoped listing, not the index.
# The index hides payload-only directories, but a half-finished write must
# be seen. Pure: takes a listing, no transport is mocked.

# Every edge case targets the same pin/version; the version string is
# written in each test to keep the file free of top-level code.

test_that("sf_check_version_collision fires on a payload-only version directory", {
  V <- "20240101T000002Z-bbb"
  listing <- sf_fixture_listing(paste0("cars/", V, "/cars.rds"))
  expect_error(
    pinsExtras:::sf_check_version_collision(
      listing, "cars", V, prefix = ""
    ),
    class = "pinsExtras_version_collision"
  )
})

test_that("sf_check_version_collision fires when data.txt is already present", {
  V <- "20240101T000002Z-bbb"
  listing <- sf_fixture_listing(paste0("cars/", V, "/data.txt"))
  expect_error(
    pinsExtras:::sf_check_version_collision(
      listing, "cars", V, prefix = ""
    ),
    class = "pinsExtras_version_collision"
  )
})

test_that("sf_check_version_collision ignores a sibling pin with a shared prefix", {
  V <- "20240101T000002Z-bbb"
  listing <- sf_fixture_listing(paste0("cars_extra/", V, "/data.txt"))
  expect_invisible(
    pinsExtras:::sf_check_version_collision(
      listing, "cars", V, prefix = ""
    )
  )
})

test_that("sf_check_version_collision ignores a different version of the same pin", {
  V <- "20240101T000002Z-bbb"
  listing <- sf_fixture_listing(
    "cars/20240101T000001Z-aaa/data.txt"
  )
  expect_invisible(
    pinsExtras:::sf_check_version_collision(
      listing, "cars", V, prefix = ""
    )
  )
})

test_that("sf_check_version_collision passes on a zero-row listing", {
  V <- "20240101T000002Z-bbb"
  listing <- sf_fixture_listing()
  expect_invisible(
    pinsExtras:::sf_check_version_collision(
      listing, "cars", V, prefix = ""
    )
  )
})

test_that("sf_check_version_collision fires under a board path with a non-empty prefix", {
  V <- "20240101T000002Z-bbb"
  listing <- sf_fixture_listing(
    paste0("team-data/cars/", V, "/cars.rds")
  )
  expect_error(
    pinsExtras:::sf_check_version_collision(
      listing, "cars", V, prefix = "team-data"
    ),
    class = "pinsExtras_version_collision"
  )
})

test_that("sf_check_version_collision misses unless the board path prefix is given", {
  V <- "20240101T000002Z-bbb"
  listing <- sf_fixture_listing(
    paste0("team-data/cars/", V, "/cars.rds")
  )
  # With prefix = "" the board path is not stripped, so the target
  # "cars/<V>" is never reached; the argument is what makes the check live.
  expect_invisible(
    pinsExtras:::sf_check_version_collision(
      listing, "cars", V, prefix = ""
    )
  )
})

test_that("sf_check_version_collision names the pin and version", {
  V <- "20240101T000002Z-bbb"
  listing <- sf_fixture_listing(paste0("cars/", V, "/cars.rds"))
  cond <- expect_error(
    pinsExtras:::sf_check_version_collision(
      listing, "cars", V, prefix = ""
    ),
    class = "pinsExtras_version_collision"
  )
  msg <- cli::ansi_strip(conditionMessage(cond))
  expect_true(grepl(V, msg, fixed = TRUE))
  expect_true(grepl("of pin", msg, fixed = TRUE))
})

# ---- PUT response validation (U8) ---------------------------------------
# Throughout, the uploaded file is "cars.rds" and the key is
# "cars/v/cars.rds".

# --- sf_check_put_result: success ---

test_that("sf_check_put_result accepts an UPLOADED response", {
  result <- data.frame(
    source = "cars.rds", target = "@~/cars/v/cars.rds",
    status = "UPLOADED", message = "", stringsAsFactors = FALSE
  )
  value <- pinsExtras:::sf_check_put_result(
    result, "cars.rds", "cars/v/cars.rds"
  )
  expect_true(value)
})

test_that("sf_check_put_result matches the status case-insensitively", {
  result <- data.frame(
    source = "cars.rds", target = "@~/cars/v/cars.rds",
    status = "uploaded", message = "", stringsAsFactors = FALSE
  )
  value <- pinsExtras:::sf_check_put_result(
    result, "cars.rds", "cars/v/cars.rds"
  )
  expect_true(value)
})

test_that("sf_check_put_result tolerates upper-case column names", {
  result <- data.frame(
    source = "cars.rds", target = "@~/cars/v/cars.rds",
    status = "UPLOADED", message = "", stringsAsFactors = FALSE
  )
  names(result) <- toupper(names(result))
  value <- pinsExtras:::sf_check_put_result(
    result, "cars.rds", "cars/v/cars.rds"
  )
  expect_true(value)
})

# --- sf_check_put_result: interpretability failures (1-3) ---

test_that("sf_check_put_result fails on a zero-row response", {
  result <- data.frame(
    source = character(), target = character(),
    status = character(), message = character(),
    stringsAsFactors = FALSE
  )
  expect_error(
    pinsExtras:::sf_check_put_result(result, "cars.rds", "cars/v/cars.rds"),
    class = "pinsExtras_upload_failed",
    regexp = "no upload result",
    fixed = TRUE
  )
})

test_that("sf_check_put_result fails when several rows come back", {
  result <- data.frame(
    source = c("cars.rds", "cars.rds"),
    target = c("@~/cars/v/cars.rds", "@~/cars/v/cars.rds"),
    status = c("UPLOADED", "UPLOADED"),
    message = c("", ""), stringsAsFactors = FALSE
  )
  expect_error(
    pinsExtras:::sf_check_put_result(result, "cars.rds", "cars/v/cars.rds"),
    class = "pinsExtras_upload_failed",
    regexp = "2 results",
    fixed = TRUE
  )
})

test_that("sf_check_put_result fails with no status column", {
  result <- data.frame(
    source = "cars.rds", target = "@~/cars/v/cars.rds",
    message = "", stringsAsFactors = FALSE
  )
  expect_error(
    pinsExtras:::sf_check_put_result(result, "cars.rds", "cars/v/cars.rds"),
    class = "pinsExtras_upload_failed",
    regexp = "could not be interpreted",
    fixed = TRUE
  )
})

test_that("sf_check_put_result fails on an NA status", {
  result <- data.frame(
    source = "cars.rds", target = "@~/cars/v/cars.rds",
    status = NA_character_, message = "", stringsAsFactors = FALSE
  )
  expect_error(
    pinsExtras:::sf_check_put_result(result, "cars.rds", "cars/v/cars.rds"),
    class = "pinsExtras_upload_failed",
    regexp = "could not be interpreted",
    fixed = TRUE
  )
})

test_that("sf_check_put_result fails when target is absent, only target_size", {
  result <- data.frame(
    source = "cars.rds", target_size = 1,
    status = "UPLOADED", stringsAsFactors = FALSE
  )
  expect_error(
    pinsExtras:::sf_check_put_result(result, "cars.rds", "cars/v/cars.rds"),
    class = "pinsExtras_upload_failed",
    regexp = "could not be interpreted",
    fixed = TRUE
  )
})

test_that("sf_check_put_result fails on an NA target", {
  result <- data.frame(
    source = "cars.rds", target = NA_character_,
    status = "UPLOADED", message = "", stringsAsFactors = FALSE
  )
  expect_error(
    pinsExtras:::sf_check_put_result(result, "cars.rds", "cars/v/cars.rds"),
    class = "pinsExtras_upload_failed",
    regexp = "could not be interpreted",
    fixed = TRUE
  )
})

# --- sf_check_put_result: explicit failures (4-6) ---

test_that("sf_check_put_result fails on a SKIPPED response", {
  result <- data.frame(
    source = "cars.rds", target = "@~/cars/v/cars.rds",
    status = "SKIPPED", message = "File already exists",
    stringsAsFactors = FALSE
  )
  expect_error(
    pinsExtras:::sf_check_put_result(result, "cars.rds", "cars/v/cars.rds"),
    class = "pinsExtras_upload_failed",
    regexp = "skipped",
    fixed = TRUE
  )
})

test_that("sf_check_put_result fails on a non-UPLOADED status", {
  result <- data.frame(
    source = "cars.rds", target = "@~/cars/v/cars.rds",
    status = "ERROR", message = "boom", stringsAsFactors = FALSE
  )
  expect_error(
    pinsExtras:::sf_check_put_result(result, "cars.rds", "cars/v/cars.rds"),
    class = "pinsExtras_upload_failed",
    regexp = "reported status",
    fixed = TRUE
  )
})

test_that("sf_check_put_result fails when the target name is wrong", {
  result <- data.frame(
    source = "cars.rds", target = "@~/cars/v/other.rds",
    status = "UPLOADED", message = "", stringsAsFactors = FALSE
  )
  expect_error(
    pinsExtras:::sf_check_put_result(result, "cars.rds", "cars/v/cars.rds"),
    class = "pinsExtras_upload_failed",
    regexp = "instead",
    fixed = TRUE
  )
})

# --- sf_check_meta_put_result: success ---

test_that("sf_check_meta_put_result accepts an UPLOADED response", {
  result <- data.frame(
    source = "cars.rds", target = "@~/cars/v/cars.rds",
    status = "UPLOADED", message = "", stringsAsFactors = FALSE
  )
  value <- pinsExtras:::sf_check_meta_put_result(
    result, "cars.rds", "cars/v/cars.rds"
  )
  expect_true(value)
})

test_that("sf_check_meta_put_result matches the status case-insensitively", {
  result <- data.frame(
    source = "cars.rds", target = "@~/cars/v/cars.rds",
    status = "uploaded", message = "", stringsAsFactors = FALSE
  )
  value <- pinsExtras:::sf_check_meta_put_result(
    result, "cars.rds", "cars/v/cars.rds"
  )
  expect_true(value)
})

test_that("sf_check_meta_put_result tolerates upper-case column names", {
  result <- data.frame(
    source = "cars.rds", target = "@~/cars/v/cars.rds",
    status = "UPLOADED", message = "", stringsAsFactors = FALSE
  )
  names(result) <- toupper(names(result))
  value <- pinsExtras:::sf_check_meta_put_result(
    result, "cars.rds", "cars/v/cars.rds"
  )
  expect_true(value)
})

# --- sf_check_meta_put_result: UNINTERPRETABLE responses -> uncertain ---

test_that("sf_check_meta_put_result is uncertain on a zero-row response", {
  result <- data.frame(
    source = character(), target = character(),
    status = character(), message = character(),
    stringsAsFactors = FALSE
  )
  expect_error(
    pinsExtras:::sf_check_meta_put_result(
      result, "cars.rds", "cars/v/cars.rds"
    ),
    class = "pinsExtras_publication_uncertain"
  )
})

test_that("sf_check_meta_put_result is uncertain when several rows come back", {
  result <- data.frame(
    source = c("cars.rds", "cars.rds"),
    target = c("@~/cars/v/cars.rds", "@~/cars/v/cars.rds"),
    status = c("UPLOADED", "UPLOADED"),
    message = c("", ""), stringsAsFactors = FALSE
  )
  expect_error(
    pinsExtras:::sf_check_meta_put_result(
      result, "cars.rds", "cars/v/cars.rds"
    ),
    class = "pinsExtras_publication_uncertain"
  )
})

test_that("sf_check_meta_put_result is uncertain with no status column", {
  result <- data.frame(
    source = "cars.rds", target = "@~/cars/v/cars.rds",
    message = "", stringsAsFactors = FALSE
  )
  expect_error(
    pinsExtras:::sf_check_meta_put_result(
      result, "cars.rds", "cars/v/cars.rds"
    ),
    class = "pinsExtras_publication_uncertain"
  )
})

test_that("sf_check_meta_put_result is uncertain on an NA status", {
  result <- data.frame(
    source = "cars.rds", target = "@~/cars/v/cars.rds",
    status = NA_character_, message = "", stringsAsFactors = FALSE
  )
  expect_error(
    pinsExtras:::sf_check_meta_put_result(
      result, "cars.rds", "cars/v/cars.rds"
    ),
    class = "pinsExtras_publication_uncertain"
  )
})

test_that("sf_check_meta_put_result is uncertain when target is absent", {
  result <- data.frame(
    source = "cars.rds", target_size = 1,
    status = "UPLOADED", stringsAsFactors = FALSE
  )
  expect_error(
    pinsExtras:::sf_check_meta_put_result(
      result, "cars.rds", "cars/v/cars.rds"
    ),
    class = "pinsExtras_publication_uncertain"
  )
})

test_that("sf_check_meta_put_result is uncertain on an NA target", {
  result <- data.frame(
    source = "cars.rds", target = NA_character_,
    status = "UPLOADED", message = "", stringsAsFactors = FALSE
  )
  expect_error(
    pinsExtras:::sf_check_meta_put_result(
      result, "cars.rds", "cars/v/cars.rds"
    ),
    class = "pinsExtras_publication_uncertain"
  )
})

# --- sf_check_meta_put_result: explicit failures -> upload_failed ---

test_that("sf_check_meta_put_result fails on a SKIPPED response", {
  result <- data.frame(
    source = "cars.rds", target = "@~/cars/v/cars.rds",
    status = "SKIPPED", message = "File already exists",
    stringsAsFactors = FALSE
  )
  expect_error(
    pinsExtras:::sf_check_meta_put_result(
      result, "cars.rds", "cars/v/cars.rds"
    ),
    class = "pinsExtras_upload_failed",
    regexp = "skipped",
    fixed = TRUE
  )
})

test_that("sf_check_meta_put_result fails on a non-UPLOADED status", {
  result <- data.frame(
    source = "cars.rds", target = "@~/cars/v/cars.rds",
    status = "ERROR", message = "boom", stringsAsFactors = FALSE
  )
  expect_error(
    pinsExtras:::sf_check_meta_put_result(
      result, "cars.rds", "cars/v/cars.rds"
    ),
    class = "pinsExtras_upload_failed",
    regexp = "reported status",
    fixed = TRUE
  )
})

test_that("sf_check_meta_put_result fails when the target name is wrong", {
  result <- data.frame(
    source = "cars.rds", target = "@~/cars/v/other.rds",
    status = "UPLOADED", message = "", stringsAsFactors = FALSE
  )
  expect_error(
    pinsExtras:::sf_check_meta_put_result(
      result, "cars.rds", "cars/v/cars.rds"
    ),
    class = "pinsExtras_upload_failed",
    regexp = "instead",
    fixed = TRUE
  )
})

# --- sf_stage_upload end to end through the transport ---

test_that("sf_stage_upload sends OVERWRITE=FALSE and validates", {
  board <- sf_mock_board()
  d <- withr::local_tempdir()
  src <- fs::path(d, "cars.rds")
  writeLines("x", src)
  rec <- sf_mock_transport()
  testthat::local_mocked_bindings(
    sf_stage_cmd = rec$responder,
    .package = "pinsExtras"
  )

  expect_true(
    pinsExtras:::sf_stage_upload(board, src, "cars/v/cars.rds")
  )
  expect_length(rec$calls, 1L)
  expect_match(
    rec$calls[[1]],
    "'@~/cars/v' AUTO_COMPRESS=FALSE OVERWRITE=FALSE",
    fixed = TRUE
  )
})

test_that("sf_stage_upload honours overwrite=TRUE", {
  board <- sf_mock_board()
  d <- withr::local_tempdir()
  src <- fs::path(d, "cars.rds")
  writeLines("x", src)
  rec <- sf_mock_transport()
  testthat::local_mocked_bindings(
    sf_stage_cmd = rec$responder,
    .package = "pinsExtras"
  )

  expect_true(
    pinsExtras:::sf_stage_upload(
      board, src, "cars/v/cars.rds", overwrite = TRUE
    )
  )
  expect_match(
    rec$calls[[1]],
    "'@~/cars/v' AUTO_COMPRESS=FALSE OVERWRITE=TRUE",
    fixed = TRUE
  )
})

test_that("sf_stage_upload renames a mismatched source basename", {
  board <- sf_mock_board()
  d <- withr::local_tempdir()
  src <- fs::path(d, "payload.rds")
  writeLines("x", src)
  rec <- sf_mock_transport()
  testthat::local_mocked_bindings(
    sf_stage_cmd = rec$responder,
    .package = "pinsExtras"
  )

  expect_true(
    pinsExtras:::sf_stage_upload(board, src, "cars/v/cars.rds")
  )
  expect_match(rec$calls[[1]], "/cars.rds'", fixed = TRUE)
  expect_false(grepl("payload.rds", rec$calls[[1]], fixed = TRUE))
  expect_match(
    rec$calls[[1]],
    "AUTO_COMPRESS=FALSE OVERWRITE=FALSE",
    fixed = TRUE
  )
})

test_that("sf_stage_upload targets the board root with no trailing slash", {
  board <- sf_mock_board()
  d <- withr::local_tempdir()
  src <- fs::path(d, "_pins.yaml")
  writeLines("x", src)
  rec <- sf_mock_transport()
  testthat::local_mocked_bindings(
    sf_stage_cmd = rec$responder,
    .package = "pinsExtras"
  )

  expect_true(pinsExtras:::sf_stage_upload(board, src, "_pins.yaml"))
  expect_match(
    rec$calls[[1]],
    "'@~' AUTO_COMPRESS=FALSE OVERWRITE=FALSE",
    fixed = TRUE
  )
})

test_that("sf_stage_upload aborts when the PUT is SKIPPED", {
  board <- sf_mock_board()
  d <- withr::local_tempdir()
  src <- fs::path(d, "cars.rds")
  writeLines("x", src)
  rec <- sf_mock_transport(
    put = data.frame(
      source = "cars.rds", target = "@~/cars/v/cars.rds",
      status = "SKIPPED", message = "File already exists",
      stringsAsFactors = FALSE
    )
  )
  testthat::local_mocked_bindings(
    sf_stage_cmd = rec$responder,
    .package = "pinsExtras"
  )

  expect_error(
    pinsExtras:::sf_stage_upload(board, src, "cars/v/cars.rds"),
    class = "pinsExtras_upload_failed",
    regexp = "skipped",
    fixed = TRUE
  )
})

test_that("sf_stage_upload_meta sends OVERWRITE=FALSE and validates", {
  board <- sf_mock_board()
  d <- withr::local_tempdir()
  src <- fs::path(d, "data.txt")
  writeLines("x", src)
  rec <- sf_mock_transport()
  testthat::local_mocked_bindings(
    sf_stage_cmd = rec$responder,
    .package = "pinsExtras"
  )

  expect_true(
    pinsExtras:::sf_stage_upload_meta(board, src, "cars/v/data.txt")
  )
  expect_match(
    rec$calls[[1]],
    "'@~/cars/v' AUTO_COMPRESS=FALSE OVERWRITE=FALSE",
    fixed = TRUE
  )
})

# =====================================================================
# pin_store.pins_board_sf_stage -- the full write sequence (U11-store)
# =====================================================================
# Each test mocks only sf_stage_cmd (the one SQL dispatch point); every
# stage helper and validation function runs for real against the mock
# transport. Request counts come from grepping the recorded SQL verbs.
# Progress output is silenced with options(pins.quiet = TRUE).

test_that("a new versioned pin issues one LIST, three PUT, zero REMOVE", {
  board <- sf_mock_board()
  meta <- list(
    api_version = 1L, file = c("cars.rds", "wheels.rds"), file_size = 12L,
    created = "20240102T000000Z", pin_hash = "abcdef0123456789",
    type = "rds"
  )
  dir <- withr::local_tempdir(.local_envir = parent.frame())
  paths <- c(
    file.path(dir, "cars.rds"),
    file.path(dir, "wheels.rds")
  )
  writeLines("a", paths[[1]])
  writeLines("b", paths[[2]])
  rec <- sf_mock_transport(list = sf_fixture_listing())
  testthat::local_mocked_bindings(
    sf_stage_cmd = rec$responder, .package = "pinsExtras"
  )
  withr::local_options(pins.quiet = TRUE)

  out <- pins::pin_store(
    board, "cars", paths, meta, versioned = TRUE, x = NULL
  )

  expect_identical(out, "cars")
  expect_identical(grep("^LIST ", rec$calls, value = TRUE), "LIST '@~/cars/'")
  expect_length(grep("^PUT ", rec$calls), 3)
  expect_length(grep("^REMOVE ", rec$calls), 0)
})

test_that("an unversioned single-payload pin issues one LIST, two PUT", {
  board <- sf_mock_board(versioned = TRUE)
  meta <- list(
    api_version = 1L, file = "cars.rds", file_size = 12L,
    created = "20240102T000000Z", pin_hash = "abcdef0123456789",
    type = "rds"
  )
  dir <- withr::local_tempdir(.local_envir = parent.frame())
  paths <- file.path(dir, "cars.rds")
  writeLines("a", paths)
  rec <- sf_mock_transport(list = sf_fixture_listing())
  testthat::local_mocked_bindings(
    sf_stage_cmd = rec$responder, .package = "pinsExtras"
  )
  withr::local_options(pins.quiet = TRUE)

  out <- pinsExtras:::pin_store.pins_board_sf_stage(
    board, "cars", paths, meta, versioned = FALSE, x = NULL
  )

  expect_identical(out, "cars")
  expect_length(grep("^LIST ", rec$calls), 1)
  # one payload PUT plus the metadata PUT, so two PUT in total
  expect_length(grep("^PUT ", rec$calls), 2)
  expect_length(grep("^REMOVE ", rec$calls), 0)
})

test_that(
  "a payload-only version is invisible until its data.txt lands",
  {
    # The pin carries an old version directory that only has a payload,
    # no data.txt. Because it is not published, pin_store neither treats
    # it as the most recent version nor collides against it: it writes a
    # fresh version. Visibility is what lets the version stay silent.
    board <- sf_mock_board(versioned = TRUE)
    meta <- list(
      api_version = 1L, file = "cars.rds", file_size = 12L,
      created = "20240102T000000Z", pin_hash = "abcdef0123456789",
      type = "rds"
    )
    dir <- withr::local_tempdir(.local_envir = parent.frame())
    paths <- file.path(dir, "cars.rds")
    writeLines("a", paths)
    old_payload <- paste0(
      "cars/", "20240101T000001Z-oldp", "/cars.rds"
    )
    rec <- sf_mock_transport(list = sf_fixture_listing(old_payload))
    testthat::local_mocked_bindings(
      sf_stage_cmd = rec$responder, .package = "pinsExtras"
    )
    withr::local_options(pins.quiet = TRUE)

    out <- pinsExtras:::pin_store.pins_board_sf_stage(
      board, "cars", paths, meta, versioned = TRUE, x = NULL
    )

    expect_identical(out, "cars")
    # the payload-only version did not block the write
    expect_length(grep("^LIST ", rec$calls), 1)
    expect_length(grep("^PUT ", rec$calls), 2)
    expect_length(grep("^REMOVE ", rec$calls), 0)
  }
)

test_that(
  "a reserved pin name aborts before a single LIST issues",
  {
    board <- sf_mock_board()
    meta <- list(
      api_version = 1L, file = "cars.rds", file_size = 12L,
      created = "20240102T000000Z", pin_hash = "abcdef0123456789",
      type = "rds"
    )
    dir <- withr::local_tempdir(.local_envir = parent.frame())
    paths <- file.path(dir, "cars.rds")
    writeLines("a", paths)
    rec <- sf_mock_transport(list = sf_fixture_listing())
    testthat::local_mocked_bindings(
      sf_stage_cmd = rec$responder, .package = "pinsExtras"
    )
    withr::local_options(pins.quiet = TRUE)

    expect_error(
      pinsExtras:::pin_store.pins_board_sf_stage(
        board, "data.txt", paths, meta, versioned = TRUE, x = NULL
      ),
      "Can't pin file called",
      fixed = TRUE
    )
    expect_length(rec$calls, 0)
  }
)

test_that(
  "an upload set that mismatches metadata aborts before any PUT",
  {
    board <- sf_mock_board()
    meta <- list(
      api_version = 1L, file = "cars.rds", file_size = 12L,
      created = "20240102T000000Z", pin_hash = "abcdef0123456789",
      type = "rds"
    )
    dir <- withr::local_tempdir(.local_envir = parent.frame())
    paths <- file.path(dir, "wheels.rds")
    writeLines("b", paths)
    rec <- sf_mock_transport(list = sf_fixture_listing())
    testthat::local_mocked_bindings(
      sf_stage_cmd = rec$responder, .package = "pinsExtras"
    )
    withr::local_options(pins.quiet = TRUE)

    expect_error(
      pinsExtras:::pin_store.pins_board_sf_stage(
        board, "cars", paths, meta, versioned = TRUE, x = NULL
      ),
      class = "pinsExtras_invalid_upload_set"
    )
    expect_length(rec$calls, 0)
  }
)

test_that(
  "a published duplicate aborts after one LIST, before any PUT",
  {
    board <- sf_mock_board()
    meta <- list(
      api_version = 1L, file = "cars.rds", file_size = 12L,
      created = "20240101T000002Z", pin_hash = "abcdef0123456789",
      type = "rds"
    )
    dir <- withr::local_tempdir(.local_envir = parent.frame())
    paths <- file.path(dir, "cars.rds")
    writeLines("a", paths)
    v <- paste0(
      meta$created, "-", substr(meta$pin_hash, 1, 5)
    )
    rec <- sf_mock_transport(
      list = sf_fixture_listing(paste0("cars/", v, "/data.txt"))
    )
    testthat::local_mocked_bindings(
      sf_stage_cmd = rec$responder, .package = "pinsExtras"
    )
    withr::local_options(pins.quiet = TRUE)

    expect_error(
      pinsExtras:::pin_store.pins_board_sf_stage(
        board, "cars", paths, meta, versioned = TRUE, x = NULL
      ),
      "the most recent version"
    )
    expect_length(grep("^LIST ", rec$calls), 1)
    expect_length(grep("^PUT ", rec$calls), 0)
  }
)

test_that(
  "a payload-only directory collides after one LIST, before any PUT",
  {
    board <- sf_mock_board()
    meta <- list(
      api_version = 1L, file = "cars.rds", file_size = 12L,
      created = "20240102T000000Z", pin_hash = "abcdef0123456789",
      type = "rds"
    )
    dir <- withr::local_tempdir(.local_envir = parent.frame())
    paths <- file.path(dir, "cars.rds")
    writeLines("a", paths)
    v <- paste0(
      meta$created, "-", substr(meta$pin_hash, 1, 5)
    )
    rec <- sf_mock_transport(
      list = sf_fixture_listing(paste0("cars/", v, "/cars.rds"))
    )
    testthat::local_mocked_bindings(
      sf_stage_cmd = rec$responder, .package = "pinsExtras"
    )
    withr::local_options(pins.quiet = TRUE)

    expect_error(
      pinsExtras:::pin_store.pins_board_sf_stage(
        board, "cars", paths, meta, versioned = TRUE, x = NULL
      ),
      class = "pinsExtras_version_collision"
    )
    expect_length(grep("^LIST ", rec$calls), 1)
    expect_length(grep("^PUT ", rec$calls), 0)
  }
)

test_that(
  "an unversioned write against a versioned pin aborts pins_pin_versioned",
  {
    board <- sf_mock_board(versioned = TRUE)
    meta <- list(
      api_version = 1L, file = "cars.rds", file_size = 12L,
      created = "20240102T000000Z", pin_hash = "abcdef0123456789",
      type = "rds"
    )
    dir <- withr::local_tempdir(.local_envir = parent.frame())
    paths <- file.path(dir, "cars.rds")
    writeLines("a", paths)
    rec <- sf_mock_transport(
      list = sf_fixture_listing(
        paste0("cars/", "20240101T000001Z-aaa", "/data.txt"),
        paste0("cars/", "20240101T000002Z-bbb", "/data.txt")
      )
    )
    testthat::local_mocked_bindings(
      sf_stage_cmd = rec$responder, .package = "pinsExtras"
    )
    withr::local_options(pins.quiet = TRUE)

    expect_error(
      pinsExtras:::pin_store.pins_board_sf_stage(
        board, "cars", paths, meta, versioned = FALSE, x = NULL
      ),
      class = "pins_pin_versioned"
    )
    expect_length(grep("^LIST ", rec$calls), 1)
    expect_length(grep("^PUT ", rec$calls), 0)
  }
)

test_that("a failing second payload PUT aborts after one LIST, two PUT", {
  board <- sf_mock_board()
  meta <- list(
    api_version = 1L, file = c("cars.rds", "wheels.rds"), file_size = 12L,
    created = "20240102T000000Z", pin_hash = "abcdef0123456789",
    type = "rds"
  )
  dir <- withr::local_tempdir(.local_envir = parent.frame())
  paths <- c(
    file.path(dir, "cars.rds"),
    file.path(dir, "wheels.rds")
  )
  writeLines("a", paths[[1]])
  writeLines("b", paths[[2]])
  bad_put <- function(sql) {
    args <- sf_mock_sql_args(sql)
    src <- basename(sub("^file://", "", args[[1]]))
    data.frame(
      source = src, target = paste0(args[[2]], "/", src),
      source_size = 1024, target_size = 1024,
      source_compression = "NONE", target_compression = "NONE",
      status = "ERROR", message = "boom",
      stringsAsFactors = FALSE
    )
  }
  rec <- sf_mock_transport(
    put = function(sql, calls) {
      if (sum(grepl("^PUT ", calls)) == 2L) bad_put(sql) else {
        sf_mock_put_response(sql)
      }
    }
  )
  testthat::local_mocked_bindings(
    sf_stage_cmd = rec$responder, .package = "pinsExtras"
  )
  withr::local_options(pins.quiet = TRUE)

  expect_error(
    pinsExtras:::pin_store.pins_board_sf_stage(
      board, "cars", paths, meta, versioned = TRUE, x = NULL
    ),
    class = "pinsExtras_upload_failed"
  )
  expect_length(grep("^LIST ", rec$calls), 1)
  # only the first payload PUT landed before the failure; the second and
  # the metadata PUT were never issued
  expect_length(grep("^PUT ", rec$calls), 2)
  expect_length(grep("^REMOVE ", rec$calls), 0)
})

test_that("a SKIPPED payload PUT is reported as an upload failure", {
  board <- sf_mock_board()
  meta <- list(
    api_version = 1L, file = "cars.rds", file_size = 12L,
    created = "20240102T000000Z", pin_hash = "abcdef0123456789",
    type = "rds"
  )
  dir <- withr::local_tempdir(.local_envir = parent.frame())
  paths <- file.path(dir, "cars.rds")
  writeLines("a", paths)
  rec <- sf_mock_transport(
    put = function(sql) {
      args <- sf_mock_sql_args(sql)
      src <- basename(sub("^file://", "", args[[1]]))
      data.frame(
        source = src, target = paste0(args[[2]], "/", src),
        source_size = 1024, target_size = 1024,
        source_compression = "NONE", target_compression = "NONE",
        status = "SKIPPED", message = "",
        stringsAsFactors = FALSE
      )
    }
  )
  testthat::local_mocked_bindings(
    sf_stage_cmd = rec$responder, .package = "pinsExtras"
  )
  withr::local_options(pins.quiet = TRUE)

  expect_error(
    pinsExtras:::pin_store.pins_board_sf_stage(
      board, "cars", paths, meta, versioned = TRUE, x = NULL
    ),
    class = "pinsExtras_upload_failed"
  )
  expect_length(grep("^LIST ", rec$calls), 1)
  expect_length(grep("^PUT ", rec$calls), 1)
  expect_length(grep("^REMOVE ", rec$calls), 0)
})

test_that("an explicit metadata PUT failure is a plain upload failure", {
  board <- sf_mock_board()
  meta <- list(
    api_version = 1L, file = c("cars.rds", "wheels.rds"), file_size = 12L,
    created = "20240102T000000Z", pin_hash = "abcdef0123456789",
    type = "rds"
  )
  dir <- withr::local_tempdir(.local_envir = parent.frame())
  paths <- c(
    file.path(dir, "cars.rds"),
    file.path(dir, "wheels.rds")
  )
  writeLines("a", paths[[1]])
  writeLines("b", paths[[2]])
  bad_put <- function(sql) {
    args <- sf_mock_sql_args(sql)
    src <- basename(sub("^file://", "", args[[1]]))
    data.frame(
      source = src, target = paste0(args[[2]], "/", src),
      source_size = 1024, target_size = 1024,
      source_compression = "NONE", target_compression = "NONE",
      status = "ERROR", message = "boom",
      stringsAsFactors = FALSE
    )
  }
  rec <- sf_mock_transport(
    put = function(sql) {
      if (grepl("data.txt", sql, fixed = TRUE)) bad_put(sql) else {
        sf_mock_put_response(sql)
      }
    }
  )
  testthat::local_mocked_bindings(
    sf_stage_cmd = rec$responder, .package = "pinsExtras"
  )
  withr::local_options(pins.quiet = TRUE)

  expect_error(
    pinsExtras:::pin_store.pins_board_sf_stage(
      board, "cars", paths, meta, versioned = TRUE, x = NULL
    ),
    class = "pinsExtras_upload_failed"
  )
  expect_length(grep("^LIST ", rec$calls), 1)
  expect_length(grep("^PUT ", rec$calls), 3)
  expect_length(grep("^REMOVE ", rec$calls), 0)
})

test_that("an uninterpretable metadata PUT is a publication uncertainty", {
  board <- sf_mock_board()
  meta <- list(
    api_version = 1L, file = c("cars.rds", "wheels.rds"), file_size = 12L,
    created = "20240102T000000Z", pin_hash = "abcdef0123456789",
    type = "rds"
  )
  dir <- withr::local_tempdir(.local_envir = parent.frame())
  paths <- c(
    file.path(dir, "cars.rds"),
    file.path(dir, "wheels.rds")
  )
  writeLines("a", paths[[1]])
  writeLines("b", paths[[2]])
  rec <- sf_mock_transport(
    put = function(sql, calls) {
      if (sum(grepl("^PUT ", calls)) == 3L) NULL else {
        sf_mock_put_response(sql)
      }
    }
  )
  testthat::local_mocked_bindings(
    sf_stage_cmd = rec$responder, .package = "pinsExtras"
  )
  withr::local_options(pins.quiet = TRUE)

  expect_error(
    pinsExtras:::pin_store.pins_board_sf_stage(
      board, "cars", paths, meta, versioned = TRUE, x = NULL
    ),
    class = "pinsExtras_publication_uncertain"
  )
  expect_length(grep("^LIST ", rec$calls), 1)
  expect_length(grep("^PUT ", rec$calls), 3)
  expect_length(grep("^REMOVE ", rec$calls), 0)
})

test_that(
  "unversioned replace with a clean old version issues 3 LIST, 3 PUT, 2 REMOVE",
  {
    board <- sf_mock_board(versioned = TRUE)
    meta <- list(
      api_version = 1L, file = c("cars.rds", "wheels.rds"), file_size = 12L,
      created = "20240102T000000Z", pin_hash = "abcdef0123456789",
      type = "rds"
    )
    oldv <- "20240101T000001Z-oldv"
    dir <- withr::local_tempdir(.local_envir = parent.frame())
    paths <- c(
      file.path(dir, "cars.rds"),
      file.path(dir, "wheels.rds")
    )
    writeLines("a", paths[[1]])
    writeLines("b", paths[[2]])
    rec <- sf_mock_transport(
      list = function(sql, calls) {
        if (endsWith(sql, paste0("cars/'"))) {
          # pin-scoped: the preflight listing shows the old version
          # published; the final listing (its second pin-scoped call)
          # shows the old version gone.
          if (sum(grepl("^LIST ", calls)) == 1L) {
            sf_fixture_listing(
              paste0("cars/", oldv, "/data.txt"), board = board
            )
          } else {
            sf_fixture_listing(board = board)
          }
        } else {
          # confirm LIST scoped to the old version dir: data.txt is gone.
          sf_fixture_listing(board = board)
        }
      }
    )
    testthat::local_mocked_bindings(
      sf_stage_cmd = rec$responder, .package = "pinsExtras"
    )
    withr::local_options(pins.quiet = TRUE)

    out <- expect_no_warning(
      pinsExtras:::pin_store.pins_board_sf_stage(
        board, "cars", paths, meta, versioned = FALSE, x = NULL
      )
    )

    expect_identical(out, "cars")
    expect_length(grep("^LIST ", rec$calls), 3)
    expect_length(grep("^PUT ", rec$calls), 3)
    expect_length(grep("^REMOVE ", rec$calls), 2)
  }
)

test_that(
  "unversioned replace that cannot confirm cleanup warns and still returns",
  {
    board <- sf_mock_board(versioned = TRUE)
    meta <- list(
      api_version = 1L, file = c("cars.rds", "wheels.rds"), file_size = 12L,
      created = "20240102T000000Z", pin_hash = "abcdef0123456789",
      type = "rds"
    )
    oldv <- "20240101T000001Z-oldv"
    dir <- withr::local_tempdir(.local_envir = parent.frame())
    paths <- c(
      file.path(dir, "cars.rds"),
      file.path(dir, "wheels.rds")
    )
    writeLines("a", paths[[1]])
    writeLines("b", paths[[2]])
    rec <- sf_mock_transport(
      list = function(sql) {
        # Every listing still shows the old version: the confirm LIST
        # cannot clear the data.txt, and the final pin listing still sees
        # the old version, so cleanup reports the version as remaining.
        sf_fixture_listing(paste0("cars/", oldv, "/data.txt"), board = board)
      }
    )
    testthat::local_mocked_bindings(
      sf_stage_cmd = rec$responder, .package = "pinsExtras"
    )
    withr::local_options(pins.quiet = TRUE)

    expect_warning(
      out <- pinsExtras:::pin_store.pins_board_sf_stage(
        board, "cars", paths, meta, versioned = FALSE, x = NULL
      ),
      regexp = "cleanup is incomplete",
      class = "pinsExtras_cleanup_incomplete"
    )
    expect_identical(out, "cars")
    # preflight, the failed confirm LIST, and the final pin listing
    expect_length(grep("^LIST ", rec$calls), 3)
    expect_length(grep("^PUT ", rec$calls), 3)
    # only the first data.txt REMOVE before the loop broke
    expect_length(grep("^REMOVE ", rec$calls), 1)
  }
)

test_that("the cleanup warning names the version inside the pin_read call", {
  board <- sf_mock_board(versioned = TRUE)
  meta <- list(
    api_version = 1L, file = c("cars.rds", "wheels.rds"), file_size = 12L,
    created = "20240102T000000Z", pin_hash = "zzz990000",
    type = "rds"
  )
  oldv <- "20240101T000001Z-oldv"
  dir <- withr::local_tempdir(.local_envir = parent.frame())
  paths <- c(
    file.path(dir, "cars.rds"),
    file.path(dir, "wheels.rds")
  )
  writeLines("a", paths[[1]])
  writeLines("b", paths[[2]])
  rec <- sf_mock_transport(
    list = function(sql) {
      # Every listing still shows the old version: the confirm LIST
      # cannot clear the data.txt, so cleanup reports the old version as
      # remaining and warns with the exact read call to use.
      sf_fixture_listing(paste0("cars/", oldv, "/data.txt"), board = board)
    }
  )
  testthat::local_mocked_bindings(
    sf_stage_cmd = rec$responder, .package = "pinsExtras"
  )
  # Widen cli so it does not wrap the pinned read call across lines.
  withr::local_options(pins.quiet = TRUE, cli.width = 300)

  w <- expect_warning(
    out <- pinsExtras:::pin_store.pins_board_sf_stage(
      board, "cars", paths, meta, versioned = FALSE, x = NULL
    ),
    class = "pinsExtras_cleanup_incomplete"
  )
  expect_identical(out, "cars")
  msg <- cli::ansi_strip(conditionMessage(w))
  expect_true(grepl(
    'pin_read(board, "cars", version = "20240102T000000Z-zzz99")',
    msg,
    fixed = TRUE
  ))
})

test_that("a version is invisible to discovery until its data.txt lands", {
  board <- sf_mock_board()
  meta <- list(
    api_version = 1L, file = "cars.rds", file_size = 12L,
    created = "20240102T000000Z", pin_hash = "zzz990000",
    type = "rds"
  )
  v <- sf_version_name(meta)
  # A pre-existing payload-only directory of a different version: it is not
  # published and is not the version being written, so it neither counts as a
  # version nor collides with the new write.
  oldp <- "20240101T000001Z-oldp"
  dir <- withr::local_tempdir(.local_envir = parent.frame())
  paths <- file.path(dir, "cars.rds")
  writeLines("a", paths)
  # The LIST responder reads the record of issued commands: only the payload
  # has been put at first, so listing shows the old payload with no data.txt,
  # unpublished; once the metadata PUT is issued, discovery reports the pin.
  rec <- sf_mock_transport(
    list = function(sql, calls) {
      if (any(grepl("data.txt", calls, fixed = TRUE))) {
        sf_fixture_listing(paste0("cars/", v, "/data.txt"))
      } else {
        sf_fixture_listing(paste0("cars/", oldp, "/cars.rds"))
      }
    }
  )
  testthat::local_mocked_bindings(
    sf_stage_cmd = rec$responder, .package = "pinsExtras"
  )
  withr::local_options(pins.quiet = TRUE)

  # No metadata yet: the payload-only version is not a published pin.
  expect_false(pins::pin_exists(board, "cars"))

  out <- pins::pin_store(
    board, "cars", paths, meta, versioned = FALSE, x = NULL
  )
  expect_identical(out, "cars")

  # The metadata PUT has now been issued, so discovery sees the pin.
  expect_true(pins::pin_exists(board, "cars"))
})
