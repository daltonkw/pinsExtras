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
