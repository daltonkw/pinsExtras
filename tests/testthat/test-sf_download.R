test_that("sf_stage_download fetches a file into a fresh directory", {
  board <- sf_mock_board()
  dest <- withr::local_tempdir()
  rec <- sf_mock_transport(
    get = sf_mock_get_files("data.txt" = "api_version: 1\n")
  )
  testthat::local_mocked_bindings(
    sf_stage_cmd = rec$responder,
    .package = "pinsExtras"
  )

  out <- pinsExtras:::sf_stage_download(board, "cars/v/data.txt", dest)

  expect_length(rec$calls, 1L)
  expect_match(
    rec$calls[[1]],
    "GET '@~/cars/v/'",
    fixed = TRUE
  )
  expect_match(
    rec$calls[[1]],
    "PATTERN = '.*cars/v/data\\\\.txt$'",
    fixed = TRUE
  )
  expect_true(fs::file_exists(fs::path(dest, "data.txt")))
  expect_identical(out, as.character(fs::path(dest, "data.txt")))
})

test_that("sf_stage_download scopes a named stage to its board path", {
  board <- sf_mock_board(path = "team-data", stage = "@mystage")
  dest <- withr::local_tempdir()
  rec <- sf_mock_transport(
    get = sf_mock_get_files("data.txt" = "fresh")
  )
  testthat::local_mocked_bindings(
    sf_stage_cmd = rec$responder,
    .package = "pinsExtras"
  )

  pinsExtras:::sf_stage_download(board, "cars/v/data.txt", dest)

  expect_match(
    rec$calls[[1]],
    "GET '@mystage/team-data/cars/v/'",
    fixed = TRUE
  )
  expect_match(
    rec$calls[[1]],
    "PATTERN = '.*team-data/cars/v/data\\\\.txt$'",
    fixed = TRUE
  )
})

test_that("a failed download does not return a stale cached file", {
  board <- sf_mock_board()
  dest <- withr::local_tempdir()
  writeLines("STALE", fs::path(dest, "data.txt"))
  rec <- sf_mock_transport(
    get = sf_mock_get_files("data.txt" = "fresh", status = "ERROR")
  )
  testthat::local_mocked_bindings(
    sf_stage_cmd = rec$responder,
    .package = "pinsExtras"
  )

  expect_error(
    pinsExtras:::sf_stage_download(board, "cars/v/data.txt", dest),
    class = "pinsExtras_download_failed"
  )
  expect_identical(readLines(fs::path(dest, "data.txt")), "STALE")
})

test_that("a non-downloaded status aborts and names it", {
  board <- sf_mock_board()
  dest <- withr::local_tempdir()
  rec <- sf_mock_transport(
    get = sf_mock_get_files("data.txt" = "x", status = "SKIPPED")
  )
  testthat::local_mocked_bindings(
    sf_stage_cmd = rec$responder,
    .package = "pinsExtras"
  )

  expect_error(
    pinsExtras:::sf_stage_download(board, "cars/v/data.txt", dest),
    "SKIPPED",
    fixed = TRUE
  )
})

test_that("a lowercase downloaded status still succeeds", {
  board <- sf_mock_board()
  dest <- withr::local_tempdir()
  rec <- sf_mock_transport(
    get = sf_mock_get_files("data.txt" = "fresh", status = "downloaded")
  )
  testthat::local_mocked_bindings(
    sf_stage_cmd = rec$responder,
    .package = "pinsExtras"
  )

  out <- pinsExtras:::sf_stage_download(board, "cars/v/data.txt", dest)

  expect_true(fs::file_exists(fs::path(dest, "data.txt")))
  expect_identical(out, as.character(fs::path(dest, "data.txt")))
})

test_that("two download results abort with a count", {
  board <- sf_mock_board()
  dest <- withr::local_tempdir()
  rec <- sf_mock_transport(
    get = data.frame(
      file = c("data.txt", "other.txt"), size = 1,
      status = "DOWNLOADED", message = "", stringsAsFactors = FALSE
    )
  )
  testthat::local_mocked_bindings(
    sf_stage_cmd = rec$responder,
    .package = "pinsExtras"
  )

  expect_error(
    pinsExtras:::sf_stage_download(board, "cars/v/data.txt", dest),
    "returned 2 results for a single-file download",
    fixed = TRUE
  )
})

test_that("zero download results abort with no result", {
  board <- sf_mock_board()
  dest <- withr::local_tempdir()
  rec <- sf_mock_transport(
    get = data.frame(
      file = character(), size = numeric(),
      status = character(), message = character(),
      stringsAsFactors = FALSE
    )
  )
  testthat::local_mocked_bindings(
    sf_stage_cmd = rec$responder,
    .package = "pinsExtras"
  )

  expect_error(
    pinsExtras:::sf_stage_download(board, "cars/v/data.txt", dest),
    "returned no download result",
    fixed = TRUE
  )
})

test_that("a response with no status column is uninterpretable", {
  board <- sf_mock_board()
  dest <- withr::local_tempdir()
  rec <- sf_mock_transport(
    get = data.frame(
      file = "data.txt", size = 1, stringsAsFactors = FALSE
    )
  )
  testthat::local_mocked_bindings(
    sf_stage_cmd = rec$responder,
    .package = "pinsExtras"
  )

  expect_error(
    pinsExtras:::sf_stage_download(board, "cars/v/data.txt", dest),
    "could not be interpreted",
    fixed = TRUE
  )
})

test_that("a wrong file name in the response aborts", {
  board <- sf_mock_board()
  dest <- withr::local_tempdir()
  rec <- sf_mock_transport(
    get = data.frame(
      file = "other.txt", size = 1,
      status = "DOWNLOADED", message = "", stringsAsFactors = FALSE
    )
  )
  testthat::local_mocked_bindings(
    sf_stage_cmd = rec$responder,
    .package = "pinsExtras"
  )

  expect_error(
    pinsExtras:::sf_stage_download(board, "cars/v/data.txt", dest),
    "other.txt",
    fixed = TRUE
  )
})

test_that("a missing transfer file aborts before copying", {
  board <- sf_mock_board()
  dest <- withr::local_tempdir()
  rec <- sf_mock_transport(
    get = sf_mock_get_files(
      "data.txt" = "fresh", missing = "data.txt"
    )
  )
  testthat::local_mocked_bindings(
    sf_stage_cmd = rec$responder,
    .package = "pinsExtras"
  )

  expect_error(
    pinsExtras:::sf_stage_download(board, "cars/v/data.txt", dest),
    "missing from the transfer directory",
    fixed = TRUE
  )
})

test_that("upper-case response columns still succeed", {
  board <- sf_mock_board()
  dest <- withr::local_tempdir()
  rec <- sf_mock_transport(
    get = sf_mock_get_files("data.txt" = "fresh"),
    casing = "upper"
  )
  testthat::local_mocked_bindings(
    sf_stage_cmd = rec$responder,
    .package = "pinsExtras"
  )

  out <- pinsExtras:::sf_stage_download(board, "cars/v/data.txt", dest)

  expect_true(fs::file_exists(fs::path(dest, "data.txt")))
  expect_identical(out, as.character(fs::path(dest, "data.txt")))
})

test_that("sf_check_get_result returns invisible TRUE on success", {
  result <- data.frame(
    file = "data.txt", size = 1024,
    status = "DOWNLOADED", message = "", stringsAsFactors = FALSE
  )

  expect_identical(
    pinsExtras:::sf_check_get_result(result, "data.txt", "cars/v/data.txt"),
    TRUE
  )
})

test_that("sf_read_meta parses valid pin metadata", {
  dir <- withr::local_tempdir()
  meta <- list(
    api_version = 1L, file = "cars.rds", file_size = 1234,
    created = "20240101T000000Z", pin_hash = "abc1234567", type = "rds"
  )
  writeLines(yaml::as.yaml(meta), fs::path(dir, "data.txt"))

  got <- pinsExtras:::sf_read_meta(dir)

  expect_identical(got$api_version, 1L)
  expect_identical(got$file, "cars.rds")
  expect_identical(got$file_size, fs::as_fs_bytes(1234))
  expect_s3_class(got$created, "POSIXct")
  expect_identical(got$user, list())
})

test_that("sf_read_meta aborts when data.txt is missing", {
  dir <- withr::local_tempdir()

  err <- expect_error(
    pinsExtras:::sf_read_meta(dir),
    class = "pinsExtras_download_failed"
  )
  msg <- cli::ansi_strip(conditionMessage(err))
  expect_true(grepl(dir, msg, fixed = TRUE))
  expect_false(grepl(fs::path(dir, "data.txt"), msg, fixed = TRUE))
})

test_that("sf_read_meta re-raises a parse error with a yaml parent", {
  dir <- withr::local_tempdir()
  writeLines("foo: 1\nfoo: 2", fs::path(dir, "data.txt"))

  cond <- testthat::expect_error(
    pinsExtras:::sf_read_meta(dir),
    "could not be parsed",
    class = "pinsExtras_download_failed"
  )

  expect_true(!is.null(cond$parent))
  expect_s3_class(cond$parent, "simpleError")
})

test_that("sf_read_meta parse error names the directory", {
  dir <- withr::local_tempdir()
  writeLines("foo: 1\nfoo: 2", fs::path(dir, "data.txt"))

  err <- expect_error(
    pinsExtras:::sf_read_meta(dir),
    class = "pinsExtras_download_failed"
  )
  msg <- cli::ansi_strip(conditionMessage(err))
  expect_true(grepl(dir, msg, fixed = TRUE))
  expect_true(grepl("could not be parsed", msg, fixed = TRUE))
})

test_that("sf_check_get_result rejects a file_size response without file", {
  cond <- expect_error(
    pinsExtras:::sf_check_get_result(
      data.frame(
        file_size = 1, size = 1, status = "DOWNLOADED",
        stringsAsFactors = FALSE
      ),
      "data.txt",
      "cars/v/data.txt"
    ),
    class = "pinsExtras_download_failed"
  )
})

test_that("sf_stage_download emits the full GET with PATTERN at the board root",
{
  board <- sf_mock_board()
  dest <- withr::local_tempdir()
  rec <- sf_mock_transport(
    get = sf_mock_get_files("data.txt" = "fresh")
  )
  testthat::local_mocked_bindings(
    sf_stage_cmd = rec$responder,
    .package = "pinsExtras"
  )

  pinsExtras:::sf_stage_download(board, "cars/v/data.txt", dest)

  # Anchored at both ends with only the unpredictable temp URI wild-carded,
  # so the LOCATION and the full PATTERN clause are pinned exactly.
  expect_match(rec$calls[[1]], "GET '@~/cars/v/' 'file://", fixed = TRUE)
  expect_match(
    rec$calls[[1]],
    "PATTERN = '.*cars/v/data\\\\.txt$'",
    fixed = TRUE
  )
})

test_that("sf_stage_download emits the full GET for a named stage and board path",
{
  board <- sf_mock_board(path = "team-data", stage = "@mystage")
  dest <- withr::local_tempdir()
  rec <- sf_mock_transport(
    get = sf_mock_get_files("data.txt" = "fresh")
  )
  testthat::local_mocked_bindings(
    sf_stage_cmd = rec$responder,
    .package = "pinsExtras"
  )

  pinsExtras:::sf_stage_download(board, "cars/v/data.txt", dest)

  expect_match(rec$calls[[1]], "GET '@mystage/team-data/cars/v/' 'file://",
    fixed = TRUE
  )
  expect_match(
    rec$calls[[1]],
    "PATTERN = '.*team-data/cars/v/data\\\\.txt$'",
    fixed = TRUE
  )
})

test_that("sf_stage_download escapes a dotted name and scopes to its directory",
{
  board <- sf_mock_board()
  dest <- withr::local_tempdir()
  rec <- sf_mock_transport(
    get = sf_mock_get_files("my.pin.rds" = "fresh")
  )
  testthat::local_mocked_bindings(
    sf_stage_cmd = rec$responder,
    .package = "pinsExtras"
  )

  pinsExtras:::sf_stage_download(board, "cars/v/my.pin.rds", dest)

  expect_match(rec$calls[[1]], "GET '@~/cars/v/'", fixed = TRUE)
  expect_match(
    rec$calls[[1]],
    "PATTERN = '.*cars/v/my\\\\.pin\\\\.rds$'",
    fixed = TRUE
  )
})

test_that("sf_stage_download scopes the board root when the key has no directory",
{
  board <- sf_mock_board()
  dest <- withr::local_tempdir()
  rec <- sf_mock_transport(
    get = sf_mock_get_files("_pins.yaml" = "fresh")
  )
  testthat::local_mocked_bindings(
    sf_stage_cmd = rec$responder,
    .package = "pinsExtras"
  )

  pinsExtras:::sf_stage_download(board, "_pins.yaml", dest)

  # fs::path_dir("_pins.yaml") is "", mapped to the board root, so the
  # LOCATION is "@~" with no trailing slash and the PATTERN still anchors it.
  expect_match(rec$calls[[1]], "GET '@~' 'file://", fixed = TRUE)
  expect_match(
    rec$calls[[1]],
    "PATTERN = '.*/_pins\\\\.yaml$'",
    fixed = TRUE
  )
})

test_that("sf_stage_download narrows a report/report.pdf collision to report",
{
  board <- sf_mock_board()
  dest <- withr::local_tempdir()
  # The mock returns whatever the PATTERN names, so this cannot reproduce the
  # live two-row read offline; what it pins is the emitted PATTERN, which is
  # the whole fix. A bare "report" location would match report.pdf too.
  rec <- sf_mock_transport(
    get = sf_mock_get_files("report" = "fresh")
  )
  testthat::local_mocked_bindings(
    sf_stage_cmd = rec$responder,
    .package = "pinsExtras"
  )

  pinsExtras:::sf_stage_download(board, "cars/v/report", dest)

  expect_match(
    rec$calls[[1]],
    "PATTERN = '.*cars/v/report$'",
    fixed = TRUE
  )
})
