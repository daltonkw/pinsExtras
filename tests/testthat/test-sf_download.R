# The GET verb, its response validation, and reading the metadata it
# fetches. Mock only the transport.

test_that("sf_stage_download emits the scoped GET and its anchored PATTERN", {
  # One table over the shapes the GET LOCATION and PATTERN take. The
  # LOCATION names the DIRECTORY, never the file, so Snowflake's prefix
  # matching cannot pull in a sibling; the PATTERN then picks the one
  # file, anchored at the end.
  cases <- list(
    list(
      name = "user stage",
      board = list(), key = "cars/v/data.txt", file = "data.txt",
      location = "GET '@~/cars/v/' 'file://",
      pattern = "PATTERN = '.*cars/v/data\\\\.txt$'"
    ),
    list(
      name = "named stage and board path",
      board = list(path = "team-data", stage = "@mystage"),
      key = "cars/v/data.txt", file = "data.txt",
      location = "GET '@mystage/team-data/cars/v/' 'file://",
      pattern = "PATTERN = '.*team-data/cars/v/data\\\\.txt$'"
    ),
    list(
      # Every dot in the name is escaped, so a dotted pin reads back.
      name = "dotted file name",
      board = list(), key = "cars/v/my.pin.rds", file = "my.pin.rds",
      location = "GET '@~/cars/v/' 'file://",
      pattern = "PATTERN = '.*cars/v/my\\\\.pin\\\\.rds$'"
    ),
    list(
      # fs::path_dir("_pins.yaml") is ".", which maps to the board root:
      # the LOCATION carries no trailing slash and the PATTERN keeps the
      # ".*/" prefix.
      name = "board root",
      board = list(), key = "_pins.yaml", file = "_pins.yaml",
      location = "GET '@~' 'file://",
      pattern = "PATTERN = '.*/_pins\\\\.yaml$'"
    ),
    list(
      # The "$" anchor is what keeps a bare "report" from also matching
      # report.pdf in the same directory.
      name = "extensionless file beside a longer sibling",
      board = list(), key = "cars/v/report", file = "report",
      location = "GET '@~/cars/v/' 'file://",
      pattern = "PATTERN = '.*cars/v/report$'"
    )
  )
  for (case in cases) {
    board <- do.call(sf_mock_board, case$board)
    dest <- withr::local_tempdir()
    contents <- list("fresh")
    names(contents) <- case$file
    rec <- sf_mock_bind(get = do.call(sf_mock_get_files, contents))

    out <- pinsExtras:::sf_stage_download(board, case$key, dest)

    expect_identical(length(rec$calls), 1L, info = case$name)
    expect_match(rec$calls[[1]], case$location, fixed = TRUE, info = case$name)
    expect_match(rec$calls[[1]], case$pattern, fixed = TRUE, info = case$name)
    expect_true(fs::file_exists(fs::path(dest, case$file)), info = case$name)
    expect_identical(
      out, as.character(fs::path(dest, case$file)), info = case$name
    )
  }
})

test_that("a failed download does not return a stale cached file", {
  # The transfer goes into a fresh temporary directory and is copied out
  # only on success, so a stale file already in the destination can never
  # stand in for what this call actually fetched. A refactor that
  # downloaded straight into the destination would break this silently.
  board <- sf_mock_board()
  dest <- withr::local_tempdir()
  writeLines("STALE", fs::path(dest, "data.txt"))
  rec <- sf_mock_bind(
    get = sf_mock_get_files("data.txt" = "fresh", status = "ERROR")
  )

  expect_error(
    pinsExtras:::sf_stage_download(board, "cars/v/data.txt", dest),
    class = "pinsExtras_download_failed"
  )
  expect_identical(readLines(fs::path(dest, "data.txt")), "STALE")
})

test_that("a validated transfer that never landed aborts before copying", {
  # A branch in sf_stage_download() itself, not in the checker: the
  # response says DOWNLOADED but no file arrived, so the filesystem, not
  # the response, has the last word.
  board <- sf_mock_board()
  dest <- withr::local_tempdir()
  rec <- sf_mock_bind(
    get = sf_mock_get_files("data.txt" = "fresh", missing = "data.txt")
  )

  expect_error(
    pinsExtras:::sf_stage_download(board, "cars/v/data.txt", dest),
    "missing from the transfer directory",
    fixed = TRUE
  )
})

# ---- sf_check_get_result ------------------------------------------------

test_that("sf_check_get_result accepts an interpretable DOWNLOADED response", {
  board <- sf_mock_board()
  cases <- list(
    list(name = "DOWNLOADED", status = "DOWNLOADED", casing = "lower"),
    # Status is matched case-insensitively ...
    list(name = "lower-case status", status = "downloaded", casing = "lower"),
    # ... and so are the driver's column names, which are not guaranteed
    # across ODBC driver versions.
    list(name = "upper-case columns", status = "DOWNLOADED", casing = "upper")
  )
  for (case in cases) {
    dest <- withr::local_tempdir()
    rec <- sf_mock_bind(
      get = sf_mock_get_files("data.txt" = "fresh", status = case$status),
      casing = case$casing
    )

    out <- pinsExtras:::sf_stage_download(board, "cars/v/data.txt", dest)

    expect_true(fs::file_exists(fs::path(dest, "data.txt")), info = case$name)
    expect_identical(
      out, as.character(fs::path(dest, "data.txt")), info = case$name
    )
  }
})

test_that("sf_check_get_result rejects every response it cannot trust", {
  cases <- list(
    list(
      name = "NULL response", response = NULL, says = "no download result"
    ),
    list(
      name = "zero rows", response = sf_fixture_get_response(n = 0L),
      says = "no download result"
    ),
    list(
      name = "two rows",
      response = sf_fixture_get_response(file = c("data.txt", "other.txt"),
                                         n = 2L),
      says = "returned 2 results for a single-file download"
    ),
    list(
      name = "no status column",
      response = sf_fixture_get_response(drop = "status"),
      says = "could not be interpreted"
    ),
    list(
      # A response carrying file_size but no file must not slip past a
      # partial-matching `$` lookup.
      name = "file_size but no file",
      response = sf_fixture_get_response(
        drop = "file", extra = list(file_size = 1)
      ),
      says = "could not be interpreted"
    ),
    list(
      name = "NA status",
      response = sf_fixture_get_response(status = NA_character_),
      says = "could not be interpreted"
    ),
    list(
      name = "NA file",
      response = sf_fixture_get_response(file = NA_character_),
      says = "could not be interpreted"
    ),
    list(
      name = "some other status",
      response = sf_fixture_get_response(status = "SKIPPED"),
      says = "SKIPPED"
    ),
    list(
      name = "wrong file name",
      response = sf_fixture_get_response(file = "other.txt"),
      says = "other.txt"
    )
  )
  for (case in cases) {
    expect_error(
      pinsExtras:::sf_check_get_result(
        case$response, "data.txt", "cars/v/data.txt"
      ),
      class = "pinsExtras_download_failed",
      regexp = case$says,
      fixed = TRUE,
      info = case$name
    )
  }
})

# ---- sf_read_meta -------------------------------------------------------

test_that("sf_read_meta parses valid pin metadata", {
  # The only place the api_version 1 coercions are pinned: file_size
  # becomes fs_bytes, created becomes POSIXct, and an absent user field
  # defaults to an empty list.
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

test_that("sf_read_meta reports an unreadable data.txt and names its directory", {
  missing_dir <- withr::local_tempdir()
  broken_dir <- withr::local_tempdir()
  writeLines("foo: 1\nfoo: 2", fs::path(broken_dir, "data.txt"))

  cases <- list(
    list(name = "data.txt absent", dir = missing_dir, says = "is missing from"),
    list(name = "data.txt unparseable", dir = broken_dir,
         says = "could not be parsed")
  )
  for (case in cases) {
    err <- expect_error(
      pinsExtras:::sf_read_meta(case$dir),
      class = "pinsExtras_download_failed",
      info = case$name
    )
    msg <- cli::ansi_strip(conditionMessage(err))
    expect_true(grepl(case$says, msg, fixed = TRUE), info = case$name)
    expect_true(grepl(case$dir, msg, fixed = TRUE), info = case$name)
  }

  # A parse failure chains the underlying error rather than replacing it,
  # so the original yaml diagnostic is still reachable.
  chained <- expect_error(
    pinsExtras:::sf_read_meta(broken_dir),
    class = "pinsExtras_download_failed"
  )
  expect_false(is.null(chained$parent))
  expect_s3_class(chained$parent, "simpleError")
})
