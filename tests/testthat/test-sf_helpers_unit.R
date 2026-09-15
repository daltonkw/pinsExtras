# Unit tests for pure helpers. No Snowflake connection, no transport.
#
# Each helper below is a matrix over one input, so each gets one
# table-driven block with a named row per case.

# ---- sf_normalize_path --------------------------------------------------

test_that("sf_normalize_path yields the exact stage-root-relative path", {
  # Every SQL location is built from this, so each case asserts the exact
  # string. Asserting only "no leading //" would pass against a strip that
  # produced the wrong path.
  cases <- list(
    list(name = "board path and dir",  path = "base/path",  dir = "subdir",
         want = "base/path/subdir"),
    list(name = "empty board path",    path = "",           dir = "subdir",
         want = "subdir"),
    list(name = "empty path and dir",  path = "",           dir = "",
         want = ""),
    list(name = "leading slash",       path = "/leading",   dir = "subdir",
         want = "leading/subdir"),
    list(name = "doubled slashes",     path = "path//with", dir = "//double",
         want = "path/with/double")
  )
  for (case in cases) {
    expect_identical(
      as.character(
        pinsExtras:::sf_normalize_path(list(path = case$path), case$dir)
      ),
      case$want,
      info = case$name
    )
  }
})

# ---- version parsing ----------------------------------------------------

test_that("sf_version_from_path parses a version into created and hash", {
  versions <- c("20231215T103045Z-abc12", "20240101T000000Z-xyz99")
  result <- pinsExtras:::sf_version_from_path(versions)

  expect_s3_class(result, "tbl_df")
  expect_equal(nrow(result), 2)
  expect_identical(result$version, versions)
  expect_identical(result$hash, c("abc12", "xyz99"))
  expect_false(any(is.na(result$created)))
  # The only place the parsed instant itself is pinned: a format change
  # would silently shift every version's created column.
  expect_equal(
    format(result$created[[1]], "%Y-%m-%d %H:%M:%S", tz = "UTC"),
    "2023-12-15 10:30:45"
  )

  # The instant comes from sf_parse_8601_compact(), which is asserted
  # here directly rather than in a block of its own.
  parsed <- pinsExtras:::sf_parse_8601_compact("20231215T103045Z")
  expect_s3_class(parsed, "POSIXct")
  expect_equal(
    format(parsed, "%Y-%m-%d %H:%M:%S", tz = "UTC"), "2023-12-15 10:30:45"
  )
})

test_that("sf_version_from_path leaves both columns NA for a malformed id", {
  cases <- list(
    list(name = "missing hash",   value = "20231215T103045Z"),
    list(name = "not a version",  value = "not-a-version")
  )
  for (case in cases) {
    result <- pinsExtras:::sf_version_from_path(case$value)
    expect_true(is.na(result$hash), info = case$name)
    expect_true(is.na(result$created), info = case$name)
  }
  expect_equal(nrow(pinsExtras:::sf_version_from_path(character(0))), 0)
})

# ---- the public constructor --------------------------------------------

test_that("board_sf_stage validates inputs", {
  # The only offline test of the constructor's argument checks; every
  # other one sits behind skip_if_no_sf_stage().
  expect_error(
    board_sf_stage(conn = NULL, stage = "@~"),
    "DBI connection"
  )
  expect_error(
    board_sf_stage(conn = "not-a-connection", stage = "@~"),
    "DBI connection"
  )
  expect_error(
    board_sf_stage(
      conn = structure(list(), class = "DBIConnection"), stage = 123
    ),
    "must be a string"
  )
})

test_that("board_deparse rebuilds an equivalent board", {
  # board_deparse() is the documented way to reproduce a board, and it has
  # no live-free test otherwise. Evaluating what it returns proves the
  # expression is not merely well-formed but actually rebuilds the board.
  withr::local_envvar(PINS_CACHE_DIR = withr::local_tempdir())
  connect_args <- list(Driver = "Snowflake", UID = "someone")
  board <- sf_mock_board(
    path = "team-data", stage = "@mystage", versioned = FALSE
  )
  board$connect_args <- connect_args

  expr <- pins::board_deparse(board)
  expect_true(is.call(expr))

  fake_conn <- structure(list(), class = c("sf_mock_conn", "DBIConnection"))
  testthat::local_mocked_bindings(
    dbConnect = function(drv, ...) fake_conn,
    .package = "DBI"
  )
  testthat::local_mocked_bindings(
    sf_stage_cmd = function(board, sql) {
      stop("board_deparse() must not issue SQL")
    },
    .package = "pinsExtras"
  )

  rebuilt <- eval(expr)

  expect_s3_class(rebuilt, "pins_board_sf_stage")
  expect_identical(rebuilt$stage, board$stage)
  expect_identical(rebuilt$path, board$path)
  expect_identical(rebuilt$versioned, board$versioned)
  expect_identical(rebuilt$connect_args, connect_args)
})

test_that("board_deparse aborts when the board stored no connect_args", {
  board <- sf_mock_board()
  expect_null(board$connect_args)
  expect_error(pins::board_deparse(board), "connect_args")
})

# ---- stage name extraction ---------------------------------------------

test_that("sf_extract_stage_name takes the last dotted component", {
  cases <- list(
    list(name = "simple",        stage = "@mystage",              want = "mystage"),
    list(name = "user stage",    stage = "@~",                    want = "~"),
    list(name = "fully qualified", stage = "@mydb.myschema.mystage",
         want = "mystage"),
    list(name = "two-part",      stage = "@myschema.mystage",     want = "mystage"),
    list(name = "no @ prefix",   stage = "mystage",               want = "mystage")
  )
  for (case in cases) {
    expect_identical(
      pinsExtras:::sf_extract_stage_name(case$stage),
      case$want,
      info = case$name
    )
  }
})

# ---- SQL literal quoting -----------------------------------------------

test_that("the SQL quoting helpers escape and wrap every literal", {
  literal <- pinsExtras:::sf_quote_sql_literal
  stage_path <- pinsExtras:::sf_quote_stage_path
  file_uri <- pinsExtras:::sf_quote_file_uri

  cases <- list(
    list(name = "plain text",        fn = literal, x = "abc",
         want = "'abc'"),
    list(name = "apostrophe",        fn = literal, x = "bob's data",
         want = "'bob\\'s data'"),
    # Backslashes are doubled BEFORE quotes are escaped, so an escaped
    # quote is not double-escaped. Reversing the two gsub calls breaks
    # this row and nothing else.
    list(name = "backslash",         fn = literal, x = "a\\b",
         want = "'a\\\\b'"),
    list(name = "empty string",      fn = literal, x = "",
         want = "''"),
    list(name = "stage root",        fn = stage_path, x = "@~",
         want = "'@~'"),
    list(name = "stage directory",   fn = stage_path,
         x = "@~/team-data/cars/", want = "'@~/team-data/cars/'"),
    list(name = "qualified stage",   fn = stage_path,
         x = "@db.schema.stage/x", want = "'@db.schema.stage/x'"),
    # file_uri adds the file:// prefix on top of the same quoting.
    list(name = "local file",        fn = file_uri, x = "/tmp/x/data.txt",
         want = "'file:///tmp/x/data.txt'"),
    list(name = "local file with apostrophe", fn = file_uri,
         x = "/tmp/o'brien/data.txt",
         want = "'file:///tmp/o\\'brien/data.txt'")
  )
  for (case in cases) {
    expect_identical(case$fn(case$x), case$want, info = case$name)
  }
})

# ---- sf_escape_regex ----------------------------------------------------

test_that("sf_escape_regex escapes exactly the Java metacharacters", {
  # Snowflake's REMOVE ... PATTERN uses Java's regex engine, so the
  # function escapes exactly the 14 characters Java treats as special and
  # nothing else. One row per character, plus the characters that must
  # stay untouched.
  escaped <- list(
    list(name = "backslash",     x = "\\", want = "\\\\"),
    list(name = "caret",         x = "^",  want = "\\^"),
    list(name = "dollar",        x = "$",  want = "\\$"),
    list(name = "dot",           x = ".",  want = "\\."),
    list(name = "pipe",          x = "|",  want = "\\|"),
    list(name = "question mark", x = "?",  want = "\\?"),
    list(name = "asterisk",      x = "*",  want = "\\*"),
    list(name = "plus",          x = "+",  want = "\\+"),
    list(name = "open paren",    x = "(",  want = "\\("),
    list(name = "close paren",   x = ")",  want = "\\)"),
    list(name = "open bracket",  x = "[",  want = "\\["),
    list(name = "close bracket", x = "]",  want = "\\]"),
    list(name = "open brace",    x = "{",  want = "\\{"),
    list(name = "close brace",   x = "}",  want = "\\}")
  )
  untouched <- list(
    list(name = "hyphen",        x = "-",     want = "-"),
    list(name = "slash",         x = "/",     want = "/"),
    list(name = "hyphenated",    x = "a-b",   want = "a-b"),
    list(name = "slashed",       x = "a/b",   want = "a/b"),
    list(name = "plain string",  x = "plain", want = "plain"),
    # nchar(s) == 0 takes the early return.
    list(name = "empty string",  x = "",      want = "")
  )
  composite <- list(
    list(name = "dotted file",   x = "data.txt", want = "data\\.txt"),
    list(name = "backslash in text", x = "a\\b", want = "a\\\\b")
  )
  for (case in c(escaped, untouched, composite)) {
    expect_identical(
      pinsExtras:::sf_escape_regex(case$x), case$want, info = case$name
    )
  }

  # Vectorised, and the names attribute vapply() leaves behind is stripped:
  # a character(0) input must come back as character(0), not a named one.
  expect_identical(
    pinsExtras:::sf_escape_regex(c("a.b", "c")), c("a\\.b", "c")
  )
  expect_identical(
    pinsExtras:::sf_escape_regex(character(0)), character(0)
  )
})

# ---- the two PATTERN builders ------------------------------------------

test_that("sf_remove_pattern is anchored to its directory", {
  # The stage root carries no parent to scope against, so the leading-path
  # group is dropped and the pattern is anchored to the file alone.
  expect_identical(
    pinsExtras:::sf_remove_pattern("", "data.txt"),
    "^data\\.txt$"
  )
  # Otherwise the directory is part of the pattern, with an optional leading
  # group so it matches whether Snowflake sees a bare relative name or the
  # full staged path under that directory.
  expect_identical(
    pinsExtras:::sf_remove_pattern("cars/V", "data.txt"),
    "^(cars/V/)?data\\.txt$"
  )
  # A dot in the directory is escaped like any other component.
  expect_identical(
    pinsExtras:::sf_remove_pattern("a.b", "data.txt"),
    "^(a\\.b/)?data\\.txt$"
  )
})

test_that("sf_remove_pattern's grepl match is scoped to its directory", {
  pat <- pinsExtras:::sf_remove_pattern("cars/V", "data.txt")
  expect_true(grepl(pat, "data.txt"))
  expect_true(grepl(pat, "cars/V/data.txt"))
  expect_false(grepl(pat, "child/data.txt"))
  expect_false(grepl(pat, "cars/V/child/data.txt"))
  expect_false(grepl(pat, "data.txt.bak"))
  expect_false(grepl(pat, "cars/V/cars.rds"))
  expect_false(grepl(pat, "mystage/cars/V/data.txt"))
})

test_that("sf_get_pattern is anchored to its directory", {
  expect_identical(
    pinsExtras:::sf_get_pattern("", "data.txt"),
    ".*/data\\.txt$"
  )
  # The leading token is a bare ".*" with NO slash: Snowflake prepends a
  # stage-name prefix with no separator before our directory on the user
  # stage, so the escaped directory still has to line up after the star.
  # A well-meaning refactor that adds the slash makes every read match
  # zero rows; this was established by a live probe.
  expect_identical(
    pinsExtras:::sf_get_pattern("cars/V", "data.txt"),
    ".*cars/V/data\\.txt$"
  )
  expect_identical(
    pinsExtras:::sf_get_pattern("a.b", "my.pin.rds"),
    ".*a\\.b/my\\.pin\\.rds$"
  )
})

test_that("sf_get_pattern's grepl match is scoped to its directory", {
  pat <- pinsExtras:::sf_get_pattern("cars/V", "data.txt")
  expect_true(grepl(pat, "stage/cars/V/data.txt"))
  # The bare ".*" absorbs any stage prefix, so the relative name alone
  # still matches: only the directory scope rejects a sibling.
  expect_true(grepl(pat, "cars/V/data.txt"))
  expect_false(grepl(pat, "child/data.txt"))
  expect_false(grepl(pat, "cars/V/child/data.txt"))
  expect_false(grepl(pat, "stage/cars/V/child/data.txt"))
  expect_false(grepl(pat, "stage/cars/V/data.txt.bak"))
})
