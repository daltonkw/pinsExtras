# Unit tests for pure helpers. No Snowflake connection, no transport.
#
# Each helper below is a matrix over one input, so each gets one
# table-driven block with a named row per case.

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
  # sf_mock_board() supplies its own cache, which is a deliberate setting
  # and must survive the round trip.
  expect_identical(as.character(rebuilt$cache), as.character(board$cache))

  # A board that took the default cache gets a machine-specific absolute
  # path, so the expression leaves it out and stays portable.
  default_board <- board_sf_stage(
    conn = fake_conn,
    stage = "@mystage",
    path = "team-data",
    connect_args = connect_args
  )
  default_expr <- pins::board_deparse(default_board)
  expect_false("cache" %in% names(as.list(default_expr)))
})

test_that("board_deparse aborts when the board stored no connect_args", {
  board <- sf_mock_board()
  expect_null(board$connect_args)
  expect_error(pins::board_deparse(board), "connect_args")
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
