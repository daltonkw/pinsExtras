# Security hardening: a path segment can never delete the whole board, a
# discovered name carrying a separator or dot-segment is never trusted, and
# no attacker-controlled string is ever echoed back or pasted into code.

# ---- sf_check_path_segment(): the validator -------------------------------

test_that("sf_check_path_segment accepts and rejects the right segments", {
  accepted <- list(
    list(name = "ordinary name",     value = "cars"),
    # A version id that will never parse is still a valid segment: that is
    # the escape hatch pin_version_delete() relies on.
    list(name = "unparseable version", value = "bogus-def12"),
    list(name = "well-formed version", value = sf_fixture_version()),
    list(name = "single dot inside",  value = "my.pin"),
    list(name = "mixed punctuation",  value = "a_b-c.d")
  )
  for (case in accepted) {
    expect_identical(
      pinsExtras:::sf_check_path_segment(case$value), TRUE, info = case$name
    )
  }

  rejected <- list(
    list(name = "empty string",     value = ""),
    list(name = "NA_character_",    value = NA_character_),
    list(name = "logical NA",       value = NA),
    list(name = "zero-length",      value = character(0)),
    list(name = "multi-element",    value = c("a", "b")),
    list(name = "non-string",       value = 123),
    list(name = "bare slash",       value = "/"),
    list(name = "nested path",      value = "a/b"),
    list(name = "dotdot",           value = ".."),
    list(name = "single dot",       value = "."),
    list(name = "traversal inside", value = "a/../b"),
    list(name = "hidden dotdot",    value = "..hidden"),
    list(name = "dotdot embedded",  value = "a..b"),
    list(name = "backslash",        value = "a\\b")
  )
  for (case in rejected) {
    expect_error(
      pinsExtras:::sf_check_path_segment(case$value),
      class = "pinsExtras_invalid_path_segment",
      info = case$name
    )
  }
})

test_that("the abort message carries the class but never repeats the value", {
  # The value is attacker-controlled, so the message says what was
  # required and never echoes what was supplied.
  cnd <- tryCatch(
    pinsExtras:::sf_check_path_segment("SENTINEL/../x"),
    error = function(e) e
  )
  expect_true("pinsExtras_invalid_path_segment" %in% class(cnd))
  expect_false(grepl("SENTINEL", conditionMessage(cnd), fixed = TRUE))
})

# ---- SEC-01: discovered names are filtered out of the index ---------------

test_that("pin_list drops a discovered name or version it cannot trust", {
  # Both filter arms at once: a pin name of ".." and a version carrying a
  # backslash. A row that fails validation disappears silently, because
  # warning would paste an attacker-chosen string into the console.
  board <- sf_mock_board(stage = "@~")
  rec <- sf_mock_bind(
    list = sf_fixture_listing(
      "../20240101T000000Z-abc12/data.txt",
      "bad/20240101T000000Z-a\\\\b/data.txt",
      "cars/20240101T000000Z-abc12/data.txt",
      board = board
    )
  )

  expect_identical(pins::pin_list(board), "cars")
})

test_that("pin_meta on a traversal name is not found and issues no GET", {
  # pin_meta() does not run sf_check_path_segment(): the index filter
  # alone is what stops a ".." cache path from ever being built.
  board <- sf_mock_board(stage = "@~")
  rec <- sf_mock_bind(
    list = sf_fixture_listing(
      "../20240101T000000Z-abc12/data.txt",
      board = board
    )
  )

  expect_error(
    pins::pin_meta(board, ".."),
    "Can't find pin called",
    fixed = TRUE
  )
  expect_length(grep("^LIST ", rec$calls), 1L)
  expect_length(grep("^GET ", rec$calls), 0L)
})

# ---- SEC-02: sf_read_meta() rejects an untrusted payload name --------------
# Driven through the real read path: the LIST resolves the version, the GET
# serves crafted metadata, and pin_meta() reads it through sf_read_meta().

sf_sec_meta <- function(file, absent = FALSE) {
  base <- list(
    api_version = 1L,
    created = "20240101",
    pin_hash = "abc1234567",
    type = "txt"
  )
  if (!absent) {
    base$file <- file
  }
  base
}

sf_sec_read_meta <- function(meta, envir = parent.frame()) {
  board <- sf_mock_board(stage = "@~", cache = withr::local_tempdir(.local_envir = envir))
  rec <- sf_mock_bind(
    list = sf_fixture_listing(
      "cars/20240101T000000Z-abc12/data.txt", board = board
    ),
    get = sf_mock_get_files("data.txt" = yaml::as.yaml(meta)),
    .envir = envir
  )
  board
}

test_that("pin_meta rejects every untrusted file field in metadata", {
  cases <- list(
    list(name = "deep traversal",   file = "../../../../private.csv"),
    list(name = "parent-relative",  file = "../sibling.csv"),
    list(name = "backslash",        file = "a\\b.csv"),
    list(name = "reserved marker",  file = "data.txt"),
    list(name = "empty string",     file = ""),
    list(name = "no file field",    file = NULL, absent = TRUE),
    list(name = "duplicate entry",  file = c("a.csv", "a.csv")),
    list(name = "one bad of two",   file = c("a.csv", "../b.csv"))
  )
  for (case in cases) {
    meta <- sf_sec_meta(case$file, absent = isTRUE(case$absent))
    board <- sf_sec_read_meta(meta)
    expect_error(
      pins::pin_meta(board, "cars"),
      class = "pinsExtras_download_failed",
      info = case$name
    )
  }
})

test_that("pin_meta rejects a traversal file without echoing the payload", {
  board <- sf_sec_read_meta(sf_sec_meta("../../../../../SENTINEL.csv"))

  cond <- expect_error(
    pins::pin_meta(board, "cars"),
    class = "pinsExtras_download_failed"
  )
  msg <- cli::ansi_strip(conditionMessage(cond))
  expect_false(grepl("SENTINEL", msg, fixed = TRUE))
})

test_that("pin_meta accepts a clean, duplicate-free file list", {
  # The negative control: the validator must not reject valid metadata.
  board <- sf_sec_read_meta(sf_sec_meta(c("a.csv", "b.csv")))

  out <- pins::pin_meta(board, "cars")
  expect_identical(out$file, c("a.csv", "b.csv"))
})

# ---- SEC-06: a write whose version will not parse moves nothing -----------

test_that("undiscoverable metadata aborts the write before any command", {
  # The whole point is that nothing is uploaded and nothing is deleted, so
  # each row asserts zero recorded commands.
  cases <- list(
    list(
      name = "created does not parse",
      meta = list(
        api_version = 1L, file = "cars.rds", file_size = 12L,
        created = "bogus", pin_hash = "def12", type = "rds"
      )
    ),
    list(
      name = "no created field",
      meta = list(
        api_version = 1L, file = "cars.rds", file_size = 12L,
        pin_hash = "abcdef0123456789", type = "rds"
      )
    ),
    list(
      name = "no pin_hash field",
      meta = list(
        api_version = 1L, file = "cars.rds", file_size = 12L,
        created = "20240102T000000Z", type = "rds"
      )
    )
  )
  dir <- withr::local_tempdir()
  paths <- file.path(dir, "cars.rds")
  writeLines("a", paths)
  withr::local_options(pins.quiet = TRUE)

  for (case in cases) {
    board <- sf_mock_board()
    rec <- sf_mock_bind(list = sf_fixture_listing())
    expect_error(
      pinsExtras:::pin_store.pins_board_sf_stage(
        board, "cars", paths, case$meta, versioned = TRUE, x = NULL
      ),
      class = "pinsExtras_invalid_upload_set",
      info = case$name
    )
    expect_identical(length(rec$calls), 0L, info = case$name)
  }
})

# ---- SEC-10 site 1: the reconnection guidance parses to one statement -----
#
# The board_sf_stage() call is built with rlang::expr() and deparse(), so
# the stage and path become R string literals. An injection pasted into
# `path` must parse back to a single statement, not a second call.

# A board the real constructor built, whose connection reports itself
# dead. DBI::dbIsValid() is mocked rather than an S4 class being defined,
# so the board under test is exactly the shape users hold.
sf_sec_dead_connection <- function(envir = parent.frame()) {
  testthat::local_mocked_bindings(
    dbIsValid = function(dbObj, ...) FALSE,
    .package = "DBI",
    .env = envir
  )
}

sf_sec_dead_board <- function(path, connect_args, envir = parent.frame()) {
  board_sf_stage(
    conn = structure(list(), class = c("sf_mock_conn", "DBIConnection")),
    stage = "@~",
    path = path,
    connect_args = connect_args,
    cache = withr::local_tempdir(.local_envir = envir)
  )
}

sf_sec_guidance <- function(board) {
  msg <- cli::ansi_strip(
    tryCatch(
      pinsExtras:::sf_check_connection(board),
      error = function(e) conditionMessage(e)
    )
  )
  lines <- strsplit(msg, "\n", fixed = TRUE)[[1]]
  hit <- grep("board <- ", lines, fixed = TRUE)
  code <- paste(lines[seq(hit, length(lines))], collapse = " ")
  trimws(sub("^.*?board <- ", "", code))
}

# Ordinary, quote, backtick and a statement injection. All four are legal
# board paths, so all four reach the message builder.
sf_sec_paths <- list(
  "team-data",
  "a\"b",
  "a`b",
  "x\"); message(\"AUDIT_SENTINEL\"); #"
)

test_that("reconnection guidance parses as one statement with connect_args", {
  withr::local_options(cli.width = 300)
  sf_sec_dead_connection()
  for (path in sf_sec_paths) {
    board <- sf_sec_dead_board(path, list(Driver = "x"))
    code <- sf_sec_guidance(board)
    # `...` is spliced as a SYMBOL, so deparse() closes the call and text
    # appended after it lands outside the parentheses.
    expect_match(code, "connect_args = ...", fixed = TRUE, info = path)
    expect_identical(length(parse(text = code)), 1L, info = path)
    if (grepl("AUDIT_SENTINEL", path, fixed = TRUE)) {
      # The injected payload survives as one escaped R string literal, so
      # it is visible in the output but never becomes a second call.
      expect_true(grepl("AUDIT_SENTINEL", code, fixed = TRUE), info = path)
      expect_true(grepl("\\", code, fixed = TRUE), info = path)
    }
  }
})

test_that("reconnection guidance parses as one statement without them", {
  withr::local_options(cli.width = 300)
  sf_sec_dead_connection()
  for (path in sf_sec_paths) {
    board <- sf_sec_dead_board(path, NULL)
    code <- sf_sec_guidance(board)
    expect_no_match(code, "connect_args", fixed = TRUE, info = path)
    expect_identical(length(parse(text = code)), 1L, info = path)
  }
})

# ---- SEC-10 site 2: the cleanup pin_read() suggestion ---------------------
#
# Drive pin_store() through the mock transport so cleanup reports an old
# version remaining and warns. The pin name carries the injection; the read
# call is rebuilt with deparse() so it parses to exactly one statement.
# cli.width is wide so the call is not wrapped across lines.

sf_sec_clean_meta <- function() {
  list(
    api_version = 1L,
    file = c("cars.rds", "wheels.rds"),
    file_size = 12L,
    created = "20240102T000000Z",
    pin_hash = "zzz990000000",
    type = "rds"
  )
}

sf_sec_parsed_read_call <- function(msg) {
  msg <- cli::ansi_strip(msg)
  hit <- grep("pin_read\\(", msg, fixed = FALSE)
  text <- msg[[hit[[1L]]]]
  parse(text = regmatches(
    text,
    regexpr("pin_read\\(.*\\)", text, perl = TRUE)
  )[[1L]])
}

test_that("the cleanup read call parses to one statement for any pin name", {
  withr::local_options(pins.quiet = TRUE, cli.width = 300)
  oldv <- "20240101T000001Z-oldv"
  names <- list(
    "cars",
    paste0("cars", '"', "x"),
    paste0("cars", "`", "x"),
    "cars\"); message(\"AUDIT_SENTINEL\"); #"
  )
  for (name in names) {
    board <- sf_mock_board(versioned = TRUE)
    dir <- withr::local_tempdir()
    paths <- c(file.path(dir, "cars.rds"), file.path(dir, "wheels.rds"))
    writeLines("a", paths[[1]])
    writeLines("b", paths[[2]])
    # The listing shows <name>/<old version>/data.txt, so the name is
    # really discovered and the write is a replace that reaches cleanup.
    rec <- sf_mock_bind(
      list = function(sql) {
        sf_fixture_listing(paste0(name, "/", oldv, "/data.txt"), board = board)
      }
    )

    w <- expect_warning(
      pinsExtras:::pin_store.pins_board_sf_stage(
        board, name, paths, sf_sec_clean_meta(), versioned = FALSE, x = NULL
      ),
      class = "pinsExtras_cleanup_incomplete"
    )
    expect_identical(
      length(sf_sec_parsed_read_call(conditionMessage(w))), 1L, info = name
    )
    if (grepl("AUDIT_SENTINEL", name, fixed = TRUE)) {
      # Present as an escaped string literal, never as a second call.
      expect_true(
        grepl("AUDIT_SENTINEL", conditionMessage(w), fixed = TRUE), info = name
      )
    }
  }
})
