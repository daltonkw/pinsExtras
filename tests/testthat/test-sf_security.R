# Security hardening: a path segment can never delete the whole board, and a
# discovered name carrying a separator or dot-segment is never trusted.

# ---- sf_check_path_segment(): the validator -------------------------------

test_that("sf_check_path_segment accepts an ordinary name", {
  expect_identical(pinsExtras:::sf_check_path_segment("cars"), TRUE)
})

test_that("sf_check_path_segment accepts a non-timestamp name (escape hatch)", {
  expect_identical(
    pinsExtras:::sf_check_path_segment("bogus-def12"),
    TRUE
  )
})

test_that("sf_check_path_segment accepts a well-formed version id", {
  expect_identical(
    pinsExtras:::sf_check_path_segment(sf_fixture_version()),
    TRUE
  )
})

test_that("sf_check_path_segment accepts hyphen, underscore and dot", {
  expect_identical(pinsExtras:::sf_check_path_segment("my.pin"), TRUE)
  expect_identical(pinsExtras:::sf_check_path_segment("a_b-c.d"), TRUE)
})

test_that("sf_check_path_segment rejects an empty string, classed", {
  expect_error(
    pinsExtras:::sf_check_path_segment(""),
    class = "pinsExtras_invalid_path_segment"
  )
})

test_that("sf_check_path_segment rejects NA_character_ and NA, classed", {
  expect_error(
    pinsExtras:::sf_check_path_segment(NA_character_),
    class = "pinsExtras_invalid_path_segment"
  )
  expect_error(
    pinsExtras:::sf_check_path_segment(NA),
    class = "pinsExtras_invalid_path_segment"
  )
})

test_that("sf_check_path_segment rejects a zero-length vector, classed", {
  expect_error(
    pinsExtras:::sf_check_path_segment(character(0)),
    class = "pinsExtras_invalid_path_segment"
  )
})

test_that("sf_check_path_segment rejects a multi-element vector, classed", {
  expect_error(
    pinsExtras:::sf_check_path_segment(c("a", "b")),
    class = "pinsExtras_invalid_path_segment"
  )
})

test_that("sf_check_path_segment rejects a non-string, classed", {
  expect_error(
    pinsExtras:::sf_check_path_segment(123),
    class = "pinsExtras_invalid_path_segment"
  )
})

test_that(
  "sf_check_path_segment rejects slash, dotdot, dot, nested, hidden,
  dot-embedded and backslash, all classed",
  {
    for (bad in c(
      "/", "a/b", "..", ".", "a/../b", "..hidden", "a..b", "a\\b"
    )) {
      expect_error(
        pinsExtras:::sf_check_path_segment(bad),
        class = "pinsExtras_invalid_path_segment"
      )
    }
  }
)

test_that(
  "the abort message carries the class but never repeats the value",
  {
    cnd <- tryCatch(
      pinsExtras:::sf_check_path_segment("SENTINEL/../x"),
      error = function(e) e
    )
    expect_true(
      "pinsExtras_invalid_path_segment" %in% class(cnd)
    )
    expect_false(
      grepl("SENTINEL", conditionMessage(cnd), fixed = TRUE)
    )
  }
)

# ---- pin_version_delete(): supplied arguments abort before any command ----

test_that("pin_version_delete(board, \"cars\", \"\") issues nothing", {
  board <- sf_mock_board()
  rec <- sf_mock_transport()
  testthat::local_mocked_bindings(
    sf_stage_cmd = rec$responder,
    .package = "pinsExtras"
  )

  expect_error(
    pins::pin_version_delete(board, "cars", "/"),
    class = "pinsExtras_invalid_path_segment"
  )
  expect_length(rec$calls, 0L)
})

test_that("pin_version_delete(board, \"/\", \"/\") issues nothing", {
  board <- sf_mock_board()
  rec <- sf_mock_transport()
  testthat::local_mocked_bindings(
    sf_stage_cmd = rec$responder,
    .package = "pinsExtras"
  )

  expect_error(
    pins::pin_version_delete(board, "/", "/"),
    class = "pinsExtras_invalid_path_segment"
  )
  expect_length(rec$calls, 0L)
})

test_that("pin_version_delete(board, \"cars\", \"..\") issues nothing", {
  board <- sf_mock_board()
  rec <- sf_mock_transport()
  testthat::local_mocked_bindings(
    sf_stage_cmd = rec$responder,
    .package = "pinsExtras"
  )

  expect_error(
    pins::pin_version_delete(board, "cars", ".."),
    class = "pinsExtras_invalid_path_segment"
  )
  expect_length(rec$calls, 0L)
})

test_that(
  "pin_version_delete still removes a malformed-but-valid name (escape hatch)",
  {
    board <- sf_mock_board()
    rec <- sf_mock_transport()
    testthat::local_mocked_bindings(
      sf_stage_cmd = rec$responder,
      .package = "pinsExtras"
    )

    out <- pins::pin_version_delete(board, "cars", "bogus-def12")
    expect_identical(out, board)
    expect_identical(rec$calls, "REMOVE '@~/cars/bogus-def12/'")
  }
)

# ---- pin_delete(): a supplied ".." aborts before any command --------------

test_that("pin_delete(board, \"..\") issues nothing", {
  board <- sf_mock_board()
  rec <- sf_mock_transport()
  testthat::local_mocked_bindings(
    sf_stage_cmd = rec$responder,
    .package = "pinsExtras"
  )

  expect_error(
    pins::pin_delete(board, ".."),
    class = "pinsExtras_invalid_path_segment"
  )
  expect_length(rec$calls, 0L)
})

# ---- sf_published_index(): discovered names are filtered ------------------

test_that("sf_published_index drops a discovered name of \"..\"", {
  out <- pinsExtras:::sf_published_index(
    sf_fixture_listing(
      "../20240101T000000Z-abc12/data.txt",
      "cars/20240101T000000Z-abc12/data.txt"
    )
  )
  expect_identical(out$name, "cars")
})

test_that("sf_published_index drops a discovered version with a backslash", {
  out <- pinsExtras:::sf_published_index(
    sf_fixture_listing(
      paste0("cars/20240101T000000Z-abc12/data.txt"),
      paste0("bad/20240101T000000Z-a\\\\b/data.txt")
    )
  )
  expect_identical(out$name, "cars")
})

# ---- read-side methods refuse a traversal name instead of escaping --------

test_that("pin_list omits a traversal name from the listing", {
  board <- sf_mock_board(stage = "@~")
  rec <- sf_mock_transport(
    list = sf_fixture_listing(
      "../20240101T000000Z-abc12/data.txt",
      "cars/20240101T000000Z-abc12/data.txt",
      board = board
    )
  )
  testthat::local_mocked_bindings(
    sf_stage_cmd = rec$responder,
    .package = "pinsExtras"
  )

  out <- pins::pin_list(board)
  expect_identical(out, "cars")
})

test_that(
  "pin_meta on a board whose only row is a traversal name is not found",
  {
    board <- sf_mock_board(stage = "@~")
    rec <- sf_mock_transport(
      list = sf_fixture_listing(
        "../20240101T000000Z-abc12/data.txt",
        board = board
      )
    )
    testthat::local_mocked_bindings(
      sf_stage_cmd = rec$responder,
      .package = "pinsExtras"
    )

    expect_error(
      pins::pin_meta(board, ".."),
      "Can't find pin called",
      fixed = TRUE
    )
    # Only the LIST issued; no GET, and no path was built outside the cache.
    expect_length(grep("^LIST ", rec$calls), 1L)
    expect_length(grep("^GET ", rec$calls), 0L)
  }
)

# ---- SEC-02: sf_read_meta() rejects an untrusted payload name --------------
# Every test drives the real read path through pins::pin_meta(): the LIST
# resolves the version, the GET serves crafted metadata, and pin_meta() reads
# it through sf_read_meta(), which now validates yaml$file.

make_meta <- function(file) {
  base <- list(
    api_version = 1L,
    created = "20240101",
    pin_hash = "abc1234567",
    type = "txt"
  )
  base$file <- file
  base
}

test_that(
  "pin_meta rejects a deep-traversal file in metadata, classed",
  {
    board <- sf_mock_board(stage = "@~")
    meta <- make_meta(file = "../../../../private.csv")
    rec <- sf_mock_transport(
      list = sf_fixture_listing(
        "cars/20240101T000000Z-abc12/data.txt", board = board
      ),
      get = sf_mock_get_files("data.txt" = yaml::as.yaml(meta))
    )
    testthat::local_mocked_bindings(
      sf_stage_cmd = rec$responder, .package = "pinsExtras"
    )

    expect_error(
      pins::pin_meta(board, "cars"),
      class = "pinsExtras_download_failed"
    )
  }
)

test_that("pin_meta rejects a parent-relative file in metadata", {
  board <- sf_mock_board(stage = "@~")
  meta <- list(
    api_version = 1L, file = "../sibling.csv",
    created = "20240101", pin_hash = "abc1234567", type = "txt"
  )
  rec <- sf_mock_transport(
    list = sf_fixture_listing(
      "cars/20240101T000000Z-abc12/data.txt", board = board
    ),
    get = sf_mock_get_files("data.txt" = yaml::as.yaml(meta))
  )
  testthat::local_mocked_bindings(
    sf_stage_cmd = rec$responder, .package = "pinsExtras"
  )

  expect_error(
    pins::pin_meta(board, "cars"),
    class = "pinsExtras_download_failed"
  )
})

test_that("pin_meta rejects a file carrying a backslash", {
  board <- sf_mock_board(stage = "@~")
  meta <- list(
    api_version = 1L, file = "a\\b.csv",
    created = "20240101", pin_hash = "abc1234567", type = "txt"
  )
  rec <- sf_mock_transport(
    list = sf_fixture_listing(
      "cars/20240101T000000Z-abc12/data.txt", board = board
    ),
    get = sf_mock_get_files("data.txt" = yaml::as.yaml(meta))
  )
  testthat::local_mocked_bindings(
    sf_stage_cmd = rec$responder, .package = "pinsExtras"
  )

  expect_error(
    pins::pin_meta(board, "cars"),
    class = "pinsExtras_download_failed"
  )
})

test_that("pin_meta rejects the reserved data.txt marker as payload", {
  board <- sf_mock_board(stage = "@~")
  meta <- list(
    api_version = 1L, file = "data.txt",
    created = "20240101", pin_hash = "abc1234567", type = "txt"
  )
  rec <- sf_mock_transport(
    list = sf_fixture_listing(
      "cars/20240101T000000Z-abc12/data.txt", board = board
    ),
    get = sf_mock_get_files("data.txt" = yaml::as.yaml(meta))
  )
  testthat::local_mocked_bindings(
    sf_stage_cmd = rec$responder, .package = "pinsExtras"
  )

  expect_error(
    pins::pin_meta(board, "cars"),
    class = "pinsExtras_download_failed"
  )
})

test_that("pin_meta rejects an empty-string file in metadata", {
  board <- sf_mock_board(stage = "@~")
  meta <- list(
    api_version = 1L, file = "",
    created = "20240101", pin_hash = "abc1234567", type = "txt"
  )
  rec <- sf_mock_transport(
    list = sf_fixture_listing(
      "cars/20240101T000000Z-abc12/data.txt", board = board
    ),
    get = sf_mock_get_files("data.txt" = yaml::as.yaml(meta))
  )
  testthat::local_mocked_bindings(
    sf_stage_cmd = rec$responder, .package = "pinsExtras"
  )

  expect_error(
    pins::pin_meta(board, "cars"),
    class = "pinsExtras_download_failed"
  )
})

test_that("pin_meta rejects metadata with no file field at all", {
  board <- sf_mock_board(stage = "@~")
  meta <- list(
    api_version = 1L,
    created = "20240101", pin_hash = "abc1234567", type = "txt"
  )
  rec <- sf_mock_transport(
    list = sf_fixture_listing(
      "cars/20240101T000000Z-abc12/data.txt", board = board
    ),
    get = sf_mock_get_files("data.txt" = yaml::as.yaml(meta))
  )
  testthat::local_mocked_bindings(
    sf_stage_cmd = rec$responder, .package = "pinsExtras"
  )

  expect_error(
    pins::pin_meta(board, "cars"),
    class = "pinsExtras_download_failed"
  )
})

test_that("pin_meta rejects a duplicate file entry, classed", {
  board <- sf_mock_board(stage = "@~")
  meta <- list(
    api_version = 1L, file = c("a.csv", "a.csv"),
    created = "20240101", pin_hash = "abc1234567", type = "txt"
  )
  rec <- sf_mock_transport(
    list = sf_fixture_listing(
      "cars/20240101T000000Z-abc12/data.txt", board = board
    ),
    get = sf_mock_get_files("data.txt" = yaml::as.yaml(meta))
  )
  testthat::local_mocked_bindings(
    sf_stage_cmd = rec$responder, .package = "pinsExtras"
  )

  expect_error(
    pins::pin_meta(board, "cars"),
    class = "pinsExtras_download_failed"
  )
})

test_that("pin_meta rejects a list containing one bad of two", {
  board <- sf_mock_board(stage = "@~")
  meta <- list(
    api_version = 1L, file = c("a.csv", "../b.csv"),
    created = "20240101", pin_hash = "abc1234567", type = "txt"
  )
  rec <- sf_mock_transport(
    list = sf_fixture_listing(
      "cars/20240101T000000Z-abc12/data.txt", board = board
    ),
    get = sf_mock_get_files("data.txt" = yaml::as.yaml(meta))
  )
  testthat::local_mocked_bindings(
    sf_stage_cmd = rec$responder, .package = "pinsExtras"
  )

  expect_error(
    pins::pin_meta(board, "cars"),
    class = "pinsExtras_download_failed"
  )
})

test_that(
  "pin_meta rejects a traversal file without echoing the payload",
  {
    board <- sf_mock_board(stage = "@~")
    meta <- list(
      api_version = 1L, file = "../../../../../SENTINEL.csv",
      created = "20240101", pin_hash = "abc1234567", type = "txt"
    )
    rec <- sf_mock_transport(
      list = sf_fixture_listing(
        "cars/20240101T000000Z-abc12/data.txt", board = board
      ),
      get = sf_mock_get_files("data.txt" = yaml::as.yaml(meta))
    )
    testthat::local_mocked_bindings(
      sf_stage_cmd = rec$responder, .package = "pinsExtras"
    )

    cond <- expect_error(
      pins::pin_meta(board, "cars"),
      class = "pinsExtras_download_failed"
    )
    msg <- cli::ansi_strip(conditionMessage(cond))
    expect_false(grepl("SENTINEL", msg, fixed = TRUE))
  }
)

test_that("pin_meta accepts a clean, duplicate-free file list", {
  board <- sf_mock_board(stage = "@~")
  meta <- list(
    api_version = 1L, file = c("a.csv", "b.csv"),
    created = "20240101", pin_hash = "abc1234567", type = "txt"
  )
  rec <- sf_mock_transport(
    list = sf_fixture_listing(
      "cars/20240101T000000Z-abc12/data.txt", board = board
    ),
    get = sf_mock_get_files("data.txt" = yaml::as.yaml(meta))
  )
  testthat::local_mocked_bindings(
    sf_stage_cmd = rec$responder, .package = "pinsExtras"
  )

  out <- pins::pin_meta(board, "cars")
  expect_identical(out$file, c("a.csv", "b.csv"))
})

# ---- SEC-06: pin_store() rejects metadata whose version is not discoverable
# These drive the write through the pin_store() method and assert the command
# count, because the whole point is that nothing moves and nothing is deleted.

test_that(
  "a version that cannot be parsed aborts the write before any command",
  {
    board <- sf_mock_board()
    meta <- list(
      api_version = 1L, file = "cars.rds", file_size = 12L,
      created = "bogus", pin_hash = "def12", type = "rds"
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
        board, "cars", paths, meta, versioned = TRUE, x = NULL
      ),
      class = "pinsExtras_invalid_upload_set"
    )
    expect_length(rec$calls, 0L)
  }
)

test_that("a write with no created field aborts before any command", {
  board <- sf_mock_board()
  meta <- list(
    api_version = 1L, file = "cars.rds", file_size = 12L,
    pin_hash = "abcdef0123456789", type = "rds"
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
      board, "cars", paths, meta, versioned = TRUE, x = NULL
    ),
    class = "pinsExtras_invalid_upload_set"
  )
  expect_length(rec$calls, 0L)
})

test_that("a write with no pin_hash field aborts before any command", {
  board <- sf_mock_board()
  meta <- list(
    api_version = 1L, file = "cars.rds", file_size = 12L,
    created = "20240102T000000Z", type = "rds"
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
      board, "cars", paths, meta, versioned = TRUE, x = NULL
    ),
    class = "pinsExtras_invalid_upload_set"
  )
  expect_length(rec$calls, 0L)
})

test_that(
  "well-formed metadata still writes with one LIST and two PUT",
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

    out <- pinsExtras:::pin_store.pins_board_sf_stage(
      board, "cars", paths, meta, versioned = FALSE, x = NULL
    )

    expect_identical(out, "cars")
    expect_length(grep("^LIST ", rec$calls), 1L)
    expect_length(grep("^PUT ", rec$calls), 2L)
    expect_length(grep("^REMOVE ", rec$calls), 0L)
  }
)

make_clean_meta <- function() {
  list(
    api_version = 1L,
    file = c("cars.rds", "wheels.rds"),
    file_size = 12L,
    created = "20240102T000000Z",
    pin_hash = "zzz990000000",
    type = "rds"
  )
}

parsed_read_call <- function(msg) {
  msg <- cli::ansi_strip(msg)
  hit <- grep("pin_read\\(", msg, fixed = FALSE)
  text <- msg[[hit[[1L]]]]
  parse(text = regmatches(
    text,
    regexpr("pin_read\\(.*\\)", text, perl = TRUE)
  )[[1L]])
}

# ---- Site 1: reconnection guidance must parse to exactly one statement --
# The board_sf_stage() call is built with rlang::expr() and deparse(), so the
# stage and path are R string literals. An injection pasted into `path` must
# therefore parse back to a single statement, not a second call.

test_that(
  "the reconnection guidance parses to one statement for an ordinary path",
  {
    library(DBI)
    withr::local_options(cli.width = 300)
    methods::setClass(
      "s5conn",
      contains = "DBIConnection"
    )
    methods::setMethod(
      "dbIsValid",
      "s5conn",
      function(dbObj, ...) FALSE
    )
    board <- pinsExtras:::board_sf_stage(
      conn = new("s5conn"),
      stage = "@~",
      path = "prod"
    )
    cond <- tryCatch(
      pinsExtras:::sf_check_connection(board),
      error = function(e) e
    )
    expect_true(inherits(cond, "error"))
    out <- cli::ansi_strip(conditionMessage(cond))
    line <- strsplit(out, "\n", fixed = TRUE)[[1L]]
    line <- line[grepl("board_sf_stage", line, fixed = TRUE)]
    expect_length(line, 1L)
    expect_length(parse(text = line), 1L)
  }
)

test_that(
  "the reconnection guidance escapes a double quote in the path, one stmt",
  {
    library(DBI)
    withr::local_options(cli.width = 300)
    methods::setClass(
      "s5conn",
      contains = "DBIConnection"
    )
    methods::setMethod(
      "dbIsValid",
      "s5conn",
      function(dbObj, ...) FALSE
    )
    board <- pinsExtras:::board_sf_stage(
      conn = new("s5conn"),
      stage = "@~",
      path = paste0("a", '"', "b")
    )
    cond <- tryCatch(
      pinsExtras:::sf_check_connection(board),
      error = function(e) e
    )
    expect_true(inherits(cond, "error"))
    out <- cli::ansi_strip(conditionMessage(cond))
    line <- strsplit(out, "\n", fixed = TRUE)[[1L]]
    line <- line[grepl("board_sf_stage", line, fixed = TRUE)]
    expect_length(line, 1L)
    expect_length(parse(text = line), 1L)
  }
)

test_that(
  "the reconnection guidance escapes a backtick in the path, one stmt",
  {
    library(DBI)
    withr::local_options(cli.width = 300)
    methods::setClass(
      "s5conn",
      contains = "DBIConnection"
    )
    methods::setMethod(
      "dbIsValid",
      "s5conn",
      function(dbObj, ...) FALSE
    )
    board <- pinsExtras:::board_sf_stage(
      conn = new("s5conn"),
      stage = "@~",
      path = paste0("a", "`", "b")
    )
    cond <- tryCatch(
      pinsExtras:::sf_check_connection(board),
      error = function(e) e
    )
    expect_true(inherits(cond, "error"))
    out <- cli::ansi_strip(conditionMessage(cond))
    line <- strsplit(out, "\n", fixed = TRUE)[[1L]]
    line <- line[grepl("board_sf_stage", line, fixed = TRUE)]
    expect_length(line, 1L)
    expect_length(parse(text = line), 1L)
  }
)

test_that(
  "the reconnection guidance neutralises a statement injection, one stmt",
  {
    library(DBI)
    withr::local_options(cli.width = 300)
    methods::setClass(
      "s5conn",
      contains = "DBIConnection"
    )
    methods::setMethod(
      "dbIsValid",
      "s5conn",
      function(dbObj, ...) FALSE
    )
    injection <- paste0(
      "x\"); message(\"AUDIT_SENTINEL\"); #"
    )
    board <- pinsExtras:::board_sf_stage(
      conn = new("s5conn"),
      stage = "@~",
      path = injection
    )
    cond <- tryCatch(
      pinsExtras:::sf_check_connection(board),
      error = function(e) e
    )
    expect_true(inherits(cond, "error"))
    out <- cli::ansi_strip(conditionMessage(cond))
    line <- strsplit(out, "\n", fixed = TRUE)[[1L]]
    line <- line[grepl("board_sf_stage", line, fixed = TRUE)]
    expect_length(line, 1L)
    parsed <- parse(text = line)
    expect_length(parsed, 1L)
    # The injected value is captured and embedded as one R string literal, so
    # parse() above returns a single statement and no message( becomes a call.
    expect_true(grepl("AUDIT_SENTINEL", line, fixed = TRUE))
    expect_true(grepl('\\', line, fixed = TRUE))
  }
)

# ---- Site 2: the cleanup pin_read() suggestion must parse to one stmt -----
# Same shape as test-sf_publish.R: drive pin_store() through the mock transport
# so cleanup reports an old version remaining and warns. The pin name carries
# the injection; the read call is rebuilt with deparse() so it parses to one.
# cli.width is wide so the call is not wrapped across lines and breaks parse.
#
# The mock listing lists <name>/<old version>/data.txt, so the injected pin
# name is actually discovered and the write is a replace that reaches cleanup.

test_that(
  "the cleanup read call parses to one statement for an ordinary name",
  {
    withr::local_options(pins.quiet = TRUE, cli.width = 300)
    name <- "cars"
    board <- sf_mock_board(versioned = TRUE)
    oldv <- "20240101T000001Z-oldv"
    dir <- withr::local_tempdir(.local_envir = parent.frame())
    paths <- c(file.path(dir, "cars.rds"), file.path(dir, "wheels.rds"))
    writeLines("a", paths[[1]])
    writeLines("b", paths[[2]])
    rec <- sf_mock_transport(
      list = function(sql) {
        sf_fixture_listing(paste0(name, "/", oldv, "/data.txt"), board = board)
      }
    )
    testthat::local_mocked_bindings(
      sf_stage_cmd = rec$responder, .package = "pinsExtras"
    )

    w <- expect_warning(
      pinsExtras:::pin_store.pins_board_sf_stage(
        board, name, paths, make_clean_meta(), versioned = FALSE, x = NULL
      ),
      class = "pinsExtras_cleanup_incomplete"
    )
    expect_length(parsed_read_call(conditionMessage(w)), 1L)
  }
)

test_that(
  "the cleanup read call parses to one statement for a quoted name",
  {
    withr::local_options(pins.quiet = TRUE, cli.width = 300)
    name <- paste0("cars", '"', "x")
    board <- sf_mock_board(versioned = TRUE)
    oldv <- "20240101T000001Z-oldv"
    dir <- withr::local_tempdir(.local_envir = parent.frame())
    paths <- c(file.path(dir, "cars.rds"), file.path(dir, "wheels.rds"))
    writeLines("a", paths[[1]])
    writeLines("b", paths[[2]])
    rec <- sf_mock_transport(
      list = function(sql) {
        sf_fixture_listing(paste0(name, "/", oldv, "/data.txt"), board = board)
      }
    )
    testthat::local_mocked_bindings(
      sf_stage_cmd = rec$responder, .package = "pinsExtras"
    )

    w <- expect_warning(
      pinsExtras:::pin_store.pins_board_sf_stage(
        board, name, paths, make_clean_meta(), versioned = FALSE, x = NULL
      ),
      class = "pinsExtras_cleanup_incomplete"
    )
    expect_length(parsed_read_call(conditionMessage(w)), 1L)
  }
)

test_that(
  "the cleanup read call parses to one statement for a backticked name",
  {
    withr::local_options(pins.quiet = TRUE, cli.width = 300)
    name <- paste0("cars", "`", "x")
    board <- sf_mock_board(versioned = TRUE)
    oldv <- "20240101T000001Z-oldv"
    dir <- withr::local_tempdir(.local_envir = parent.frame())
    paths <- c(file.path(dir, "cars.rds"), file.path(dir, "wheels.rds"))
    writeLines("a", paths[[1]])
    writeLines("b", paths[[2]])
    rec <- sf_mock_transport(
      list = function(sql) {
        sf_fixture_listing(paste0(name, "/", oldv, "/data.txt"), board = board)
      }
    )
    testthat::local_mocked_bindings(
      sf_stage_cmd = rec$responder, .package = "pinsExtras"
    )

    w <- expect_warning(
      pinsExtras:::pin_store.pins_board_sf_stage(
        board, name, paths, make_clean_meta(), versioned = FALSE, x = NULL
      ),
      class = "pinsExtras_cleanup_incomplete"
    )
    expect_length(parsed_read_call(conditionMessage(w)), 1L)
  }
)

test_that(
  "the cleanup read call neutralises a statement injection, one stmt",
  {
    withr::local_options(pins.quiet = TRUE, cli.width = 300)
    name <- paste0("cars\"); message(\"AUDIT_SENTINEL\"); #")
    board <- sf_mock_board(versioned = TRUE)
    oldv <- "20240101T000001Z-oldv"
    dir <- withr::local_tempdir(.local_envir = parent.frame())
    paths <- c(file.path(dir, "cars.rds"), file.path(dir, "wheels.rds"))
    writeLines("a", paths[[1]])
    writeLines("b", paths[[2]])
    rec <- sf_mock_transport(
      list = function(sql) {
        sf_fixture_listing(paste0(name, "/", oldv, "/data.txt"), board = board)
      }
    )
    testthat::local_mocked_bindings(
      sf_stage_cmd = rec$responder, .package = "pinsExtras"
    )

    w <- expect_warning(
      pinsExtras:::pin_store.pins_board_sf_stage(
        board, name, paths, make_clean_meta(), versioned = FALSE, x = NULL
      ),
      class = "pinsExtras_cleanup_incomplete"
    )
    parsed <- parsed_read_call(conditionMessage(w))
    expect_length(parsed, 1L)
    # The injected payload is now an R string literal, so it is present in the
    # deparse() output but escaped, never a second call.
    expect_true(grepl("AUDIT_SENTINEL", conditionMessage(w), fixed = TRUE))
  }
)

# SEC-10, the branch that runs for every board the constructor built.
# board_sf_stage() always stores connect_args, so this is the common path;
# deparse() closes the call, and text appended after that lands outside the
# parentheses. The guidance must parse as exactly one statement.

sf_sec_dead_board <- function(path, connect_args) {
  methods::setClass(
    "sf_sec_conn",
    contains = "DBIConnection",
    where = topenv(environment())
  )
  methods::setMethod(
    "dbIsValid", "sf_sec_conn",
    function(dbObj, ...) FALSE,
    where = topenv(environment())
  )
  structure(
    list(
      stage = "@~", path = path, connect_args = connect_args,
      versioned = TRUE, conn = methods::new("sf_sec_conn")
    ),
    class = c("pins_board_sf_stage", "pins_board")
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

test_that("reconnection guidance parses as one statement with connect_args", {
  withr::local_options(cli.width = 300)
  for (path in list("team-data", "a\"b", "a`b", "x\"); message(\"X\"); #")) {
    board <- sf_sec_dead_board(path, list(Driver = "x"))
    code <- sf_sec_guidance(board)
    expect_match(code, "connect_args = ...", fixed = TRUE, info = path)
    expect_length(parse(text = code), 1L)
  }
})

test_that("reconnection guidance parses as one statement without them", {
  withr::local_options(cli.width = 300)
  for (path in list("team-data", "x\"); message(\"X\"); #")) {
    board <- sf_sec_dead_board(path, NULL)
    code <- sf_sec_guidance(board)
    expect_no_match(code, "connect_args", fixed = TRUE, info = path)
    expect_length(parse(text = code), 1L)
  }
})
