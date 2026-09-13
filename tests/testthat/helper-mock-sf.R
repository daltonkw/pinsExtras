# Offline fixtures for the Snowflake stage board.
#
# These are supervisor-owned. Worker tasks use them and never modify them.
#
# The whole offline suite has exactly one mocking point: `sf_stage_cmd()`, the
# single function in the package that hands SQL to DBI. Everything above it --
# listing, discovery, version resolution, validation, publication -- runs for
# real against these fixtures. Nothing else is ever mocked.
#
# Typical shape of a test:
#
#   test_that("pin_exists() issues one pin-scoped listing", {
#     board <- sf_mock_board()
#     rec <- sf_mock_transport(
#       list = sf_fixture_listing("cars/20240101T000000Z-abc12/data.txt")
#     )
#     testthat::local_mocked_bindings(
#       sf_stage_cmd = rec$responder,
#       .package = "pinsExtras"
#     )
#
#     expect_true(pin_exists(board, "cars"))
#     expect_identical(rec$calls, "LIST '@~/cars/'")
#   })


# A real board_sf_stage() object around an inert connection.
#
# The connection satisfies board_sf_stage()'s `inherits(conn, "DBIConnection")`
# check and nothing else. It has no DBI methods, so any test that reaches the
# real sf_stage_cmd() -- i.e. forgot to mock it -- fails loudly instead of
# quietly trying to talk to Snowflake. That is deliberate.
#
# cache defaults to a temporary directory tied to the calling test, so no test
# writes into the user's pins cache or into the package directory.
sf_mock_board <- function(path = "",
                          stage = "@~",
                          versioned = TRUE,
                          cache = NULL) {
  cache <- cache %||% withr::local_tempdir(.local_envir = parent.frame())
  board_sf_stage(
    conn = structure(list(), class = c("sf_mock_conn", "DBIConnection")),
    stage = stage,
    path = path,
    versioned = versioned,
    cache = cache
  )
}


# A recording transport.
#
# Returns an environment with two elements:
#   $calls     character vector of the SQL issued so far, in order. It is read
#              live, so `rec$calls` after the act step is the full record.
#   $responder the function to bind to sf_stage_cmd() with
#              testthat::local_mocked_bindings().
#
# `list`, `put`, `get` and `remove` are the responses for each SQL verb. Each
# may be:
#   * a data frame, returned as-is for every call to that verb, or
#   * a function of `(sql)`, or of `(sql, calls)` where `calls` is the record
#     so far including this call. The two-argument form is how a test makes a
#     verb answer differently on successive identical calls, for example to
#     show a pin appearing only after its data.txt is uploaded:
#
#       list = function(sql, calls) {
#         if (sum(grepl("^LIST", calls)) == 1L) before else after
#       }
#
# Omitted verbs get a plausible success response (see the sf_mock_*_response
# helpers below), so a test only has to describe the part it cares about.
#
# `casing = "upper"` upper-cases the column names of every response, whoever
# produced it. The ODBC driver's casing is not guaranteed across versions, so
# every verb is exercised with both casings somewhere in the suite; flipping
# this one argument is how.
sf_mock_transport <- function(list = NULL,
                              put = NULL,
                              get = NULL,
                              remove = NULL,
                              casing = c("lower", "upper")) {
  # `list` is a formal here because the plan fixes these argument names, which
  # shadows base::list() inside this function. Capture the responses under
  # different names immediately and never call bare list() below.
  responses <- base::list(
    LIST = list,
    PUT = put,
    GET = get,
    REMOVE = remove
  )
  defaults <- base::list(
    LIST = sf_mock_list_response,
    PUT = sf_mock_put_response,
    GET = sf_mock_get_response,
    REMOVE = sf_mock_remove_response
  )
  casing <- match.arg(casing)

  rec <- new.env(parent = emptyenv())
  rec$calls <- character()

  rec$responder <- function(board, sql) {
    rec$calls <- c(rec$calls, sql)
    verb <- sf_mock_verb(sql)
    resp <- responses[[verb]] %||% defaults[[verb]]
    out <- sf_mock_apply(resp, sql, rec$calls)
    sf_mock_recase(out, casing)
  }

  rec
}


# LIST rows, from paths written the way a test thinks about them.
#
# `...` are paths relative to the *board*, e.g. "cars/20240101T000000Z-abc12/
# data.txt". When `board` is supplied this helper adds what Snowflake would
# actually put in front of them: the board's own `path`, and the stage name for
# a named stage (Snowflake prefixes LIST output with the stage name, but not
# for the `@~` user stage). Pass `stage_prefix` explicitly to override that --
# `stage_prefix = ""` forces no prefix, which is how you test that partially
# prefixed responses are left alone.
sf_fixture_listing <- function(...,
                               board = NULL,
                               stage_prefix = NULL,
                               size = 1024,
                               md5 = "d41d8cd98f00b204e9800998ecf8427e",
                               last_modified = "Mon, 1 Jan 2024 00:00:00 GMT") {
  paths <- unlist(base::list(...), use.names = FALSE)
  if (is.null(paths)) {
    paths <- character()
  }
  paths <- as.character(paths)

  board_path <- board$path %||% ""
  if (board_path != "") {
    paths <- paste0(board_path, "/", paths)
  }

  prefix <- stage_prefix %||% sf_mock_stage_prefix(board)
  if (prefix != "") {
    paths <- paste0(prefix, "/", paths)
  }

  data.frame(
    name = paths,
    size = rep_len(size, length(paths)),
    md5 = rep_len(md5, length(paths)),
    last_modified = rep_len(last_modified, length(paths)),
    stringsAsFactors = FALSE
  )
}


# A version identifier of the shape sf_version_name() produces.
#
# Fixed by default: no test may depend on the wall clock.
sf_fixture_version <- function(ts = "20240101T000000Z", hash = "abc12") {
  paste0(ts, "-", hash)
}


# ---- internals used by the fixtures above -------------------------------

# The SQL verb: the first word of the command.
sf_mock_verb <- function(sql) {
  toupper(sub("^\\s*([A-Za-z]+).*$", "\\1", sql))
}

# The stage locations and file URIs of a command, in order, unquoted.
#
# Once every SQL builder routes through the quoting helper these are simply the
# quoted tokens. The unquoted fallback keeps the fixtures usable against a verb
# that has not been migrated yet, so a test fails on the behaviour it is about
# rather than on the mock. Test paths never contain apostrophes, so the simple
# scan is sufficient here.
sf_mock_sql_args <- function(sql) {
  quoted <- regmatches(sql, gregexpr("'[^']*'", sql))[[1]]
  if (length(quoted) > 0L) {
    return(substr(quoted, 2L, nchar(quoted) - 1L))
  }
  regmatches(sql, gregexpr("(?:file://|@)[^[:space:]]+", sql))[[1]]
}

# A response may be a data frame or a function of (sql) or (sql, calls).
sf_mock_apply <- function(resp, sql, calls) {
  if (is.function(resp)) {
    if (length(formals(resp)) >= 2L) resp(sql, calls) else resp(sql)
  } else {
    resp
  }
}

sf_mock_recase <- function(df, casing) {
  if (is.null(df) || is.null(names(df))) {
    return(df)
  }
  names(df) <- if (casing == "upper") toupper(names(df)) else tolower(names(df))
  df
}

# "mystage" for @mystage or @db.schema.mystage; "" for the @~ user stage.
sf_mock_stage_prefix <- function(board) {
  if (is.null(board)) {
    return("")
  }
  name <- pinsExtras:::sf_extract_stage_name(board$stage)
  if (name == "~") "" else name
}

# Default responses ------------------------------------------------------

sf_mock_list_response <- function(sql) {
  sf_fixture_listing()
}

sf_mock_put_response <- function(sql) {
  args <- sf_mock_sql_args(sql)
  src <- basename(sub("^file://", "", args[[1]]))
  data.frame(
    source = src,
    target = paste0(args[[2]], "/", src),
    source_size = 1024,
    target_size = 1024,
    source_compression = "NONE",
    target_compression = "NONE",
    status = "UPLOADED",
    message = "",
    stringsAsFactors = FALSE
  )
}

# GET both reports success and creates the real file, because the code under
# test copies it out of the transfer directory and then reads it. A test that
# needs real content (metadata YAML, a payload) supplies its own `get`
# responder; see sf_mock_get_files() below.
sf_mock_get_response <- function(sql) {
  sf_mock_get_files()(sql)
}

sf_mock_remove_response <- function(sql) {
  data.frame(
    name = character(),
    result = character(),
    stringsAsFactors = FALSE
  )
}

# Build a `get` responder that serves real file content.
#
#   get = sf_mock_get_files("data.txt" = yaml::as.yaml(meta))
#
# Names are file basenames; values are the text written to the transfer
# directory. A requested file with no entry is written empty. Files listed in
# `missing` are reported DOWNLOADED but never written, which is how the
# "validated transfer that lands under an unexpected name" case is built.
sf_mock_get_files <- function(..., status = "DOWNLOADED", missing = character()) {
  contents <- base::list(...)
  function(sql) {
    args <- sf_mock_sql_args(sql)
    src <- args[[1]]
    dest_dir <- sub("^file://", "", args[[2]])
    # Since U15 the GET verb names a DIRECTORY and selects one file with a
    # PATTERN of the form '.*/<escaped file>$', so the file name lives in the
    # pattern rather than in the location. Recover it from there when a
    # pattern is present; fall back to the location for any caller that still
    # passes a full file path.
    file <- if (length(args) >= 3L) {
      # Since S3 the pattern also carries the directory, as
      # '.*<dir>/<file>$' -- note the bare '.*' with no slash, which is what
      # the live probe showed GET requires. Take the basename of whatever is
      # left after stripping the leading wildcard and the anchor; the strip
      # is a no-op for that form and basename() does the work. Correct for
      # the stage-root form '.*/<file>$' too.
      basename(
        gsub("\\\\", "", sub("\\$$", "", sub("^\\.\\*/", "", args[[3]])))
      )
    } else {
      basename(src)
    }

    if (!file %in% missing) {
      text <- contents[[file]] %||% ""
      fs::dir_create(dest_dir)
      writeLines(as.character(text), fs::path(dest_dir, file))
    }

    data.frame(
      file = file,
      size = 1024,
      status = status,
      message = "",
      stringsAsFactors = FALSE
    )
  }
}
