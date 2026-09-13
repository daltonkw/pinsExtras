skip_if_no_sf_stage <- function() {
  needed <- c(
    "PINS_SF_SERVER",
    "PINS_SF_USER",
    "PINS_SF_AUTHENTICATOR",
    "PINS_SF_PRIVATE_KEY_FILE",
    "PINS_SF_WAREHOUSE"
  )
  if (!all(sf_has_envvars(needed))) {
    testthat::skip("Snowflake env vars not set")
  }
  if (!requireNamespace("DBI", quietly = TRUE) ||
    !requireNamespace("odbc", quietly = TRUE)) {
    testthat::skip("DBI/odbc not installed")
  }
}

sf_stage_test_conn <- function() {
  # Build connection args, filtering out NULL values for optional params
  conn_args <- purrr::compact(list(
    drv = odbc::odbc(),
    Driver = pinsExtras:::sf_odbc_driver(),  # Auto-detect driver name
    Server = Sys.getenv("PINS_SF_SERVER"),
    UID = Sys.getenv("PINS_SF_USER"),
    Authenticator = Sys.getenv("PINS_SF_AUTHENTICATOR"),
    PRIV_KEY_FILE = Sys.getenv("PINS_SF_PRIVATE_KEY_FILE"),
    Warehouse = Sys.getenv("PINS_SF_WAREHOUSE"),
    Database = sf_null_if_na(Sys.getenv("PINS_SF_DATABASE", NA)),
    Schema = sf_null_if_na(Sys.getenv("PINS_SF_SCHEMA", NA)),
    Role = sf_null_if_na(Sys.getenv("PINS_SF_ROLE", NA))
  ))
  do.call(DBI::dbConnect, conn_args)
}

sf_stage_test_args <- function() {
  purrr::compact(list(
    Driver = pinsExtras:::sf_odbc_driver(),
    Server = Sys.getenv("PINS_SF_SERVER"),
    UID = Sys.getenv("PINS_SF_USER"),
    Authenticator = Sys.getenv("PINS_SF_AUTHENTICATOR"),
    PRIV_KEY_FILE = Sys.getenv("PINS_SF_PRIVATE_KEY_FILE"),
    Warehouse = Sys.getenv("PINS_SF_WAREHOUSE"),
    Database = sf_null_if_na(Sys.getenv("PINS_SF_DATABASE", NA)),
    Schema = sf_null_if_na(Sys.getenv("PINS_SF_SCHEMA", NA)),
    Role = sf_null_if_na(Sys.getenv("PINS_SF_ROLE", NA))
  ))
}

# ---- unique prefixes and centralised cleanup ------------------------------
#
# Every integration test works inside its own top-level prefix on the stage,
# and exactly one cleanup removes that prefix. Two rules make this safe:
#
#   * the prefix is unique per board, so two tests can never share one (the
#     old `as.integer(Sys.time())` form has one-second resolution, so two
#     tests starting in the same second collided);
#   * cleanup deletes the board's OWN root, `sf_stage_delete_dir(board, "")`,
#     rather than a path relative to a board already rooted at that path.
#     The old form passed `path_base` to a board already at `path_base`,
#     producing `@~/<prefix>/<prefix>/`, which has never existed and so has
#     never deleted anything.
#
# sf_stage_delete_dir() refuses `dir = ""` when the board path is empty
# (class `pinsExtras_invalid_delete_target`), so a cleanup registered this
# way cannot wipe a stage root even if it is called by mistake.

sf_stage_test_prefix <- function(tag) {
  if (!is.character(tag) || length(tag) != 1L || is.na(tag) || tag == "") {
    stop("`tag` must be a non-empty string")
  }
  paste0(
    "pins-sf-", tag, "-", as.integer(Sys.time()), "-",
    paste(sample(c(letters, 0:9), 6L, replace = TRUE), collapse = "")
  )
}

# Delete everything under a test board's own prefix. Never throws: a stage
# hiccup during teardown must not turn a passing test into a failing one.
# (Verified: a withr::defer() handler that throws does NOT stop the later
# handlers from running, so the disconnect is safe either way -- but the
# error does surface as a test failure, which is what this prevents.)
# Reports what went wrong as a message instead.
sf_stage_test_cleanup <- function(board) {
  tryCatch(
    pinsExtras:::sf_stage_delete_dir(board, ""),
    error = function(e) {
      message(
        "Test cleanup failed for stage prefix ",
        encodeString(board$path, quote = "\""), ": ", conditionMessage(e)
      )
      invisible(FALSE)
    }
  )
}

# Delete one named pin from a test board. For boards with an EMPTY path,
# where deleting the board root would mean deleting the whole stage: such a
# test owns its uniquely named pins and nothing else.
sf_stage_test_cleanup_pin <- function(board, name) {
  tryCatch(
    pinsExtras:::sf_stage_delete_dir(board, name),
    error = function(e) {
      message(
        "Test cleanup failed for pin ", encodeString(name, quote = "\""),
        ": ", conditionMessage(e)
      )
      invisible(FALSE)
    }
  )
}

# Build a board for one test, and register its teardown on the caller's frame.
#
# Handlers run last-in-first-out, so the disconnect is registered FIRST in
# order to run LAST: cleanup needs the connection to still be open.
#
# `path` must be a unique non-empty prefix from sf_stage_test_prefix(). A
# board with an empty path is built by the one test that needs it, which
# registers per-pin cleanup instead.
sf_stage_test_board <- function(path, .local_envir = parent.frame()) {
  if (!is.character(path) || length(path) != 1L || is.na(path) || path == "") {
    stop("`path` must be a unique non-empty prefix; see sf_stage_test_prefix()")
  }
  conn <- sf_stage_test_conn()
  board <- board_sf_stage(
    conn = conn,
    stage = Sys.getenv("PINS_SF_STAGE", "@~"),
    path = path,
    connect_args = sf_stage_test_args()
  )
  withr::defer(
    try(DBI::dbDisconnect(conn), silent = TRUE),
    envir = .local_envir
  )
  withr::defer(sf_stage_test_cleanup(board), envir = .local_envir)
  board
}

sf_null_if_na <- function(x) {
  if (length(x) == 1 && is.na(x)) NULL else x
}

sf_has_envvars <- function(x) {
  all(Sys.getenv(x) != "")
}
