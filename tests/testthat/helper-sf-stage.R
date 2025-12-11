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

sf_stage_test_board <- function(path) {

  conn <- sf_stage_test_conn()
  # Note: connection lifecycle is managed by the test via withr::defer()

  # Do NOT disconnect here - the board object holds a reference to conn

  board_sf_stage(
    conn = conn,
    stage = Sys.getenv("PINS_SF_STAGE", "@~"),
    path = path,
    connect_args = sf_stage_test_args()
  )
}

sf_null_if_na <- function(x) {
  if (length(x) == 1 && is.na(x)) NULL else x
}

sf_has_envvars <- function(x) {
  all(Sys.getenv(x) != "")
}
