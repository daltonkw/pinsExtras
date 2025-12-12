#' Use a Snowflake stage as a board
#'
#' Pin data to a Snowflake internal stage using the Snowflake ODBC driver
#' via DBI and odbc. This allows you to share pins across projects and
#' users through Snowflake's stage infrastructure.
#'
#' # Authentication
#'
#' `board_sf_stage()` requires an active DBI connection to Snowflake. You
#' typically create this connection using `DBI::dbConnect(odbc::odbc(), ...)`.
#' The Snowflake ODBC driver supports several authentication methods:
#'
#' * **Username and password**: Pass `UID` and `PWD` to `dbConnect()`.
#'   (Not recommended since credentials may be recorded in `.Rhistory`)
#'
#' * **Key pair authentication (JWT)**: Pass `Authenticator = "SNOWFLAKE_JWT"`
#'   and `PRIV_KEY_FILE = "path/to/key.p8"`. This is the recommended method
#'   for automated workflows.
#'
#' * **SSO/Federated authentication**: Pass `Authenticator = "externalbrowser"`
#'   to use browser-based SSO.
#'
#' * **OAuth**: Pass `Authenticator = "oauth"` and `Token` with your OAuth token.
#'
#' The connection must have appropriate privileges on the target stage
#' (e.g., `READ`, `WRITE` for internal stages, or access to the underlying
#' cloud storage for external stages).
#'
#' # Details
#'
#' **Important**: This board is designed for **internal Snowflake stages only**.
#' External stages (backed by S3, Azure Blob Storage, or GCS) are not currently
#' supported and may produce unexpected results.
#'
#' * The `@~` stage is a special user stage that exists by default for each
#'   Snowflake user. It's convenient for testing but not suitable for sharing.
#'
#' * You can use `path` to maintain multiple independent pin boards within
#'   a single stage, similar to how `prefix` works for S3 boards.
#'
#' * Stage names can be simple (`@mystage`) or fully qualified
#'   (`@database.schema.mystage`). The `@` prefix is added automatically if
#'   omitted.
#'
#' * `board_sf_stage()` is powered by the DBI and odbc packages, which are
#'   suggested dependencies of pinsExtras. You also need the Snowflake ODBC
#'   driver installed on your system.
#'
#' # Edge Cases and Limitations
#'
#' **Pin Names**:
#' * Pin names can contain hyphens (`-`), underscores (`_`), dots (`.`), and
#'   numbers. These have been tested and work correctly.
#' * Very long pin names (100+ characters) are supported and tested.
#' * Pin names cannot be `"data.txt"` (reserved for metadata).
#'
#' **Empty and Special Data**:
#' * Empty data frames (zero rows), NA values, and zero-length vectors are
#'   fully supported and will round-trip correctly.
#' * Single-value data (scalars) and complex nested structures work as expected.
#'
#' **Concurrent Access**:
#' * With `versioned = TRUE` (default), concurrent writes create separate
#'   versions safely. Each write generates a unique version based on timestamp
#'   and content hash.
#' * With `versioned = FALSE`, concurrent writes follow last-write-wins behavior.
#'   The most recent write will overwrite earlier writes.
#'
#' **Connection Management**:
#' * Snowflake connections can become invalid due to timeouts or network issues.
#'   The board checks connection health before operations and provides clear
#'   error messages with reconnection guidance.
#' * Auto-reconnect is not supported. If a connection becomes invalid, you must
#'   create a new connection and board object manually.
#' * Store `connect_args` when creating the board to get helpful reconnection
#'   examples in error messages.
#'
#' **Metadata**:
#' * Pin metadata (tags, URLs, descriptions) fully supports special characters,
#'   Unicode, and complex strings. All metadata is preserved exactly during
#'   write/read cycles.
#' * Large numbers of tags (50+) and multiple URLs are supported.
#'
#' **Versions**:
#' * Many versions (10+) per pin are supported and tested. Version management
#'   operations (listing, deleting specific versions) work correctly with
#'   large version counts.
#' * Deleting a middle version doesn't affect other versions.
#'
#' @inheritParams pins::new_board
#' @param conn A live DBI connection to Snowflake, created with
#'   `DBI::dbConnect(odbc::odbc(), Driver = "Snowflake", ...)`.
#' @param stage Stage name (e.g., `"@mystage"`, `"@~"`, or
#'   `"@database.schema.mystage"`). If the `@` prefix is missing, it will
#'   be added automatically.
#' @param path Optional path prefix inside the stage for this board. Use this
#'   to create multiple independent boards within a single stage. Defaults to
#'   `""` (stage root).
#' @param connect_args Optional named list of arguments that can be passed to
#'   `DBI::dbConnect(odbc::odbc(), ...)` to recreate the connection. This is
#'   stored in the board object and used by [pins::board_deparse()] to generate
#'   reproducible board creation code. If `NULL`, `board_deparse()` will fail.
#'
#' @return A Snowflake stage board object, which is a subclass of `pins_board`.
#'
#' @export
#' @examples
#' \dontrun{
#' # Connect to Snowflake using key pair authentication
#' library(DBI)
#' con <- dbConnect(
#'   odbc::odbc(),
#'   Driver = "Snowflake",
#'   Server = "myaccount.snowflakecomputing.com",
#'   UID = "myuser",
#'   Authenticator = "SNOWFLAKE_JWT",
#'   PRIV_KEY_FILE = "~/.snowflake/rsa_key.p8",
#'   Warehouse = "COMPUTE_WH"
#' )
#'
#' # Create a board using the user stage
#' board <- board_sf_stage(con, stage = "@~")
#'
#' # Write and read a pin
#' board |> pin_write(mtcars, "mtcars")
#' board |> pin_read("mtcars")
#'
#' # Use a named stage with a path prefix
#' board_prod <- board_sf_stage(
#'   con,
#'   stage = "@my_database.my_schema.pin_stage",
#'   path = "production"
#' )
#'
#' # Store connect_args for reproducibility
#' connect_args <- list(
#'   Driver = "Snowflake",
#'   Server = "myaccount.snowflakecomputing.com",
#'   UID = "myuser",
#'   Authenticator = "SNOWFLAKE_JWT",
#'   PRIV_KEY_FILE = "~/.snowflake/rsa_key.p8",
#'   Warehouse = "COMPUTE_WH"
#' )
#'
#' board <- board_sf_stage(
#'   con,
#'   stage = "@~",
#'   connect_args = connect_args
#' )
#'
#' # Now board_deparse() works
#' board_deparse(board)
#' }

board_sf_stage <- function(
  conn,
  stage,
  path = "",
  connect_args = NULL,
  versioned = TRUE,
  cache = NULL
) {
  # Verify that required packages (DBI, odbc) are installed
  sf_stage_check_driver()

  # Validate connection object
  if (!inherits(conn, "DBIConnection")) {
    cli::cli_abort("`conn` must be a DBI connection")
  }

  # Validate and normalize stage name
  if (!rlang::is_string(stage)) {
    cli::cli_abort("`stage` must be a string")
  }
  # Ensure stage name starts with @ (Snowflake convention)
  if (!startsWith(stage, "@")) {
    stage <- paste0("@", stage)
  }

  # Normalize path: "/" should be treated as empty path
  if (path == "/") {
    path <- ""
  }

  # Create a unique cache directory based on stage and path
  # This ensures different boards don't share cache even if using same stage
  cache <- cache %||% pins::board_cache_path(paste0("sf-", digest::digest(paste(stage, path))))

  # Create the board object with all necessary components
  pins::new_board(
    board = "pins_board_sf_stage",
    api = 1L,                    # Use pins API version 1
    cache = cache,
    versioned = versioned,
    name = "sf_stage",
    conn = conn,                 # Store connection for later use
    stage = stage,               # Normalized stage name
    path = path,                 # Path prefix within stage
    connect_args = connect_args  # For board_deparse() recreation
  )
}

#' @export
pin_list.pins_board_sf_stage <- function(board, ...) {
  # List all pin names (top-level directories) in the board
  sf_children(board)
}

#' @export
pin_exists.pins_board_sf_stage <- function(board, name, ...) {
  # Check if a pin exists by looking for its name in the pin list
  name %in% sf_children(board)
}

#' @export
pin_delete.pins_board_sf_stage <- function(board, names, ...) {
  # Delete one or more pins (all versions) from the board
  for (name in names) {
    sf_check_pin_exists(board, name)
    # Recursively delete the entire pin directory
    sf_stage_delete(board, name)
  }
  invisible(board)
}

#' @export
pin_versions.pins_board_sf_stage <- function(board, name, ...) {
  # Return a tibble of all versions for a given pin
  sf_check_pin_exists(board, name)
  # Get all version directories for this pin
  children <- sf_children(board, name)
  # Parse version strings into structured data
  sf_version_from_path(children)
}

#' @export
pin_version_delete.pins_board_sf_stage <- function(board, name, version, ...) {
  # Delete a specific version of a pin (not all versions)
  sf_stage_delete(board, fs::path(name, version))
}

#' @export
pin_meta.pins_board_sf_stage <- function(board, name, version = NULL, ...) {
  # Verify the pin exists before attempting to read metadata
  sf_check_pin_exists(board, name)

  # Resolve version: if NULL, get the latest; otherwise validate it exists
  version <- sf_check_pin_version(board, name, version)

  # Metadata is always stored as data.txt in the version directory
  metadata_key <- fs::path(name, version, "data.txt")

  # Verify the metadata file exists in the stage
  if (!sf_stage_exists(board, metadata_key)) {
    sf_abort_pin_version_missing(version)
  }

  # Create local cache directory for this specific version
  path_version <- fs::path(board$cache, name, version)
  fs::dir_create(path_version)

  # Download metadata file from stage to local cache
  sf_stage_download(board, metadata_key, dest_dir = path_version)

  # Parse the metadata file and add local path information
  sf_local_meta(
    sf_read_meta(path_version),
    name = name,
    dir = path_version,
    version = version
  )
}

#' @export
pin_fetch.pins_board_sf_stage <- function(board, name, version = NULL, ...) {
  # Get metadata for the pin (downloads data.txt if needed)
  meta <- pin_meta(board, name, version = version)

  # Update cache timestamp for this pin version
  sf_cache_touch(board, meta)

  # Download all data files associated with this pin
  # meta$file contains the list of files (e.g., "data.rds", "data.csv")
  for (file in meta$file) {
    # Construct the full path in the stage: name/version/file
    key <- fs::path(name, meta$local$version, file)
    # Download to the local cache directory for this version
    sf_stage_download(board, key, dest_dir = meta$local$dir)
  }

  # Return metadata object with all files now available locally
  meta
}

#' @export
pin_store.pins_board_sf_stage <- function(
  board,
  name,
  paths,
  metadata,
  versioned = NULL,
  x = NULL,
  ...
) {
  # Ensure any additional arguments are actually used
  rlang::check_dots_used()

  # Validate pin name (e.g., can't be "data.txt")
  sf_check_pin_name(name)

  # Determine version string and handle versioning logic
  # If versioned=FALSE and pin exists, this deletes the old version
  version <- sf_version_setup(
    board,
    name,
    sf_version_name(metadata),  # Generate version from timestamp + hash
    versioned = versioned
  )

  # All files for this pin version go in: name/version/
  version_dir <- fs::path(name, version)

  # Upload metadata file (data.txt) first
  # Write to temporary file then upload to stage
  tmp_meta <- withr::local_tempfile()
  yaml::write_yaml(metadata, tmp_meta)
  sf_stage_upload(board, src = tmp_meta, dest = fs::path(version_dir, "data.txt"))

  # Upload all data files (e.g., data.rds, data.csv, etc.)
  for (path in paths) {
    # Preserve original filename in the stage
    dest <- fs::path(version_dir, fs::path_file(path))
    sf_stage_upload(board, src = path, dest = dest)
  }

  # Return pin name (standard pins API)
  name
}

#' @export
board_deparse.pins_board_sf_stage <- function(board, ...) {
  # Deparsing requires connect_args to recreate the connection
  if (is.null(board$connect_args)) {
    cli::cli_abort("No `connect_args` stored for this board; cannot deparse connection")
  }

  # Build an R expression that recreates the DBI connection
  connect_call <- rlang::expr(DBI::dbConnect(odbc::odbc(), !!!board$connect_args))

  # Build the board_sf_stage() call with all necessary arguments
  # compact() removes NULL values
  board_args <- purrr::compact(list(
    conn = connect_call,
    stage = board$stage,
    path = board$path,
    connect_args = board$connect_args,
    versioned = board$versioned
  ))

  # Return an expression that recreates this board
  rlang::expr(board_sf_stage(!!!board_args))
}

#' @export
write_board_manifest_yaml.pins_board_sf_stage <- function(board, manifest, ...) {
  # Manifest is stored at the root of the board as _pins.yaml
  manifest_path <- sf_manifest_pin_yaml_filename

  # Delete existing manifest if present (will be replaced)
  if (sf_stage_exists(board, manifest_path)) {
    sf_stage_delete(board, manifest_path)
  }

  # Write manifest to temporary file then upload to stage
  temp_file <- withr::local_tempfile()
  yaml::write_yaml(manifest, file = temp_file)
  sf_stage_upload(board, src = temp_file, dest = manifest_path)
}

#' @export
required_pkgs.pins_board_sf_stage <- function(x, ...) {
  rlang::check_dots_empty()
  c("DBI", "odbc")
}
