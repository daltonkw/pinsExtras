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
#'   required dependencies of pinsExtras and are installed with it. You also
#'   need the Snowflake ODBC driver installed on your system, which is not an
#'   R package and cannot be installed by `install.packages()`.
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
#' * Writes to a single pin are **not** safe to run concurrently. The contract
#'   is serialized per pin: one writer at a time. Writes to *different* pins
#'   may run at the same time.
#' * A version id is derived from a timestamp and a content hash, so two
#'   writers of the same content in the same second produce the *same* id.
#'   That is detected and raised as an error rather than merging two payloads
#'   into one version directory. It is a collision report, not a guarantee of
#'   safe interleaving.
#' * With `versioned = FALSE`, a replacement uploads the new version before
#'   removing the old one, so the pin is never absent. Two concurrent
#'   replacements are still unsupported and may leave an extra version
#'   directory behind; use `pin_versions()` and `pin_version_delete()` to
#'   inspect and clean up.
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
  # Derive the published index from one board-scoped listing and hand back
  # the pin names. The index proves publication, so a payload-only directory
  # never appears as a pin.
  index <- sf_board_index(board)
  sf_index_pins(index)
}

# Build the published-pin index from a single listing.
#
#   board: the board object
#   dir:   the scope for the LIST ("" for the board root, a pin name otherwise).
# The prefix is the board's own path only. sf_stage_list() still scopes the
# request to `dir`, but sf_published_index() strips just the board path so the
# pin and version segments survive: scoping the strip to the pin would leave
# only "<version>/data.txt", and the three-segment rule would reject every
# row. A board-scoped and a pin-scoped listing are therefore read the same way.
sf_board_index <- function(board, dir = "") {
  listing <- sf_stage_list(board, dir)
  sf_published_index(listing, prefix = sf_normalize_path(board))
}

#' @export
pin_exists.pins_board_sf_stage <- function(board, name, ...) {
  # Answer from the index, not from the directory tree, so a half-finished
  # upload (payloads but no data.txt) is not mistaken for a real pin.
  index <- sf_board_index(board, name)
  sf_index_has_pin(index, name)
}

#' @export
pin_delete.pins_board_sf_stage <- function(board, names, ...) {
  # Delete one or more pins (all versions) from the board
  for (name in names) {
    # A name must be a single safe path segment: non-empty, no directory
    # separator, no dot or dotdot. A supplied ".." would delete the whole
    # board, so the validator aborts before any listing or REMOVE.
    sf_check_path_segment(name, arg = "names")
    # One pin-scoped listing, then the published check on the derived index.
    # A payload-only orphan has no data.txt, so it is not published and is
    # reported as absent: use pin_version_delete() to remove it raw.
    listing <- sf_stage_list(board, name)
    index <- sf_published_index(listing, prefix = sf_normalize_path(board))
    sf_check_pin_published(index, name)
    # Delete the entire pin directory, scoped with a trailing slash.
    sf_stage_delete_dir(board, name)
  }
  invisible(board)
}

#' @export
pin_versions.pins_board_sf_stage <- function(board, name, ...) {
  # One pin-scoped listing, then the versions from the index. The published
  # check aborts "Can't find pin called ..." when the pin has no version.
  index <- sf_board_index(board, name)
  sf_check_pin_published(index, name)
  sf_index_versions(index, name)
}

#' @export
pin_version_delete.pins_board_sf_stage <- function(board, name, version, ...) {
  # Delete a specific version of a pin (not all versions). No listing and no
  # existence check: this is the raw directory delete, used for an incomplete
  # version directory that discovery cannot see.
  # A supplied ".." or separator would delete the whole board, so the
  # validator aborts before any REMOVE is issued.
  sf_check_path_segment(name, arg = "name")
  sf_check_path_segment(version, arg = "version")
  sf_stage_delete_dir(board, fs::path(name, version))
  invisible(board)
}

#' @export
pin_meta.pins_board_sf_stage <- function(board, name, version = NULL, ...) {
  # One pin-scoped listing, then resolve the version locally against the index
  # we already hold. sf_resolve_version() performs the published-pin check,
  # so there is no separate existence test and no extra listing here.
  index <- sf_board_index(board, name)
  version <- sf_resolve_version(index, name, version)

  # Metadata is always stored as data.txt in the version directory
  path_version <- fs::path(board$cache, name, version)
  fs::dir_create(path_version)

  # Download metadata file from stage to local cache
  sf_stage_download(board, fs::path(name, version, "data.txt"),
                    dest_dir = path_version)

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
  # pin_meta() downloads the metadata and issues the only metadata GET; this
  # method then fetches each payload named in that metadata and issues one
  # GET per file. No further listing happens.
  meta <- pin_meta(board, name, version = version)

  # Update cache timestamp for this pin version
  sf_cache_touch(board, meta)

  # Download each payload file named in the metadata. Use the version pin_meta
  # resolved (may have been NULL), not the caller's raw argument.
  for (file in meta$file) {
    key <- fs::path(name, meta$local$version, file)
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

  # Validate pin name and the whole local upload set before a single PUT
  # moves a byte, so nothing is sent to the stage on bad input.
  sf_check_pin_name(name)
  sf_check_upload_set(name, paths, metadata)

  # Resolve the new version and do exactly one pin-scoped listing. Both the
  # plan below and the collision check read the same listing, so neither
  # lists again. A published duplicate is caught by sf_version_plan(), an
  # incomplete-directory collision by sf_check_version_collision(); both
  # happen before any PUT, and the first is the upstream "same as the most
  # recent version" behaviour.
  version <- sf_version_name(metadata)
  prefix  <- sf_normalize_path(board)
  listing <- sf_stage_list(board, name)
  index   <- sf_published_index(listing, prefix = prefix)
  plan    <- sf_version_plan(
    index,
    name,
    version,
    versioned = versioned,
    board_versioned = board$versioned
  )
  sf_check_version_collision(listing, name, version, prefix = prefix)

  # Progress output the user can silence with options(pins.quiet = TRUE).
  if (plan$action == "create") {
    sf_inform("Creating new version {.val {version}}")
  } else {
    sf_inform(
      "Replacing version {.val {plan$old_versions}} with {.val {version}}"
    )
  }

  # Upload each payload keyed as name/version/filename, in the order given.
  version_dir <- fs::path(name, version)
  for (path in paths) {
    dest <- fs::path(version_dir, fs::path_file(path))
    sf_stage_upload(board, src = path, dest = dest)
  }

  # data.txt is ALWAYS LAST and goes through the metadata upload: an
  # uninterpretable metadata response therefore becomes publication_
  # uncertain rather than a plain failure, which is what lets the caller
  # skip cleanup when it cannot tell whether the version published.
  tmp_meta <- withr::local_tempfile()
  yaml::write_yaml(metadata, tmp_meta)
  sf_stage_upload_meta(
    board,
    src = tmp_meta,
    dest = fs::path(version_dir, "data.txt")
  )

  # Cleanup only for replace, only over the old versions, never touching the
  # new version. A cleanup failure must not fail an otherwise successful
  # write, so it warns instead of aborting.
  if (plan$action == "replace") {
    remaining <- sf_cleanup_old_versions(board, name, plan$old_versions)
    if (length(remaining) > 0L) {
      cli::cli_warn(
        c(
          paste0(
            "Published pin {.val {name}} version {.val {version}}, but ",
            "cleanup is incomplete."
          ),
          "i" = "These old versions still have files: {.val {remaining}}.",
          "i" = paste0(
            "Read the new version explicitly with ",
            "{.code pin_read(board, \"{name}\", version = \"{version}\")}."
          )
        ),
        class = "pinsExtras_cleanup_incomplete"
      )
    }
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
  # Manifest is stored at the root of the board as _pins.yaml. Overwrite it
  # directly; the delete-before-upload step is gone.
  manifest_path <- sf_manifest_pin_yaml_filename

  temp_file <- withr::local_tempfile()
  yaml::write_yaml(manifest, file = temp_file)
  sf_stage_upload(
    board,
    src = temp_file,
    dest = manifest_path,
    overwrite = TRUE
  )
}

#' @export
required_pkgs.pins_board_sf_stage <- function(x, ...) {
  rlang::check_dots_empty()
  c("DBI", "odbc")
}
