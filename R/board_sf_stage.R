#' Use a Snowflake stage as a board
#'
#' Pin data to a Snowflake internal stage using the Snowflake ODBC driver via
#' DBI + odbc.
#'
#' @inheritParams pins::new_board
#' @param conn A live `DBIConnection` to Snowflake, typically created with
#'   `DBI::dbConnect(odbc::odbc(), ...)`.
#' @param stage Stage name (e.g., "@mystage" or "@~"). If missing the `@`
#'   prefix, it will be added.
#' @param path Optional path prefix inside the stage for this board.
#' @param connect_args Optional list of arguments that can be used to recreate
#'   the connection via `DBI::dbConnect(odbc::odbc(), !!!connect_args)`. Stored
#'   for use by [pins::board_deparse()].
#' @export
board_sf_stage <- function(
  conn,
  stage,
  path = "",
  connect_args = NULL,
  versioned = TRUE,
  cache = NULL
) {
  sf_stage_check_driver()

  if (!inherits(conn, "DBIConnection")) {
    cli::cli_abort("`conn` must be a DBI connection")
  }
  if (!rlang::is_string(stage)) {
    cli::cli_abort("`stage` must be a string")
  }
  if (!startsWith(stage, "@")) {
    stage <- paste0("@", stage)
  }
  if (path == "/") {
    path <- ""
  }
  cache <- cache %||% pins::board_cache_path(paste0("sf-", digest::digest(paste(stage, path))))

  pins::new_board(
    board = "pins_board_sf_stage",
    api = 1L,
    cache = cache,
    versioned = versioned,
    name = "sf_stage",
    conn = conn,
    stage = stage,
    path = path,
    connect_args = connect_args
  )
}

#' @method pin_list pins_board_sf_stage
#' @export
pin_list.pins_board_sf_stage <- function(board, ...) {
  sf_children(board)
}

#' @export
#' @method pin_exists pins_board_sf_stage
pin_exists.pins_board_sf_stage <- function(board, name, ...) {
  name %in% sf_children(board)
}

#' @export
#' @method pin_delete pins_board_sf_stage
pin_delete.pins_board_sf_stage <- function(board, names, ...) {
  for (name in names) {
    sf_check_pin_exists(board, name)
    sf_stage_delete(board, name)
  }
  invisible(board)
}

#' @export
#' @method pin_versions pins_board_sf_stage
pin_versions.pins_board_sf_stage <- function(board, name, ...) {
  sf_check_pin_exists(board, name)
  children <- sf_children(board, name)
  sf_version_from_path(children)
}

#' @export
#' @method pin_version_delete pins_board_sf_stage
pin_version_delete.pins_board_sf_stage <- function(board, name, version, ...) {
  sf_stage_delete(board, fs::path(name, version))
}

#' @export
#' @method pin_meta pins_board_sf_stage
pin_meta.pins_board_sf_stage <- function(board, name, version = NULL, ...) {
  sf_check_pin_exists(board, name)
  version <- sf_check_pin_version(board, name, version)
  metadata_key <- fs::path(name, version, "data.txt")

  if (!sf_stage_exists(board, metadata_key)) {
    sf_abort_pin_version_missing(version)
  }

  path_version <- fs::path(board$cache, name, version)
  fs::dir_create(path_version)

  sf_stage_download(board, metadata_key, dest_dir = path_version)
  sf_local_meta(
    sf_read_meta(path_version),
    name = name,
    dir = path_version,
    version = version
  )
}

#' @export
#' @method pin_fetch pins_board_sf_stage
pin_fetch.pins_board_sf_stage <- function(board, name, version = NULL, ...) {
  meta <- pin_meta(board, name, version = version)
  sf_cache_touch(board, meta)

  for (file in meta$file) {
    key <- fs::path(name, meta$local$version, file)
    sf_stage_download(board, key, dest_dir = meta$local$dir)
  }

  meta
}

#' @export
#' @method pin_store pins_board_sf_stage
pin_store.pins_board_sf_stage <- function(
  board,
  name,
  paths,
  metadata,
  versioned = NULL,
  x = NULL,
  ...
) {
  rlang::check_dots_used()
  sf_check_pin_name(name)
  version <- sf_version_setup(
    board,
    name,
    sf_version_name(metadata),
    versioned = versioned
  )

  version_dir <- fs::path(name, version)

  tmp_meta <- withr::local_tempfile()
  yaml::write_yaml(metadata, tmp_meta)
  sf_stage_upload(board, src = tmp_meta, dest = fs::path(version_dir, "data.txt"))

  for (path in paths) {
    dest <- fs::path(version_dir, fs::path_file(path))
    sf_stage_upload(board, src = path, dest = dest)
  }

  name
}

#' @export
#' @method board_deparse pins_board_sf_stage
board_deparse.pins_board_sf_stage <- function(board, ...) {
  if (is.null(board$connect_args)) {
    cli::cli_abort("No `connect_args` stored for this board; cannot deparse connection")
  }
  connect_call <- rlang::expr(DBI::dbConnect(odbc::odbc(), !!!board$connect_args))

  board_args <- purrr::compact(list(
    conn = connect_call,
    stage = board$stage,
    path = board$path,
    connect_args = board$connect_args,
    versioned = board$versioned
  ))

  rlang::expr(board_sf_stage(!!!board_args))
}

#' @export
#' @method write_board_manifest_yaml pins_board_sf_stage
write_board_manifest_yaml.pins_board_sf_stage <- function(board, manifest, ...) {
  manifest_path <- sf_manifest_pin_yaml_filename
  if (sf_stage_exists(board, manifest_path)) {
    sf_stage_delete(board, manifest_path)
  }

  temp_file <- withr::local_tempfile()
  yaml::write_yaml(manifest, file = temp_file)
  sf_stage_upload(board, src = temp_file, dest = manifest_path)
}

#' @export
#' @method required_pkgs pins_board_sf_stage
required_pkgs.pins_board_sf_stage <- function(x, ...) {
  rlang::check_dots_empty()
  c("DBI", "odbc")
}
