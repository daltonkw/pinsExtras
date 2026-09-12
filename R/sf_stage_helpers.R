# Helper functions for Snowflake stage boards

sf_stage_check_driver <- function() {
  rlang::check_installed("DBI")
  rlang::check_installed("odbc")
}

sf_stage_path <- function(board, ...) {
  path <- sf_normalize_path(board, ...)
  fs::path(board$stage, path)
}

sf_normalize_path <- function(board, ...) {
  path <- fs::path(board$path, ...)
  bads <- nchar(path) > 1 & grepl("^/", path)
  path[bads] <- substr(path[bads], 2, nchar(path[bads]))
  path <- gsub("//+", "/", path)
  # Handle edge case: fs::path("", "") returns "/" which should be ""
  path[path == "/"] <- ""
  path
}

sf_stage_cmd <- function(board, sql) {
  # Check connection health before executing command
  # This provides helpful error messages if connection is dead
  sf_check_connection(board)

  DBI::dbGetQuery(board$conn, sql)
}

sf_stage_list <- function(board, dir = "") {
  # Scope the LIST to the requested directory. Snowflake matches by path
  # prefix, so include a trailing slash whenever the prefix is non-empty to
  # keep "cars" from also matching a sibling named "cars_extra".
  prefix <- sf_normalize_path(board, dir)
  if (prefix == "") {
    target <- board$stage
  } else {
    target <- paste0(board$stage, "/", prefix, "/")
  }
  df <- sf_stage_cmd(board, sprintf("LIST %s", sf_quote_stage_path(target)))

  # A driver that returns upper-case columns must behave identically to one
  # that returns lower-case: normalise the column names before use. Do this
  # before the zero-row return so an empty response keeps its names too.
  names(df) <- tolower(names(df))

  if (nrow(df) == 0) {
    return(df)
  }

  # For named stages, Snowflake prefixes paths with the stage name (e.g., "mystage/...")
  # Strip this prefix so paths are relative to the stage root
  # Note: Snowflake stores unquoted identifiers as uppercase but LIST returns lowercase paths
  # So we must use case-insensitive matching here
  stage_name <- sf_extract_stage_name(board$stage)
  stage_prefix <- paste0(stage_name, "/")
  if (all(startsWith(tolower(df$name), tolower(stage_prefix)))) {
    df$name <- substr(df$name, nchar(stage_prefix) + 1L, nchar(df$name))
  }

  if (prefix != "") {
    # Match exact path OR directory children (prefix + "/")
    # This prevents "mtcars" from matching "mtcars_pqt"
    is_exact <- df$name == prefix
    is_child <- startsWith(df$name, paste0(prefix, "/"))
    df <- df[is_exact | is_child, , drop = FALSE]
  }
  df
}

# Extract the stage name from a stage reference
# "@db.schema.stage" -> "stage"
# "@stage" -> "stage"
# "@~" -> "~"
sf_extract_stage_name <- function(stage) {
  # Remove leading @
  s <- sub("^@", "", stage)
  # Get the last component (after last dot, if any)
  parts <- strsplit(s, "\\.")[[1]]
  parts[[length(parts)]]
}

sf_stage_upload <- function(board, src, dest) {
  src <- normalizePath(src, winslash = "/", mustWork = TRUE)
  dest_dir <- fs::path_dir(dest)
  if (dest_dir == ".") {
    dest_dir <- ""
  }
  fname <- fs::path_file(dest)

  upload_src <- src
  if (fs::path_file(src) != fname) {
    tmp_dir <- withr::local_tempdir(.local_envir = parent.frame())
    upload_src <- fs::path(tmp_dir, fname)
    fs::file_copy(src, upload_src, overwrite = TRUE)
  }

  target <- sf_stage_path(board, dest_dir)
  sql <- sprintf(
    "PUT file://%s %s AUTO_COMPRESS=FALSE OVERWRITE=TRUE",
    upload_src,
    target
  )
  sf_stage_cmd(board, sql)
}

sf_stage_download <- function(
  board, key, dest_dir, call = rlang::caller_env()
) {
  # Transfer into a fresh directory so a stale copy can never stand in for
  # what this call actually fetched (see 3.1).
  tmp <- withr::local_tempdir()
  dest_dir <- fs::path_abs(fs::path_expand(dest_dir))
  fs::dir_create(dest_dir)
  target <- sf_stage_path(board, key)
  result <- sf_stage_cmd(
    board,
    sprintf(
      "GET %s %s",
      sf_quote_stage_path(target),
      sf_quote_file_uri(fs::path(tmp, ""))
    )
  )
  # Prove the transfer succeeded before returning anything.
  sf_check_get_result(result, fs::path_file(key), key, call = call)
  tmp_file <- fs::path(tmp, fs::path_file(key))
  if (!fs::file_exists(tmp_file)) {
    cli::cli_abort(
      c(
        "Failed to download {.path {key}}.",
        "x" = "The downloaded file is missing from the transfer directory."
      ),
      class = "pinsExtras_download_failed",
      call = call
    )
  }
  out <- fs::path(dest_dir, fs::path_file(key))
  fs::file_copy(tmp_file, out, overwrite = TRUE)
  invisible(as.character(out))
}

# Validate a Snowflake GET response before trusting the file that landed.
#
#   result: the raw response row set from the GET command
#   file: the expected file basename
#   key: the board-relative key, used in the abort message
sf_check_get_result <- function(result, file, key, call = rlang::caller_env()) {
  # Column names and status are matched case-insensitively.
  if (!is.null(result)) {
    names(result) <- tolower(names(result))
  }
  if (is.null(result) || nrow(result) == 0L) {
    cli::cli_abort(
      c(
        "Failed to download {.path {key}}.",
        "x" = "Snowflake returned no download result."
      ),
      class = "pinsExtras_download_failed",
      call = call
    )
  }
  if (nrow(result) > 1L) {
    rows <- nrow(result)
    cli::cli_abort(
      c(
        "Failed to download {.path {key}}.",
        "x" = "Snowflake returned {rows} results for a single-file download."
      ),
      class = "pinsExtras_download_failed",
      call = call
    )
  }
  if (!("file" %in% names(result)) || !("status" %in% names(result))) {
    cli::cli_abort(
      c(
        "Failed to download {.path {key}}.",
        "x" = "Snowflake's download response could not be interpreted."
      ),
      class = "pinsExtras_download_failed",
      call = call
    )
  }
  status <- toupper(as.character(result$status))
  if (status != "DOWNLOADED") {
    cli::cli_abort(
      c(
        "Failed to download {.path {key}}.",
        "x" = "Snowflake reported status {.val {status}}."
      ),
      class = "pinsExtras_download_failed",
      call = call
    )
  }
  got <- fs::path_file(result$file[[1]])
  if (got != file) {
    cli::cli_abort(
      c(
        "Failed to download {.path {key}}.",
        "x" = "Snowflake returned {.path {got}} instead."
      ),
      class = "pinsExtras_download_failed",
      call = call
    )
  }
  invisible(TRUE)
}

sf_stage_exists <- function(board, path) {
  nrow(sf_stage_list(board, path)) > 0
}

sf_stage_delete <- function(board, path) {
  target <- sf_stage_path(board, path)
  sql <- sprintf("REMOVE %s", target)
  sf_stage_cmd(board, sql)
}

sf_children <- function(board, dir = "") {
  dir_norm <- sf_normalize_path(board, dir)
  df <- sf_stage_list(board, dir)
  if (nrow(df) == 0) {
    return(character())
  }

  rel <- df$name
  dir_prefix <- if (dir_norm == "") "" else paste0(sf_end_with_slash(dir_norm))
  rel <- sub(paste0("^", dir_prefix), "", rel)
  pieces <- strsplit(rel, "/")
  unique(purrr::map_chr(pieces, ~ .x[[1]]))
}
