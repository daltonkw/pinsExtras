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
  DBI::dbGetQuery(board$conn, sql)
}

sf_stage_list <- function(board, dir = "") {
  prefix <- sf_normalize_path(board, dir)
  df <- sf_stage_cmd(board, sprintf("LIST %s", board$stage))

  if (nrow(df) == 0) {
    return(df)
  }

 # For named stages, Snowflake prefixes paths with the stage name (e.g., "mystage/...")
 # Strip this prefix so paths are relative to the stage root
  stage_name <- sf_extract_stage_name(board$stage)
  stage_prefix <- paste0(stage_name, "/")
  if (all(startsWith(df$name, stage_prefix))) {
    df$name <- sub(paste0("^", stage_prefix), "", df$name)
  }

  if (prefix != "") {
    # Match exact path OR directory children (prefix + "/")
    # This prevents "mtcars" from matching "mtcars_*"
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

sf_stage_download <- function(board, key, dest_dir) {
  dest_dir <- fs::path_abs(fs::path_expand(dest_dir))
  fs::dir_create(dest_dir)
  target <- sf_stage_path(board, key)
  sql <- sprintf(
    "GET %s file://%s",
    target,
    fs::path(dest_dir, "")
  )
  sf_stage_cmd(board, sql)
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
