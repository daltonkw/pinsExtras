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

sf_stage_build_put <- function(board, src, dest, overwrite) {
  # One construction point for the PUT verb, shared by sf_stage_upload() and
  # sf_stage_upload_meta(). OVERWRITE=FALSE is the default: a version
  # directory that already holds a failed attempt's files must never be
  # silently replaced, which would mix two attempts together.
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

  sprintf(
    "PUT %s %s AUTO_COMPRESS=FALSE OVERWRITE=%s",
    sf_quote_file_uri(upload_src),
    sf_quote_stage_path(sf_stage_path(board, dest_dir)),
    if (overwrite) "TRUE" else "FALSE"
  )
}

sf_stage_upload <- function(board, src, dest, overwrite = FALSE,
                            call = rlang::caller_env()) {
  sql <- sf_stage_build_put(board, src, dest, overwrite)
  result <- sf_stage_cmd(board, sql)
  sf_check_put_result(result, fs::path_file(dest), dest, call = call)
  invisible(TRUE)
}

sf_stage_upload_meta <- function(board, src, dest, call = rlang::caller_env()) {
  # data.txt is the publication marker; it must never overwrite either, so
  # OVERWRITE is fixed to FALSE regardless of the caller's intent.
  sql <- sf_stage_build_put(board, src, dest, overwrite = FALSE)
  result <- sf_stage_cmd(board, sql)
  sf_check_meta_put_result(result, fs::path_file(dest), dest, call = call)
  invisible(TRUE)
}

# Validate a Snowflake PUT response before trusting the file that landed.
#
#   result: the raw response row set from the PUT command
#   file: the expected file basename
#   key: the board-relative key, used in the abort message
sf_check_put_result <- function(result, file, key, call = rlang::caller_env()) {
  # Column names and status are matched case-insensitively.
  if (!is.null(result)) {
    names(result) <- tolower(names(result))
  }
  if (is.null(result) || nrow(result) == 0L) {
    cli::cli_abort(
      c(
        "Failed to upload {.path {key}}.",
        "x" = "Snowflake returned no upload result."
      ),
      class = "pinsExtras_upload_failed",
      call = call
    )
  }
  if (nrow(result) > 1L) {
    rows <- nrow(result)
    cli::cli_abort(
      c(
        "Failed to upload {.path {key}}.",
        "x" = "Snowflake returned {rows} results for a single-file upload."
      ),
      class = "pinsExtras_upload_failed",
      call = call
    )
  }
  # Use a names() membership test, never "$": "$" partial-matches, so a
  # response carrying "target_size" but no "target" would slip past a
  # is.null(result$target) check and die on an unclassed error below.
  if (
    !("target" %in% names(result)) ||
      !("status" %in% names(result))
  ) {
    cli::cli_abort(
      c(
        "Failed to upload {.path {key}}.",
        "x" = "Snowflake's upload response could not be interpreted."
      ),
      class = "pinsExtras_upload_failed",
      call = call
    )
  }
  status <- toupper(as.character(result$status))
  # An NA status throws on "!=" rather than branching, so check it here
  # before any comparison against "UPLOADED" or "SKIPPED".
  if (is.na(status)) {
    cli::cli_abort(
      c(
        "Failed to upload {.path {key}}.",
        "x" = "Snowflake's upload response could not be interpreted."
      ),
      class = "pinsExtras_upload_failed",
      call = call
    )
  }
  if (status == "SKIPPED") {
    cli::cli_abort(
      c(
        "Failed to upload {.path {key}}.",
        "x" = "Snowflake skipped the upload, so the file was already present."
      ),
      class = "pinsExtras_upload_failed",
      call = call
    )
  }
  if (status != "UPLOADED") {
    cli::cli_abort(
      c(
        "Failed to upload {.path {key}}.",
        "x" = "Snowflake reported status {.val {status}}."
      ),
      class = "pinsExtras_upload_failed",
      call = call
    )
  }
  got <- fs::path_file(result$target[[1]])
  if (got != file) {
    cli::cli_abort(
      c(
        "Failed to upload {.path {key}}.",
        "x" = "Snowflake returned {.path {got}} instead."
      ),
      class = "pinsExtras_upload_failed",
      call = call
    )
  }
  invisible(TRUE)
}

# Validate the PUT response for the metadata (data.txt) upload.
#
# data.txt is the publication marker: whether it landed decides whether the
# version is visible to every reader. So an UNINTERPRETABLE response is
# treated as publication_uncertain (the caller must stop and inspect), while
# a clear, interpretable FAILURE (SKIPPED, another status, wrong target)
# is just an upload failure. This mirrors sf_check_put_result() but splits
# its aborts across two classes. The split is what lets U11 avoid deleting
# the previous version when we cannot tell whether data.txt actually landed.
#
#   result: the raw response row set from the PUT command
#   file: the expected file basename
#   key: the board-relative key, used in the abort message
sf_check_meta_put_result <- function(
  result,
  file,
  key,
  call = rlang::caller_env()
) {
  # Column names and status are matched case-insensitively.
  if (!is.null(result)) {
    names(result) <- tolower(names(result))
  }
  # Checks 1-3: no result, several results, or a missing/NA column means we
  # cannot tell whether the metadata landed. This is the uncertain case.
  if (is.null(result) || nrow(result) == 0L) {
    cli::cli_abort(
      c(
        "Publication of {.path {key}} is uncertain.",
        "x" = paste0(
          "Snowflake's response to the metadata upload ",
          "could not be interpreted."
        ),
        "i" = paste0(
          "The version may or may not be published; ",
          "inspect it before writing again."
        ),
        "i" = "Nothing was deleted."
      ),
      class = "pinsExtras_publication_uncertain",
      call = call
    )
  }
  if (nrow(result) > 1L) {
    cli::cli_abort(
      c(
        "Publication of {.path {key}} is uncertain.",
        "x" = paste0(
          "Snowflake's response to the metadata upload ",
          "could not be interpreted."
        ),
        "i" = paste0(
          "The version may or may not be published; ",
          "inspect it before writing again."
        ),
        "i" = "Nothing was deleted."
      ),
      class = "pinsExtras_publication_uncertain",
      call = call
    )
  }
  if (
    !("target" %in% names(result)) ||
      !("status" %in% names(result))
  ) {
    cli::cli_abort(
      c(
        "Publication of {.path {key}} is uncertain.",
        "x" = paste0(
          "Snowflake's response to the metadata upload ",
          "could not be interpreted."
        ),
        "i" = paste0(
          "The version may or may not be published; ",
          "inspect it before writing again."
        ),
        "i" = "Nothing was deleted."
      ),
      class = "pinsExtras_publication_uncertain",
      call = call
    )
  }
  status <- toupper(as.character(result$status))
  if (is.na(status)) {
    cli::cli_abort(
      c(
        "Publication of {.path {key}} is uncertain.",
        "x" = paste0(
          "Snowflake's response to the metadata upload ",
          "could not be interpreted."
        ),
        "i" = paste0(
          "The version may or may not be published; ",
          "inspect it before writing again."
        ),
        "i" = "Nothing was deleted."
      ),
      class = "pinsExtras_publication_uncertain",
      call = call
    )
  }
  # From here the response is interpretable. A clear failure aborts as a
  # normal upload failure, reusing sf_check_put_result()'s wording.
  if (status == "SKIPPED") {
    cli::cli_abort(
      c(
        "Failed to upload {.path {key}}.",
        "x" = "Snowflake skipped the upload, so the file was already present."
      ),
      class = "pinsExtras_upload_failed",
      call = call
    )
  }
  if (status != "UPLOADED") {
    cli::cli_abort(
      c(
        "Failed to upload {.path {key}}.",
        "x" = "Snowflake reported status {.val {status}}."
      ),
      class = "pinsExtras_upload_failed",
      call = call
    )
  }
  got <- fs::path_file(result$target[[1]])
  if (got != file) {
    cli::cli_abort(
      c(
        "Failed to upload {.path {key}}.",
        "x" = "Snowflake returned {.path {got}} instead."
      ),
      class = "pinsExtras_upload_failed",
      call = call
    )
  }
  invisible(TRUE)
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

# Delete a directory and everything under it from the stage.
#
# The only difference from sf_stage_delete() is the trailing slash: it
# scopes the REMOVE to the directory. Without it Snowflake matches by
# prefix and "cars" would also remove a sibling named "cars_extra".
sf_stage_delete_dir <- function(board, dir, call = rlang::caller_env()) {
  # Guard against the stage root, not the board root: a board with a path
  # ("team-data") is allowed to delete that whole path with dir = "".
  if (
    !is.character(dir) || length(dir) != 1L || is.na(dir) ||
      sf_normalize_path(board, dir) == ""
  ) {
    cli::cli_abort(
      c(
        "Refusing to delete the whole stage.",
        "x" = "{.arg dir} must name a directory inside the board."
      ),
      class = "pinsExtras_invalid_delete_target",
      call = call
    )
  }
  sql <- sprintf(
    "REMOVE %s",
    sf_quote_stage_path(paste0(sf_stage_path(board, dir), "/"))
  )
  sf_stage_cmd(board, sql)
  invisible(TRUE)
}

# Delete exactly one file from the stage, and nothing else.
#
# A bare prefix is again too broad, so in addition to the trailing slash
# this anchors a PATTERN that matches only the named file. Because Snowflake
# applies the PATTERN to the full staged path, it is built from the
# board-relative directory: sf_remove_pattern() prefixes "^(.*/)?" to absorb
# whatever stage-name prefix the full path carries.
sf_stage_delete_file <- function(board, dir, file, call = rlang::caller_env()) {
  # dir is intentionally not guarded here: deleting one named file from the
  # board root is how the manifest is removed.
  if (
    !is.character(file) || length(file) != 1L || is.na(file) ||
      file == "" || grepl("/", file, fixed = TRUE)
  ) {
    cli::cli_abort(
      "{.arg file} must be a single file name",
      class = "pinsExtras_invalid_delete_target",
      call = call
    )
  }
  sql <- sprintf(
    "REMOVE %s PATTERN = %s",
    sf_quote_stage_path(paste0(sf_stage_path(board, dir), "/")),
    sf_quote_sql_literal(
      sf_remove_pattern(sf_normalize_path(board, dir), file)
    )
  )
  sf_stage_cmd(board, sql)
  invisible(TRUE)
}
