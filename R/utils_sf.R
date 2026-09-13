# Small utility helpers to avoid relying on pins internals

sf_manifest_pin_yaml_filename <- "_pins.yaml"

sf_end_with_slash <- function(x) {
  has_slash <- grepl("/$", x)
  x[!has_slash] <- paste0(x[!has_slash], "/")
  x
}

sf_check_pin_name <- function(name, call = rlang::caller_env()) {
  # Not a string is first: more than one condition can be true at once, and
  # this order is the contract.
  if (!rlang::is_string(name)) {
    cli::cli_abort("{.arg name} must be a string", call = call)
  }
  if (name == "") {
    cli::cli_abort("{.arg name} must not be empty", call = call)
  }
  # "data.txt" is reserved wherever it appears; path_file() strips any
  # directory first, so "a/data.txt" is reported as the reserved name.
  if (fs::path_file(name) == "data.txt") {
    cli::cli_abort("Can't pin file called {.code data.txt}", call = call)
  }
  # A name that can escape its directory, or is exactly ".", is rejected.
  has_sep <- grepl("/", name, fixed = TRUE) ||
    grepl("\\", name, fixed = TRUE) ||
    grepl("*", name, fixed = TRUE) ||
    grepl("?", name, fixed = TRUE) ||
    grepl("..", name, fixed = TRUE) ||
    name == "."
  if (has_sep) {
    cli::cli_abort(
      c(
        "Invalid pin name {.val {name}}.",
        "x" = paste0(
          "Pin names cannot contain {.code /}, {.code \\},",
          " {.code *}, {.code ?} or {.code ..}."
        )
      ),
      call = call
    )
  }
  invisible(TRUE)
}

# Validate the whole local upload set before the first PUT moves a byte,
# so a partial failure cannot leave files on the stage a later reader
# cannot make sense of. Every abort shares a first line, a class and a
# call; the six checks are written out inline rather than through a
# wrapper so cli can find the check's own locals.
sf_check_upload_set <- function(
  name, paths, metadata, call = rlang::caller_env()
) {
  if (length(paths) == 0L) {
    cli::cli_abort(
      c(
        "Can't upload pin {.val {name}}.",
        "x" = "The upload set is empty."
      ),
      class = "pinsExtras_invalid_upload_set",
      call = call
    )
  }
  absent <- paths[!fs::file_exists(paths)]
  if (length(absent) > 0L) {
    cli::cli_abort(
      c(
        "Can't upload pin {.val {name}}.",
        "x" = "These local files do not exist: {.path {absent}}."
      ),
      class = "pinsExtras_invalid_upload_set",
      call = call
    )
  }
  basenames <- fs::path_file(paths)
  if (any(basenames == "data.txt")) {
    cli::cli_abort(
      c(
        "Can't upload pin {.val {name}}.",
        "x" = "A pinned file cannot be named {.path data.txt}."
      ),
      class = "pinsExtras_invalid_upload_set",
      call = call
    )
  }
  bad <- basenames == "" |
    basenames == "." |
    grepl("/", basenames, fixed = TRUE) |
    grepl("\\", basenames, fixed = TRUE) |
    grepl("*", basenames, fixed = TRUE) |
    grepl("?", basenames, fixed = TRUE) |
    grepl("..", basenames, fixed = TRUE)
  if (any(bad)) {
    cli::cli_abort(
      c(
        "Can't upload pin {.val {name}}.",
        "x" = "These file names are not allowed: {.path {basenames[bad]}}."
      ),
      class = "pinsExtras_invalid_upload_set",
      call = call
    )
  }
  dupes <- unique(basenames[duplicated(basenames)])
  if (length(dupes) > 0L) {
    cli::cli_abort(
      c(
        "Can't upload pin {.val {name}}.",
        "x" = "Duplicate file names in the upload set: {.path {dupes}}."
      ),
      class = "pinsExtras_invalid_upload_set",
      call = call
    )
  }
  meta_files <-
    if ("file" %in% names(metadata)) metadata$file else character(0)
  if (!setequal(meta_files, basenames)) {
    cli::cli_abort(
      c(
        "Can't upload pin {.val {name}}.",
        "x" = paste0(
          "Metadata lists {.path {meta_files}} but the upload set",
          " contains {.path {basenames}}."
        )
      ),
      class = "pinsExtras_invalid_upload_set",
      call = call
    )
  }
  invisible(TRUE)
}

sf_abort_pin_version_missing <- function(version, call = rlang::caller_env()) {
  cli::cli_abort("Can't find version {.val {version}}", call = call)
}

sf_local_meta <- function(x, name, dir, url = NULL, version = NULL, ...) {
  x$name <- name
  x$local <- list(
    dir = dir,
    url = url,
    version = version,
    ...
  )
  structure(x, class = "pins_meta")
}

sf_read_meta <- function(path, call = rlang::caller_env()) {
  file <- fs::path(path, "data.txt")
  if (!fs::file_exists(file)) {
    cli::cli_abort(
      c(
        "Can't read pin metadata.",
        "x" = "{.path data.txt} is missing from {.path {path}}."
      ),
      class = "pinsExtras_download_failed",
      call = call
    )
  }
  # An unparseable file is an error, not a crash to be passed through.
  yaml <- tryCatch(
    yaml::read_yaml(file, eval.expr = FALSE),
    error = function(c) cli::cli_abort(
      c(
        "Can't read pin metadata.",
        "x" = "{.path data.txt} in {.path {path}} could not be parsed."
      ),
      parent = c,
      class = "pinsExtras_download_failed",
      call = call
    )
  )
  if (is.null(yaml$api_version)) {
    yaml$api_version <- 0L
    yaml$file <- yaml$path %||% yaml$file
  } else if (yaml$api_version == 1) {
    yaml$file_size <- fs::as_fs_bytes(yaml$file_size)
    yaml$created <- sf_parse_8601_compact(yaml$created)
    yaml$user <- yaml$user %||% list()
  }
  yaml
}

sf_cache_touch <- function(board, meta) {
  meta
}

sf_version_name <- function(metadata) {
  paste0(metadata$created, "-", substr(metadata$pin_hash, 1, 5))
}

sf_version_from_path <- function(x) {
  if (!is.character(x)) {
    cli::cli_abort("`version` must be a character vector")
  }
  out <- tibble::tibble(
    version = x,
    created = .POSIXct(NA_real_, tz = ""),
    hash = NA_character_
  )
  pieces <- strsplit(x, "-")
  n_ok <- lengths(pieces) == 2
  out$created[n_ok] <- sf_parse_8601_compact(purrr::map_chr(pieces[n_ok], 1))
  out$hash[n_ok] <- purrr::map_chr(pieces[n_ok], 2)
  out
}

sf_parse_8601_compact <- function(x) {
  y <- as.POSIXct(strptime(x, "%Y%m%dT%H%M%S", tz = "UTC"))
  attr(y, "tzone") <- ""
  y
}

#' Find Snowflake ODBC Driver Name
#'
#' Auto-detects the Snowflake ODBC driver name on the current system.
#' Can be overridden via the `PINS_SF_DRIVER` environment variable.
#'
#' @return Character string with driver name
#' @keywords internal
sf_odbc_driver <- function() {
 # Allow explicit override
  explicit <- Sys.getenv("PINS_SF_DRIVER", unset = NA)
 if (!is.na(explicit) && nchar(explicit) > 0) {
    return(explicit)
  }

  # Query available ODBC drivers
  if (!requireNamespace("odbc", quietly = TRUE)) {
    cli::cli_abort("Package {.pkg odbc} is required but not installed.")
  }

  drivers <- tryCatch(
    odbc::odbcListDrivers()$name,
    error = function(e) character(0)
  )

  # Look for Snowflake driver (common names)
  snowflake_patterns <- c(
    "SnowflakeDSIIDriver",
    "Snowflake"
  )

  for (pattern in snowflake_patterns) {
    match <- drivers[grepl(pattern, drivers, ignore.case = TRUE)]
    if (length(match) > 0) {
      return(match[[1]])
    }
  }

  # Default based on OS if no driver found (will likely error later)
  if (.Platform$OS.type == "windows") {
    "SnowflakeDSIIDriver"
  } else {
    "Snowflake"
  }
}

# %||% is imported from rlang in pinsExtras-package.R

#' Check Snowflake Connection Health
#'
#' Verifies that the DBI connection is valid and provides helpful error messages
#' for reconnection if the connection has been closed or is invalid.
#'
#' @param board A pins board object with a `conn` element
#' @param call Caller environment for error reporting
#'
#' @return Invisible NULL if connection is valid; otherwise throws an error
#' @keywords internal
sf_check_connection <- function(board, call = rlang::caller_env()) {
  # Check if connection object exists
  if (is.null(board$conn)) {
    cli::cli_abort(
      c(
        "Board has no database connection.",
        "i" = "The board object may have been created incorrectly."
      ),
      call = call
    )
  }

  # Check if connection is valid using DBI
  if (!DBI::dbIsValid(board$conn)) {
    msg <- c(
      "Database connection is no longer valid.",
      "i" = "The connection may have been closed or timed out.",
      ">" = "To reconnect, create a new board with a fresh connection:"
    )

    # Add reconnection guidance if connect_args are available
    if (!is.null(board$connect_args)) {
      msg <- c(
        msg,
        " " = "  conn <- DBI::dbConnect(odbc::odbc(), ...)",
        " " = "  board <- board_sf_stage(conn, stage = \"{board$stage}\", path = \"{board$path}\", connect_args = ...)"
      )
    } else {
      msg <- c(
        msg,
        " " = "  conn <- DBI::dbConnect(odbc::odbc(), ...)",
        " " = "  board <- board_sf_stage(conn, stage = \"{board$stage}\", path = \"{board$path}\")"
      )
    }

    cli::cli_abort(msg, call = call)
  }

  invisible(NULL)
}

# Escape and quote text for a Snowflake string literal
#
# Snowflake treats backslash as an escape character inside string literals,
# so escape backslashes before single quotes to avoid double-escaping.
sf_quote_sql_literal <- function(x) {
  x <- gsub("\\", "\\\\", x, fixed = TRUE)
  x <- gsub("'", "\\'", x, fixed = TRUE)
  paste0("'", x, "'")
}

# Build a quoted stage location for SQL
#
# Single construction point for a stage location: it is just the shared
# literal quoting applied to the stage path.
sf_quote_stage_path <- function(path) {
  sf_quote_sql_literal(path)
}

# Build a quoted local file URI for SQL
#
# Single construction point for a local file URI: prefix the path with
# "file://" and then quote it like any other stage location.
sf_quote_file_uri <- function(path) {
  sf_quote_sql_literal(paste0("file://", path))
}

# Escape regex metacharacters so a string matches literally
#
# The one place in the package that builds a regular expression. Escape
# exactly the 14 characters Java treats as special, because that is the
# engine Snowflake uses for REMOVE ... PATTERN; matching R and Java here is
# what lets the same pattern match literally in both.
sf_escape_regex <- function(x) {
  metachars <- c(
    "\\", "^", "$", ".", "|", "?", "*", "+",
    "(", ")", "[", "]", "{", "}"
  )
  out <- vapply(x, function(s) {
    if (nchar(s) == 0) {
      return("")
    }
    chars <- strsplit(s, "", fixed = TRUE)[[1]]
    escaped <- vapply(chars, function(ch) {
      if (ch %in% metachars) {
        paste0("\\", ch)
      } else {
        ch
      }
    }, character(1))
    paste(escaped, collapse = "")
  }, character(1))
  # vapply(character(0)) yields a names attribute of character(0), not NULL
  structure(out, names = NULL)
}

# Build a REMOVE ... PATTERN expression for Snowflake. Anchor the escaped
# board-relative path at both ends with an optional leading-path group so it
# matches the full staged path (with or without a named-stage prefix) and
# deletes exactly one file.
sf_remove_pattern <- function(dir, file) {
  tail <- if (dir == "") file else paste0(dir, "/", file)
  paste0("^(.*/)?", sf_escape_regex(tail), "$")
}

# Strip the board path from a listing so names are board-relative.
#
# sf_stage_list() returns stage-root-relative names (with the board's path
# in front); every index rule below is written against board-relative names.
# Literal string operations only -- prefix is a user board path, never a
# regex.
sf_board_relative <- function(listing, prefix = "") {
  if (prefix == "") {
    return(listing)
  }
  drop <- paste0(prefix, "/")
  keep <- listing$name == prefix | startsWith(listing$name, drop)
  out <- listing[keep, , drop = FALSE]
  out$name <- substr(out$name, nchar(drop) + 1L, nchar(out$name))
  out
}

# Build the published-pin index from an already-fetched listing.
#
# A version counts as published only when its data.txt is present, so the
# three-segment rule below (<pin>/<version>/data.txt) is the proof of
# publication. Returns exactly the name and version columns.
sf_published_index <- function(listing, prefix = "") {
  boarded <- sf_board_relative(listing, prefix)
  names <- boarded$name

  pieces <- strsplit(names, "/", fixed = TRUE)
  n_seg <- lengths(pieces)
  third <- vapply(
    pieces,
    function(p) if (length(p) == 3L) p[[3L]] else NA_character_,
    character(1)
  )
  keep <- n_seg == 3L & third == "data.txt"

  kept_names <- vapply(
    pieces,
    function(p) if (length(p) == 3L) p[[1L]] else NA_character_,
    character(1)
  )[keep]
  kept_versions <- vapply(
    pieces,
    function(p) if (length(p) == 3L) p[[2L]] else NA_character_,
    character(1)
  )[keep]

  # A row is published only when it parsed to a real timestamp AND a hash.
  # Checking hash alone is not enough: "bogus-abc12" has two "-" pieces, so
  # sf_version_from_path() sets hash but leaves created = NA.
  parsed <- sf_version_from_path(kept_versions)
  ok <- !is.na(parsed$created) & !is.na(parsed$hash)
  kept_names <- kept_names[ok]
  parsed <- parsed[ok, , drop = FALSE]

  # Deduplicate on the (pin, version) pair, keeping the first seen.
  dup <- duplicated(
    data.frame(name = kept_names, version = parsed$version)
  )
  kept_names <- kept_names[!dup]
  parsed <- parsed[!dup, , drop = FALSE]

  index <- tibble::tibble(
    name = kept_names,
    version = parsed$version,
    created = parsed$created
  )
  ord <- order(index$name, index$created, index$version)
  tibble::tibble(
    name = index$name[ord],
    version = index$version[ord]
  )
}

# The pin names that have at least one published version, in ascending order.
# The index is already sorted by name, so unique() needs no re-sort.
sf_index_pins <- function(index) {
  unique(index$name)
}

# Whether a single pin has any published version.
sf_index_has_pin <- function(index, name) {
  name %in% index$name
}

# That pin's versions, in index order, with parsed created and hash.
sf_index_versions <- function(index, name) {
  versions <- index[index$name == name, , drop = FALSE]
  sf_version_from_path(versions$version)
}

# Confirm a pin is published. Mirrors pins' "Can't find pin called ..." so
# existing callers and tests keep working.
sf_check_pin_published <- function(index, name, call = rlang::caller_env()) {
  if (!sf_index_has_pin(index, name)) {
    cli::cli_abort("Can't find pin called {.val {name}}", call = call)
  }
  invisible(TRUE)
}

# Resolve the version to use for a pin, matching pins:::check_pin_version()
# for the NULL case (it takes the last version returned by pin_versions()).
sf_resolve_version <- function(index, name, version = NULL,
                               call = rlang::caller_env()) {
  sf_check_pin_published(index, name, call = call)

  versions <- sf_index_versions(index, name)$version

  if (is.null(version)) {
    return(versions[[length(versions)]])
  }
  if (!rlang::is_string(version)) {
    cli::cli_abort("{.arg version} must be a string", call = call)
  }
  if (version %in% versions) {
    version
  } else {
    sf_abort_pin_version_missing(version, call = call)
  }
}

# Decide, purely, what a write should do to the versions already on the
# board. Mirrors pins:::version_setup()'s decision; U11 carries it out in
# an order that never deletes the old version before the new one lands.
# Pure: no SQL, no filesystem, no board, no messages.
sf_version_plan <- function(
  index,
  name,
  new_version,
  versioned = NULL,
  board_versioned = TRUE,
  call = rlang::caller_env()
) {
  published <- sf_index_versions(index, name)$version
  n <- length(published)

  # A write whose new version already exists is a no-op the caller almost
  # certainly did not intend. upstream pins:::version_setup() compares only
  # against versions$version[[1]], the first (oldest) row of the ascending
  # table; we check against every published version, a strict superset, so
  # this can never wrongly allow a duplicate. upstream's wording is kept.
  if (new_version %in% published) {
    cli::cli_abort(
      c(
        paste0(
          "The new version {.val {new_version}} is the same as",
          " the most recent version."
        ),
        "i" = paste0(
          "Did you try to create a new version with the same",
          " timestamp as the last version?"
        )
      ),
      call = call
    )
  }

  # With several versions already published and no caller override, pins
  # forces versioning on (pins:::version_setup()); otherwise the per-write
  # override, when given, wins over the board's own flag.
  effective <- versioned %||% if (n > 1L) TRUE else board_versioned

  if (n == 0L || effective) {
    return(list(
      version = new_version,
      action = "create",
      old_versions = character()
    ))
  }
  if (n == 1L && !effective) {
    return(list(
      version = new_version,
      action = "replace",
      old_versions = published
    ))
  }

  # n > 1L && !effective: an existing versioned pin cannot be rewritten
  # without versions. Wording and class are upstream's, verbatim; note the
  # lines carry no full stop, which is what upstream prints.
  cli::cli_abort(
    c(
      "Pin is versioned, but you have requested a write without versions",
      "i" = "To un-version a pin, you must delete it"
    ),
    class = "pins_pin_versioned",
    call = call
  )
}

# A half-finished write leaves files under the version directory with no
# data.txt; re-writing into that directory would mix two attempts' files.
# We therefore look at the raw pin-scoped listing, not the index, which
# hides payload-only directories.
sf_check_version_collision <- function(listing, name, version, prefix = "",
                                       call = rlang::caller_env()) {
  relative <- sf_board_relative(listing, prefix)
  target <- paste0(name, "/", version)
  collide <- relative$name == target |
    startsWith(relative$name, paste0(target, "/"))
  if (any(collide)) {
    cli::cli_abort(
      c(
        "Version {.val {version}} of pin {.val {name}} already exists.",
        "x" = "No upload was attempted.",
        "i" = paste0(
          "Remove it with {.code pin_version_delete()} before",
          " writing again."
        )
      ),
      class = "pinsExtras_version_collision",
      call = call
    )
  }
  invisible(TRUE)
}

# The single informational helper, mirroring pins:::pins_inform():
# progress output the user can switch off with options(pins.quiet = TRUE).
#
#   ...: the cli message template, interpolated in .envir.
#   .envir: the caller's frame, threaded through so cli can find the
#           caller's locals; sf_inform() is a wrapper by design, so the
#           default (parent.frame() evaluated inside cli_inform()) would
#           look in sf_inform()'s own frame and fail.
sf_inform <- function(..., .envir = parent.frame()) {
  if (isTRUE(getOption("pins.quiet", FALSE))) {
    return(invisible())
  }
  cli::cli_inform(..., .envir = .envir)
}
