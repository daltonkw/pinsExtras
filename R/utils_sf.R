# Small utility helpers to avoid relying on pins internals

sf_manifest_pin_yaml_filename <- "_pins.yaml"

sf_end_with_slash <- function(x) {
  has_slash <- grepl("/$", x)
  x[!has_slash] <- paste0(x[!has_slash], "/")
  x
}

sf_check_pin_exists <- function(board, name, call = rlang::caller_env()) {
  if (!pin_exists(board, name)) {
    cli::cli_abort("Can't find pin called {.val {name}}", call = call)
  }
}

sf_check_pin_name <- function(name, call = rlang::caller_env()) {
  if (!rlang::is_string(name)) {
    cli::cli_abort("`name` must be a string", call = call)
  }
  if (fs::path_file(name) == "data.txt") {
    cli::cli_abort("Can't pin file called `data.txt`", call = call)
  }
}

sf_check_pin_version <- function(board, name, version, call = rlang::caller_env()) {
  versions <- pin_versions(board, name)
  if (nrow(versions) == 0) {
    cli::cli_abort("No versions available for {.val {name}}", call = call)
  }

  if (is.null(version)) {
    # Return the LAST (newest) version, matching pins behavior
    versions$version[[nrow(versions)]]
  } else if (version %in% versions$version) {
    version
  } else {
    sf_abort_pin_version_missing(version, call = call)
  }
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

sf_read_meta <- function(path) {
  path <- fs::path(path, "data.txt")
  if (!fs::file_exists(path)) {
    return(list(api_version = 1L))
  }
  yaml <- yaml::read_yaml(path, eval.expr = FALSE)
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

sf_version_setup <- function(board, name, new_version, versioned = NULL) {
  n_versions <- 0
  if (pin_exists(board, name)) {
    versions <- pin_versions(board, name)
    n_versions <- nrow(versions)
  }
  ver_flag <- versioned %||% board$versioned %||% TRUE
  if (!ver_flag && n_versions > 0) {
    sf_stage_delete(board, name)
  }
  new_version
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
