# Write-path validation. sf_check_pin_name() and sf_check_upload_set()
# check local inputs before a single PUT moves a byte, so neither issues
# SQL and there is nothing to mock.
#
# Both functions are matrices over one input, so each is driven by one
# table: a row per literal, named so a failure says which cell broke.

# ---- sf_check_pin_name -------------------------------------------------

test_that("sf_check_pin_name accepts every ordinary name shape", {
  accepted <- c(
    "mtcars", "my-pin", "my_pin", "my.pin", "2024data",
    "valid-name", "valid_name", "valid.name"
  )
  for (name in accepted) {
    expect_silent(ok <- pinsExtras:::sf_check_pin_name(name))
    expect_identical(ok, TRUE, info = name)
  }
})

test_that("sf_check_pin_name rejects every unsafe or reserved name", {
  cases <- list(
    list(name = "empty",            value = "",            msg = "must not be empty"),
    list(name = "reserved marker",  value = "data.txt",    msg = "Can't pin file called"),
    # path_file() strips the directory first, so a directory-qualified
    # data.txt is reported as the reserved name, not as a separator.
    list(name = "a/data.txt",       value = "a/data.txt",  msg = "Can't pin file called"),
    list(name = "slash",            value = "a/b",         msg = "Invalid pin name"),
    list(name = "backslash",        value = "a\\b",        msg = "Invalid pin name"),
    list(name = "leading dotdot",   value = "../escape",   msg = "Invalid pin name"),
    list(name = "bare dot",         value = ".",           msg = "Invalid pin name"),
    list(name = "asterisk",         value = "pin*",        msg = "Invalid pin name"),
    list(name = "question mark",    value = "pin?",        msg = "Invalid pin name"),
    list(name = "non-string scalar", value = 123,          msg = "must be a string"),
    list(name = "non-string vector", value = c("a", "b"),  msg = "must be a string")
  )
  for (case in cases) {
    expect_error(
      pinsExtras:::sf_check_pin_name(case$value),
      regexp = case$msg,
      fixed = TRUE,
      info = case$name
    )
  }
})

# ---- sf_check_upload_set -----------------------------------------------

# Build an upload set of real files in one temporary directory. Returns the
# full paths, in the order given.
sf_upload_files <- function(..., envir = parent.frame()) {
  names <- unlist(list(...), use.names = FALSE)
  d <- withr::local_tempdir(.local_envir = envir)
  paths <- fs::path(d, names)
  for (p in paths) {
    writeLines("x", p)
  }
  as.character(paths)
}

test_that("sf_check_upload_set accepts a set that matches its metadata", {
  cases <- list(
    list(
      name = "matching set",
      files = c("a.rds", "b.csv"),
      meta_files = c("a.rds", "b.csv")
    ),
    # setequal, not identical: pins does not promise an order, so a
    # refactor to identical() would break real multi-file writes.
    list(
      name = "metadata in a different order",
      files = c("a.rds", "b.csv"),
      meta_files = c("b.csv", "a.rds")
    ),
    # A shared stem with two types is a shape pins actually produces, so
    # nothing may deduplicate on the stem.
    list(
      name = "same stem, two types",
      files = c("a.rds", "a.csv"),
      meta_files = c("a.rds", "a.csv")
    )
  )
  for (case in cases) {
    paths <- sf_upload_files(case$files)
    expect_invisible(
      pinsExtras:::sf_check_upload_set(
        "cars", paths, list(file = case$meta_files)
      ),
      label = case$name
    )
  }
})

test_that("sf_check_upload_set rejects every invalid upload set", {
  d <- withr::local_tempdir()
  present <- fs::path(d, "a.rds")
  writeLines("x", present)
  other <- fs::path(d, "b.csv")
  writeLines("x", other)

  one <- fs::path(withr::local_tempdir(), "one")
  two <- fs::path(withr::local_tempdir(), "two")
  fs::dir_create(one)
  fs::dir_create(two)
  dupe1 <- fs::path(one, "a.rds")
  dupe2 <- fs::path(two, "a.rds")
  writeLines("x", dupe1)
  writeLines("x", dupe2)

  dtxt <- fs::path(d, "data.txt")
  writeLines("x", dtxt)

  cases <- list(
    list(
      name = "empty set",
      paths = character(0), meta_files = NULL,
      says = "The upload set is empty."
    ),
    list(
      name = "one missing file",
      paths = c(present, fs::path(d, "gone.rds")), meta_files = "a.rds",
      says = "gone.rds"
    ),
    list(
      name = "two missing files",
      paths = c(present, fs::path(d, "gone1.rds"), fs::path(d, "gone2.rds")),
      meta_files = "a.rds",
      says = c("gone1.rds", "gone2.rds")
    ),
    list(
      name = "data.txt payload",
      paths = dtxt, meta_files = "data.txt",
      says = "A pinned file cannot be named"
    ),
    list(
      name = "duplicate basenames",
      paths = c(dupe1, dupe2), meta_files = "a.rds",
      says = "a.rds",
      # The message reports basenames, never the local directories they
      # came from.
      never = c("/one/", "/two/")
    ),
    list(
      name = "metadata disagrees with the set",
      paths = c(present, other), meta_files = "a.rds",
      says = "Metadata lists"
    ),
    # names(metadata) still contains "file", so meta_files is NULL and
    # setequal fails rather than the field being treated as absent.
    list(
      name = "NULL metadata$file",
      paths = c(present, other), meta_files = NULL,
      says = "Metadata lists"
    )
  )

  for (case in cases) {
    cond <- expect_error(
      pinsExtras:::sf_check_upload_set(
        "cars", case$paths, list(file = case$meta_files)
      ),
      class = "pinsExtras_invalid_upload_set",
      info = case$name
    )
    msg <- cli::ansi_strip(conditionMessage(cond))
    for (fragment in case$says) {
      expect_true(
        grepl(fragment, msg, fixed = TRUE),
        info = paste(case$name, "says", fragment)
      )
    }
    for (fragment in case$never %||% character()) {
      expect_false(
        grepl(fragment, msg, fixed = TRUE),
        info = paste(case$name, "never says", fragment)
      )
    }
  }
})

test_that("sf_check_upload_set rejects a forbidden basename", {
  # The `bad` vector in sf_check_upload_set() is
  #   "" | "." | contains "/" | "\\" | "*" | "?" | ".."
  # Two of those seven can never be produced from a path that exists:
  # fs::path_file() strips any directory, so a basename never contains a
  # "/", and fs::file_exists() is FALSE for a path containing a backslash,
  # so such a path aborts as missing before this branch. The five that are
  # reachable are covered here, each by a path that really exists.
  d <- withr::local_tempdir()
  starred <- fs::path(d, "a*")
  queried <- fs::path(d, "a?")
  dotted <- fs::path(d, "a..b")
  writeLines("x", starred)
  writeLines("x", queried)
  writeLines("x", dotted)

  cases <- list(
    # fs::path_file("/") is "" and the stage root exists.
    list(name = "empty basename", path = "/"),
    list(name = "bare dot",       path = "."),
    list(name = "asterisk",       path = starred),
    list(name = "question mark",  path = queried),
    list(name = "dotdot inside",  path = dotted)
  )

  for (case in cases) {
    cond <- expect_error(
      pinsExtras:::sf_check_upload_set(
        "cars", as.character(case$path), list(file = "a.rds")
      ),
      class = "pinsExtras_invalid_upload_set",
      info = case$name
    )
    msg <- cli::ansi_strip(conditionMessage(cond))
    expect_true(
      grepl("These file names are not allowed", msg, fixed = TRUE),
      info = case$name
    )
  }
})
