# Write-path validation. sf_check_pin_name() and sf_check_upload_set()
# check local inputs before a single PUT moves a byte, so neither issues
# SQL and there is nothing to mock.
#
# Both functions are matrices over one input, so each is driven by one
# table: a row per literal, named so a failure says which cell broke.

# ---- sf_check_pin_name -------------------------------------------------

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
