# Write-path validation. sf_check_pin_name() and sf_check_upload_set()
# check local inputs before a single PUT moves a byte, so neither issues
# SQL and there is nothing to mock.

# ---- sf_check_pin_name -------------------------------------------------

test_that("sf_check_pin_name accepts ordinary names", {
  expect_silent(pinsExtras:::sf_check_pin_name("mtcars"))
  expect_silent(pinsExtras:::sf_check_pin_name("my-pin"))
  expect_silent(pinsExtras:::sf_check_pin_name("my_pin"))
  expect_silent(pinsExtras:::sf_check_pin_name("my.pin"))
  expect_silent(pinsExtras:::sf_check_pin_name("2024data"))
})

test_that("sf_check_pin_name accepts a 120-character name", {
  name <- paste0(strrep("a", 115), strrep("1", 5))
  expect_equal(nchar(name), 120L)
  expect_silent(pinsExtras:::sf_check_pin_name(name))
})

test_that("sf_check_pin_name rejects an empty name", {
  expect_error(
    pinsExtras:::sf_check_pin_name(""),
    "must not be empty",
    fixed = TRUE
  )
})

test_that("sf_check_pin_name rejects the reserved data.txt name", {
  expect_error(
    pinsExtras:::sf_check_pin_name("data.txt"),
    "Can't pin file called",
    fixed = TRUE
  )
})

test_that("sf_check_pin_name treats a/data.txt as the reserved name", {
  expect_error(
    pinsExtras:::sf_check_pin_name("a/data.txt"),
    "Can't pin file called",
    fixed = TRUE
  )
})

test_that("sf_check_pin_name rejects a slash", {
  expect_error(
    pinsExtras:::sf_check_pin_name("a/b"),
    "Invalid pin name",
    fixed = TRUE
  )
})

test_that("sf_check_pin_name rejects a backslash", {
  expect_error(
    pinsExtras:::sf_check_pin_name("a\\b"),
    "Invalid pin name",
    fixed = TRUE
  )
})

test_that("sf_check_pin_name rejects a leading dot-dot", {
  expect_error(
    pinsExtras:::sf_check_pin_name("../escape"),
    "Invalid pin name",
    fixed = TRUE
  )
})

test_that("sf_check_pin_name rejects a bare dot", {
  expect_error(
    pinsExtras:::sf_check_pin_name("."),
    "Invalid pin name",
    fixed = TRUE
  )
})

test_that("sf_check_pin_name rejects an asterisk", {
  expect_error(
    pinsExtras:::sf_check_pin_name("pin*"),
    "Invalid pin name",
    fixed = TRUE
  )
})

test_that("sf_check_pin_name rejects a question mark", {
  expect_error(
    pinsExtras:::sf_check_pin_name("pin?"),
    "Invalid pin name",
    fixed = TRUE
  )
})

test_that("sf_check_pin_name rejects a non-string scalar", {
  expect_error(
    pinsExtras:::sf_check_pin_name(123),
    "must be a string",
    fixed = TRUE
  )
})

test_that("sf_check_pin_name rejects a non-string vector", {
  expect_error(
    pinsExtras:::sf_check_pin_name(c("a", "b")),
    "must be a string",
    fixed = TRUE
  )
})

# ---- sf_check_upload_set -----------------------------------------------

test_that("sf_check_upload_set accepts a matching upload set", {
  d <- withr::local_tempdir()
  a <- fs::path(d, "a.rds")
  b <- fs::path(d, "b.csv")
  writeLines("x", a)
  writeLines("x", b)
  expect_invisible(
    pinsExtras:::sf_check_upload_set(
      "cars", c(a, b), list(file = c("a.rds", "b.csv"))
    )
  )
})

test_that("sf_check_upload_set ignores metadata$file order", {
  d <- withr::local_tempdir()
  a <- fs::path(d, "a.rds")
  b <- fs::path(d, "b.csv")
  writeLines("x", a)
  writeLines("x", b)
  expect_invisible(
    pinsExtras:::sf_check_upload_set(
      "cars", c(a, b), list(file = c("b.csv", "a.rds"))
    )
  )
})

test_that("sf_check_upload_set allows same-stem multi-type payloads", {
  d <- withr::local_tempdir()
  a <- fs::path(d, "a.rds")
  b <- fs::path(d, "a.csv")
  writeLines("x", a)
  writeLines("x", b)
  expect_invisible(
    pinsExtras:::sf_check_upload_set(
      "cars", c(a, b), list(file = c("a.rds", "a.csv"))
    )
  )
})

test_that("sf_check_upload_set rejects an empty set", {
  expect_error(
    pinsExtras:::sf_check_upload_set(
      "cars", character(0), list(file = NULL)
    ),
    "The upload set is empty.",
    fixed = TRUE
  )
})

test_that("sf_check_upload_set names one missing file", {
  d <- withr::local_tempdir()
  a <- fs::path(d, "a.rds")
  writeLines("x", a)
  cond <- expect_error(
    pinsExtras:::sf_check_upload_set(
      "cars", c(a, fs::path(d, "gone.rds")), list(file = "a.rds")
    ),
    class = "pinsExtras_invalid_upload_set"
  )
  msg <- cli::ansi_strip(conditionMessage(cond))
  expect_true(grepl("gone.rds", msg, fixed = TRUE))
})

test_that("sf_check_upload_set names both missing files", {
  d <- withr::local_tempdir()
  a <- fs::path(d, "a.rds")
  writeLines("x", a)
  g1 <- fs::path(d, "gone1.rds")
  g2 <- fs::path(d, "gone2.rds")
  cond <- expect_error(
    pinsExtras:::sf_check_upload_set(
      "cars", c(a, g1, g2), list(file = "a.rds")
    ),
    class = "pinsExtras_invalid_upload_set"
  )
  msg <- cli::ansi_strip(conditionMessage(cond))
  expect_true(grepl("gone1.rds", msg, fixed = TRUE))
  expect_true(grepl("gone2.rds", msg, fixed = TRUE))
})

test_that("sf_check_upload_set rejects a data.txt payload", {
  d <- withr::local_tempdir()
  dtxt <- fs::path(d, "data.txt")
  writeLines("x", dtxt)
  expect_error(
    pinsExtras:::sf_check_upload_set(
      "cars", dtxt, list(file = "data.txt")
    ),
    "A pinned file cannot be named",
    fixed = TRUE
  )
})

test_that("sf_check_upload_set rejects duplicate basenames", {
  one <- fs::path(withr::local_tempdir(), "one")
  two <- fs::path(withr::local_tempdir(), "two")
  fs::dir_create(one)
  fs::dir_create(two)
  a1 <- fs::path(one, "a.rds")
  a2 <- fs::path(two, "a.rds")
  writeLines("x", a1)
  writeLines("x", a2)
  cond <- expect_error(
    pinsExtras:::sf_check_upload_set(
      "cars", c(a1, a2), list(file = "a.rds")
    ),
    class = "pinsExtras_invalid_upload_set"
  )
  msg <- cli::ansi_strip(conditionMessage(cond))
  expect_true(grepl("a.rds", msg, fixed = TRUE))
  expect_false(grepl("/one/", msg, fixed = TRUE))
  expect_false(grepl("/two/", msg, fixed = TRUE))
})

test_that("sf_check_upload_set rejects a metadata/upload-set mismatch", {
  d <- withr::local_tempdir()
  a <- fs::path(d, "a.rds")
  b <- fs::path(d, "b.csv")
  writeLines("x", a)
  writeLines("x", b)
  cond <- expect_error(
    pinsExtras:::sf_check_upload_set(
      "cars", c(a, b), list(file = "a.rds")
    ),
    class = "pinsExtras_invalid_upload_set"
  )
  msg <- cli::ansi_strip(conditionMessage(cond))
  expect_true(grepl("Metadata lists", msg, fixed = TRUE))
})

test_that("sf_check_upload_set treats NULL metadata$file as a mismatch", {
  d <- withr::local_tempdir()
  a <- fs::path(d, "a.rds")
  b <- fs::path(d, "b.csv")
  writeLines("x", a)
  writeLines("x", b)
  expect_error(
    pinsExtras:::sf_check_upload_set(
      "cars", c(a, b), list(file = NULL)
    ),
    "Metadata lists",
    fixed = TRUE
  )
})
