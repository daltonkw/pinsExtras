# Pure decision on a write's action plus the progress helper. Nothing is
# executed here: U11 carries the plan out. No transport is mocked.

test_that("sf_version_plan creates when no version is published", {
  idx <- pinsExtras:::sf_published_index(
    sf_fixture_listing(
      "cars/20240101T000001Z-aaa/data.txt",
      "cars/20240101T000002Z-bbb/data.txt",
      "cars/20240101T000003Z-ccc/data.txt"
    )
  )
  plan <- pinsExtras:::sf_version_plan(
    idx,
    "cars",
    "20240101T000009Z-zzz",
    versioned = NULL,
    board_versioned = TRUE
  )
  expect_identical(plan$version, "20240101T000009Z-zzz")
  expect_identical(plan$action, "create")
  expect_identical(plan$old_versions, character())
})

test_that("sf_version_plan creates even when unversioned is asked for at n == 0", {
  idx <- pinsExtras:::sf_published_index(sf_fixture_listing())
  plan <- pinsExtras:::sf_version_plan(
    idx,
    "cars",
    "20240101T000009Z-zzz",
    versioned = FALSE,
    board_versioned = FALSE
  )
  expect_identical(plan$action, "create")
  expect_identical(plan$old_versions, character())
})

test_that("sf_version_plan creates when a single version exists on a versioned board", {
  idx <- pinsExtras:::sf_published_index(
    sf_fixture_listing("cars/20240101T000001Z-aaa/data.txt")
  )
  plan <- pinsExtras:::sf_version_plan(
    idx,
    "cars",
    "20240101T000009Z-zzz",
    versioned = NULL,
    board_versioned = TRUE
  )
  expect_identical(plan$action, "create")
  expect_identical(plan$old_versions, character())
})

test_that("sf_version_plan replaces a single version when unversioned", {
  idx <- pinsExtras:::sf_published_index(
    sf_fixture_listing("cars/20240101T000001Z-aaa/data.txt")
  )
  plan <- pinsExtras:::sf_version_plan(
    idx,
    "cars",
    "20240101T000009Z-zzz",
    versioned = FALSE,
    board_versioned = TRUE
  )
  expect_identical(plan$action, "replace")
  expect_identical(plan$old_versions, "20240101T000001Z-aaa")
})

test_that("sf_version_plan replaces a single version even on an unversioned board", {
  idx <- pinsExtras:::sf_published_index(
    sf_fixture_listing("cars/20240101T000001Z-aaa/data.txt")
  )
  plan <- pinsExtras:::sf_version_plan(
    idx,
    "cars",
    "20240101T000009Z-zzz",
    versioned = NULL,
    board_versioned = FALSE
  )
  expect_identical(plan$action, "replace")
  expect_identical(plan$old_versions, "20240101T000001Z-aaa")
})

test_that("sf_version_plan creates more-than-one version on an unversioned board", {
  idx <- pinsExtras:::sf_published_index(
    sf_fixture_listing(
      "cars/20240101T000001Z-aaa/data.txt",
      "cars/20240101T000002Z-bbb/data.txt",
      "cars/20240101T000003Z-ccc/data.txt"
    )
  )
  plan <- pinsExtras:::sf_version_plan(
    idx,
    "cars",
    "20240101T000009Z-zzz",
    versioned = NULL,
    board_versioned = FALSE
  )
  expect_identical(plan$action, "create")
  expect_identical(plan$old_versions, character())
})

test_that("sf_version_plan creates more-than-one version on a versioned board", {
  idx <- pinsExtras:::sf_published_index(
    sf_fixture_listing(
      "cars/20240101T000001Z-aaa/data.txt",
      "cars/20240101T000002Z-bbb/data.txt",
      "cars/20240101T000003Z-ccc/data.txt"
    )
  )
  plan <- pinsExtras:::sf_version_plan(
    idx,
    "cars",
    "20240101T000009Z-zzz",
    versioned = NULL,
    board_versioned = TRUE
  )
  expect_identical(plan$action, "create")
  expect_identical(plan$old_versions, character())
})

test_that("sf_version_plan aborts pins_pin_versioned when more than one version is unversionable", {
  idx <- pinsExtras:::sf_published_index(
    sf_fixture_listing(
      "cars/20240101T000001Z-aaa/data.txt",
      "cars/20240101T000002Z-bbb/data.txt",
      "cars/20240101T000003Z-ccc/data.txt"
    )
  )
  expect_error(
    pinsExtras:::sf_version_plan(
      idx,
      "cars",
      "20240101T000009Z-zzz",
      versioned = FALSE,
      board_versioned = TRUE
    ),
    class = "pins_pin_versioned"
  )
})

test_that("sf_version_plan rejects a duplicate new version when versioned is NULL", {
  idx <- pinsExtras:::sf_published_index(
    sf_fixture_listing("cars/20240101T000002Z-bbb/data.txt")
  )
  expect_error(
    pinsExtras:::sf_version_plan(
      idx,
      "cars",
      "20240101T000002Z-bbb",
      versioned = NULL
    ),
    "same as the most recent version",
    fixed = TRUE
  )
})

test_that("sf_version_plan rejects a duplicate new version when versioned is TRUE", {
  idx <- pinsExtras:::sf_published_index(
    sf_fixture_listing("cars/20240101T000002Z-bbb/data.txt")
  )
  expect_error(
    pinsExtras:::sf_version_plan(
      idx,
      "cars",
      "20240101T000002Z-bbb",
      versioned = TRUE
    ),
    "same as the most recent version",
    fixed = TRUE
  )
})

test_that("sf_version_plan rejects a duplicate new version when versioned is FALSE", {
  idx <- pinsExtras:::sf_published_index(
    sf_fixture_listing("cars/20240101T000002Z-bbb/data.txt")
  )
  expect_error(
    pinsExtras:::sf_version_plan(
      idx,
      "cars",
      "20240101T000002Z-bbb",
      versioned = FALSE
    ),
    "same as the most recent version",
    fixed = TRUE
  )
})

# ---- sf_inform ---------------------------------------------------------

test_that("sf_inform emits a progress message", {
  expect_message(
    pinsExtras:::sf_inform("Creating new version 20240101T000002Z-bbb"),
    "Creating new version"
  )
})

test_that("sf_inform interpolates a caller's local", {
  f <- function() {
    v <- "20240101T000002Z-bbb"
    pinsExtras:::sf_inform("Creating new version {.val {v}}")
  }
  expect_message(f(), "20240101T000002Z-bbb")
})

test_that("sf_inform is silent when pins.quiet is set", {
  withr::local_options(pins.quiet = TRUE)
  expect_no_message(
    pinsExtras:::sf_inform("Creating new version {.val {v}}")
  )
})

test_that("sf_inform is silent and returns invisibly under pins.quiet", {
  withr::local_options(pins.quiet = TRUE)
  expect_invisible(pinsExtras:::sf_inform("anything"))
})
