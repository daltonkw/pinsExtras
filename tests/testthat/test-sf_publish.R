# Collision detection against the raw pin-scoped listing, not the index.
# The index hides payload-only directories, but a half-finished write must
# be seen. Pure: takes a listing, no transport is mocked.

# Every edge case targets the same pin/version; the version string is
# written in each test to keep the file free of top-level code.

test_that("sf_check_version_collision fires on a payload-only version directory", {
  V <- "20240101T000002Z-bbb"
  listing <- sf_fixture_listing(paste0("cars/", V, "/cars.rds"))
  expect_error(
    pinsExtras:::sf_check_version_collision(
      listing, "cars", V, prefix = ""
    ),
    class = "pinsExtras_version_collision"
  )
})

test_that("sf_check_version_collision fires when data.txt is already present", {
  V <- "20240101T000002Z-bbb"
  listing <- sf_fixture_listing(paste0("cars/", V, "/data.txt"))
  expect_error(
    pinsExtras:::sf_check_version_collision(
      listing, "cars", V, prefix = ""
    ),
    class = "pinsExtras_version_collision"
  )
})

test_that("sf_check_version_collision ignores a sibling pin with a shared prefix", {
  V <- "20240101T000002Z-bbb"
  listing <- sf_fixture_listing(paste0("cars_extra/", V, "/data.txt"))
  expect_invisible(
    pinsExtras:::sf_check_version_collision(
      listing, "cars", V, prefix = ""
    )
  )
})

test_that("sf_check_version_collision ignores a different version of the same pin", {
  V <- "20240101T000002Z-bbb"
  listing <- sf_fixture_listing(
    "cars/20240101T000001Z-aaa/data.txt"
  )
  expect_invisible(
    pinsExtras:::sf_check_version_collision(
      listing, "cars", V, prefix = ""
    )
  )
})

test_that("sf_check_version_collision passes on a zero-row listing", {
  V <- "20240101T000002Z-bbb"
  listing <- sf_fixture_listing()
  expect_invisible(
    pinsExtras:::sf_check_version_collision(
      listing, "cars", V, prefix = ""
    )
  )
})

test_that("sf_check_version_collision fires under a board path with a non-empty prefix", {
  V <- "20240101T000002Z-bbb"
  listing <- sf_fixture_listing(
    paste0("team-data/cars/", V, "/cars.rds")
  )
  expect_error(
    pinsExtras:::sf_check_version_collision(
      listing, "cars", V, prefix = "team-data"
    ),
    class = "pinsExtras_version_collision"
  )
})

test_that("sf_check_version_collision misses unless the board path prefix is given", {
  V <- "20240101T000002Z-bbb"
  listing <- sf_fixture_listing(
    paste0("team-data/cars/", V, "/cars.rds")
  )
  # With prefix = "" the board path is not stripped, so the target
  # "cars/<V>" is never reached; the argument is what makes the check live.
  expect_invisible(
    pinsExtras:::sf_check_version_collision(
      listing, "cars", V, prefix = ""
    )
  )
})

test_that("sf_check_version_collision names the pin and version", {
  V <- "20240101T000002Z-bbb"
  listing <- sf_fixture_listing(paste0("cars/", V, "/cars.rds"))
  cond <- expect_error(
    pinsExtras:::sf_check_version_collision(
      listing, "cars", V, prefix = ""
    ),
    class = "pinsExtras_version_collision"
  )
  msg <- cli::ansi_strip(conditionMessage(cond))
  expect_true(grepl(V, msg, fixed = TRUE))
  expect_true(grepl("of pin", msg, fixed = TRUE))
})
