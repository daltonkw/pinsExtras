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

# ---- sf_cleanup_old_versions --------------------------------------------
#
# An unversioned write publishes the new version and only then removes the
# old one. This function reports the versions whose removal could not be
# confirmed; it never aborts and never warns, whatever the transport does.

test_that("an empty request issues no commands at all", {
  board <- sf_mock_board()
  name <- "cars"
  rec <- sf_mock_transport(
    list = function(sql) stop("must not be reached")
  )
  testthat::local_mocked_bindings(
    sf_stage_cmd = rec$responder,
    .package = "pinsExtras"
  )

  out <- pinsExtras:::sf_cleanup_old_versions(board, "cars", character())

  expect_identical(out, character())
  expect_length(rec$calls, 0L)
})

test_that("one clean version reports nothing and issues four commands", {
  board <- sf_mock_board()
  name <- "cars"
  v <- "20240101T000001Z-aaa"
  rec <- sf_mock_transport()
  testthat::local_mocked_bindings(
    sf_stage_cmd = rec$responder,
    .package = "pinsExtras"
  )

  out <- pinsExtras:::sf_cleanup_old_versions(board, "cars", v)

  expect_identical(out, character())
  expect_length(rec$calls, 4L)
  expect_length(grep("^LIST ", rec$calls), 2L)
  expect_identical(
    rec$calls[[1]],
    paste0(
      "REMOVE '@~/cars/", v, "/' PATTERN = '^(.*/)?data\\\\.txt$'"
    )
  )
})

test_that("two clean versions report nothing and issue seven commands in order", {
  board <- sf_mock_board()
  name <- "cars"
  v1 <- "20240101T000001Z-aaa"
  v2 <- "20240101T000002Z-bbb"
  rec <- sf_mock_transport()
  testthat::local_mocked_bindings(
    sf_stage_cmd = rec$responder,
    .package = "pinsExtras"
  )

  out <- pinsExtras:::sf_cleanup_old_versions(board, "cars", c(v1, v2))

  expect_identical(out, character())
  expect_length(rec$calls, 7L)
  expect_length(grep("^LIST ", rec$calls), 3L)
  expect_identical(
    rec$calls[[1]],
    paste0(
      "REMOVE '@~/cars/", v1, "/' PATTERN = '^(.*/)?data\\\\.txt$'"
    )
  )
  expect_identical(
    rec$calls[[3]],
    paste0("REMOVE '@~/cars/", v1, "/'")
  )
  expect_identical(
    rec$calls[[4]],
    paste0(
      "REMOVE '@~/cars/", v2, "/' PATTERN = '^(.*/)?data\\\\.txt$'"
    )
  )
  expect_identical(rec$calls[[7]], "LIST '@~/cars/'")
})

test_that("data.txt REMOVE raising on V1 stops the loop and reports both", {
  board <- sf_mock_board()
  name <- "cars"
  v1 <- "20240101T000001Z-aaa"
  v2 <- "20240101T000002Z-bbb"
  rec <- sf_mock_transport(
    remove = function(sql) {
      if (grepl("PATTERN", sql, fixed = TRUE)) {
        stop("403 denied")
      }
      data.frame(
        name = character(), result = character(),
        stringsAsFactors = FALSE
      )
    },
    list = function(sql) {
      if (endsWith(sql, paste0(name, "/'"))) {
        sf_fixture_listing(
          paste0("cars/", v1, "/cars.rds"),
          paste0("cars/", v2, "/cars.rds")
        )
      } else {
        sf_fixture_listing()
      }
    }
  )
  testthat::local_mocked_bindings(
    sf_stage_cmd = rec$responder,
    .package = "pinsExtras"
  )

  out <- pinsExtras:::sf_cleanup_old_versions(board, "cars", c(v1, v2))

  expect_identical(out, c(v1, v2))
  # the failing REMOVE and the final listing only; V2 is never attempted
  expect_length(rec$calls, 2L)
  expect_length(grep("^REMOVE ", rec$calls), 1L)
})

test_that("a confirming LIST that still shows data.txt stops V1 with no deletes", {
  board <- sf_mock_board()
  name <- "cars"
  v1 <- "20240101T000001Z-aaa"
  v2 <- "20240101T000002Z-bbb"
  rec <- sf_mock_transport(
    list = function(sql) {
      if (endsWith(sql, paste0(name, "/'"))) {
        sf_fixture_listing(
          paste0("cars/", v1, "/cars.rds"),
          paste0("cars/", v2, "/cars.rds")
        )
      } else {
        sf_fixture_listing(paste0("cars/", v1, "/data.txt"))
      }
    }
  )
  testthat::local_mocked_bindings(
    sf_stage_cmd = rec$responder,
    .package = "pinsExtras"
  )

  out <- pinsExtras:::sf_cleanup_old_versions(board, "cars", c(v1, v2))

  expect_identical(out, c(v1, v2))
  # REMOVE, confirming LIST, final LIST: zero directory deletes
  expect_length(rec$calls, 3L)
  expect_length(grep("REMOVE ", rec$calls), 1L)
  expect_length(grep("^LIST ", rec$calls), 2L)
})

test_that("a final listing that raises reports every version after clean loop", {
  board <- sf_mock_board()
  name <- "cars"
  v1 <- "20240101T000001Z-aaa"
  v2 <- "20240101T000002Z-bbb"
  rec <- sf_mock_transport(
    list = function(sql) {
      if (endsWith(sql, paste0(name, "/'"))) {
        stop("dead connection")
      }
      sf_fixture_listing()
    }
  )
  testthat::local_mocked_bindings(
    sf_stage_cmd = rec$responder,
    .package = "pinsExtras"
  )

  out <- pinsExtras:::sf_cleanup_old_versions(board, "cars", c(v1, v2))

  expect_identical(out, c(v1, v2))
  expect_length(rec$calls, 7L)
})

test_that("a final listing that raises after one clean version returns that version", {
  board <- sf_mock_board()
  name <- "cars"
  v1 <- "20240101T000001Z-aaa"
  rec <- sf_mock_transport(
    list = function(sql) {
      if (endsWith(sql, paste0(name, "/'"))) {
        stop("dead connection")
      }
      sf_fixture_listing()
    }
  )
  testthat::local_mocked_bindings(
    sf_stage_cmd = rec$responder,
    .package = "pinsExtras"
  )

  out <- pinsExtras:::sf_cleanup_old_versions(board, "cars", v1)

  expect_identical(out, v1)
  expect_length(rec$calls, 4L)
})

test_that("the final listing decides truth: only a still-present version is reported", {
  board <- sf_mock_board()
  name <- "cars"
  v1 <- "20240101T000001Z-aaa"
  v2 <- "20240101T000002Z-bbb"
  rec <- sf_mock_transport(
    list = function(sql) {
      if (endsWith(sql, paste0(name, "/'"))) {
        sf_fixture_listing(paste0("cars/", v2, "/cars.rds"))
      } else {
        sf_fixture_listing()
      }
    }
  )
  testthat::local_mocked_bindings(
    sf_stage_cmd = rec$responder,
    .package = "pinsExtras"
  )

  out <- pinsExtras:::sf_cleanup_old_versions(board, "cars", c(v1, v2))

  # V1 removed cleanly, so only V2 remains
  expect_identical(out, v2)
  expect_length(rec$calls, 7L)
})

test_that("the final listing reports a version not asked for as nothing", {
  board <- sf_mock_board()
  name <- "cars"
  v1 <- "20240101T000001Z-aaa"
  v2 <- "20240101T000002Z-bbb"
  v3 <- "20240101T000003Z-ccc"
  rec <- sf_mock_transport(
    list = function(sql) {
      if (endsWith(sql, paste0(name, "/'"))) {
        sf_fixture_listing(paste0("cars/", v3, "/cars.rds"))
      } else {
        sf_fixture_listing()
      }
    }
  )
  testthat::local_mocked_bindings(
    sf_stage_cmd = rec$responder,
    .package = "pinsExtras"
  )

  out <- pinsExtras:::sf_cleanup_old_versions(board, "cars", c(v1, v2))

  # only the versions asked about are ever reported
  expect_identical(out, character())
  expect_length(rec$calls, 7L)
})

test_that("all REMOVEs reported success but files remain are reported", {
  board <- sf_mock_board()
  name <- "cars"
  v1 <- "20240101T000001Z-aaa"
  v2 <- "20240101T000002Z-bbb"
  rec <- sf_mock_transport(
    list = function(sql) {
      if (endsWith(sql, paste0(name, "/'"))) {
        sf_fixture_listing(
          paste0("cars/", v1, "/cars.rds"),
          paste0("cars/", v2, "/cars.rds")
        )
      } else {
        sf_fixture_listing()
      }
    }
  )
  testthat::local_mocked_bindings(
    sf_stage_cmd = rec$responder,
    .package = "pinsExtras"
  )

  out <- pinsExtras:::sf_cleanup_old_versions(board, "cars", c(v1, v2))

  # the loop removed everything it was told, but the listing says both remain
  expect_identical(out, c(v1, v2))
  expect_length(rec$calls, 7L)
})

test_that("the board path is honoured when deciding what remains", {
  board <- sf_mock_board(path = "team-data")
  name <- "cars"
  v1 <- "20240101T000001Z-aaa"
  rec <- sf_mock_transport(
    list = function(sql) {
      if (endsWith(sql, paste0(name, "/'"))) {
        sf_fixture_listing(paste0("cars/", v1, "/cars.rds"), board = board)
      } else {
        sf_fixture_listing()
      }
    }
  )
  testthat::local_mocked_bindings(
    sf_stage_cmd = rec$responder,
    .package = "pinsExtras"
  )

  out <- pinsExtras:::sf_cleanup_old_versions(board, "cars", v1)

  # without sf_board_relative() the "team-data/" prefix would never match
  expect_identical(out, v1)
  expect_length(rec$calls, 4L)
})

test_that("loop stops early and the final listing raises reports every version", {
  board <- sf_mock_board()
  name <- "cars"
  v1 <- "20240101T000001Z-aaa"
  v2 <- "20240101T000002Z-bbb"
  rec <- sf_mock_transport(
    remove = function(sql) {
      if (grepl("PATTERN", sql, fixed = TRUE)) {
        stop("403 denied")
      }
      data.frame(
        name = character(), result = character(),
        stringsAsFactors = FALSE
      )
    },
    list = function(sql) {
      if (endsWith(sql, paste0(name, "/'"))) {
        stop("dead connection")
      }
      sf_fixture_listing()
    }
  )
  testthat::local_mocked_bindings(
    sf_stage_cmd = rec$responder,
    .package = "pinsExtras"
  )

  out <- pinsExtras:::sf_cleanup_old_versions(board, "cars", c(v1, v2))

  expect_identical(out, c(v1, v2))
  # the failing REMOVE, then the final listing; V2 is never attempted
  expect_length(rec$calls, 2L)
})

test_that("expect_no_error and expect_no_warning around a raising final listing", {
  board <- sf_mock_board()
  name <- "cars"
  v1 <- "20240101T000001Z-aaa"
  v2 <- "20240101T000002Z-bbb"
  rec <- sf_mock_transport(
    list = function(sql) {
      if (endsWith(sql, paste0(name, "/'"))) {
        stop("dead connection")
      }
      sf_fixture_listing()
    }
  )
  testthat::local_mocked_bindings(
    sf_stage_cmd = rec$responder,
    .package = "pinsExtras"
  )

  result <- expect_no_error(
    expect_no_warning(
      pinsExtras:::sf_cleanup_old_versions(board, "cars", c(v1, v2))
    )
  )
  expect_identical(result, c(v1, v2))
})

test_that("expect_no_error and expect_no_warning around a failing data.txt REMOVE", {
  board <- sf_mock_board()
  name <- "cars"
  v1 <- "20240101T000001Z-aaa"
  v2 <- "20240101T000002Z-bbb"
  rec <- sf_mock_transport(
    remove = function(sql) {
      if (grepl("PATTERN", sql, fixed = TRUE)) {
        stop("403 denied")
      }
      data.frame(
        name = character(), result = character(),
        stringsAsFactors = FALSE
      )
    },
    list = function(sql) sf_fixture_listing(
      paste0("cars/", v1, "/cars.rds"),
      paste0("cars/", v2, "/cars.rds")
    ),
  )
  testthat::local_mocked_bindings(
    sf_stage_cmd = rec$responder,
    .package = "pinsExtras"
  )

  result <- expect_no_error(
    expect_no_warning(
      pinsExtras:::sf_cleanup_old_versions(board, "cars", c(v1, v2))
    )
  )
  expect_identical(result, c(v1, v2))
})

test_that("every failure case returns a plain character vector, never NULL", {
  board <- sf_mock_board()
  name <- "cars"
  v1 <- "20240101T000001Z-aaa"
  v2 <- "20240101T000002Z-bbb"
  rec <- sf_mock_transport(
    remove = function(sql) {
      if (grepl("PATTERN", sql, fixed = TRUE)) {
        stop("403 denied")
      }
      data.frame(
        name = character(), result = character(),
        stringsAsFactors = FALSE
      )
    },
    list = function(sql) {
      if (endsWith(sql, paste0(name, "/'"))) {
        stop("dead connection")
      }
      sf_fixture_listing()
    }
  )
  testthat::local_mocked_bindings(
    sf_stage_cmd = rec$responder,
    .package = "pinsExtras"
  )

  out <- pinsExtras:::sf_cleanup_old_versions(board, "cars", c(v1, v2))

  expect_type(out, "character")
  expect_length(out, 2L)
})
