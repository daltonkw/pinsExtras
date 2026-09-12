# Pure-helper tests for the published-pin index. No board, no Snowflake.

test_that("sf_board_relative leaves a listing alone when the prefix is empty", {
  v <- sf_fixture_version()
  listing <- sf_fixture_listing(paste0("cars/", v, "/data.txt"))
  out <- pinsExtras:::sf_board_relative(listing, "")
  expect_identical(out$name, paste0("cars/", v, "/data.txt"))
})

test_that("sf_board_relative strips the board path from the names", {
  v <- sf_fixture_version()
  listing <- sf_fixture_listing(paste0("team-data/cars/", v, "/data.txt"))
  out <- pinsExtras:::sf_board_relative(listing, "team-data")
  expect_identical(out$name, paste0("cars/", v, "/data.txt"))
})

test_that("sf_board_relative drops a sibling board that shares the prefix", {
  listing <- sf_fixture_listing(
    paste0("team-data-archive/cars/", sf_fixture_version(), "/data.txt")
  )
  out <- pinsExtras:::sf_board_relative(listing, "team-data")
  expect_length(out$name, 0)
})

test_that("sf_board_relative keeps a name that equals the prefix", {
  listing <- sf_fixture_listing("team-data")
  out <- pinsExtras:::sf_board_relative(listing, "team-data")
  expect_identical(out$name, "")
})

test_that("sf_board_relative drops names outside the board", {
  listing <- sf_fixture_listing("other/x")
  out <- pinsExtras:::sf_board_relative(listing, "team-data")
  expect_length(out$name, 0)
})

test_that("sf_board_relative strips a nested board path", {
  v <- sf_fixture_version()
  listing <- sf_fixture_listing(paste0("a/b/cars/", v, "/data.txt"))
  out <- pinsExtras:::sf_board_relative(listing, "a/b")
  expect_identical(out$name, paste0("cars/", v, "/data.txt"))
})

test_that("sf_board_relative keeps a zero-row listing", {
  listing <- sf_fixture_listing()
  out <- pinsExtras:::sf_board_relative(listing, "team-data")
  expect_length(out$name, 0)
})

test_that("sf_published_index returns zero rows for an empty listing", {
  expect_identical(
    pinsExtras:::sf_published_index(sf_fixture_listing()),
    tibble::tibble(name = character(), version = character())
  )
})

test_that("sf_published_index keeps a three-segment data.txt path", {
  out <- pinsExtras:::sf_published_index(
    sf_fixture_listing(paste0("cars/", sf_fixture_version(), "/data.txt"))
  )
  expect_identical(out$name, "cars")
  expect_identical(out$version, sf_fixture_version())
})

test_that("sf_published_index drops a data.txt.bak file", {
  out <- pinsExtras:::sf_published_index(
    sf_fixture_listing(paste0("cars/", sf_fixture_version(), "/data.txt.bak"))
  )
  expect_length(out$name, 0)
})

test_that("sf_published_index drops a payload-only file", {
  out <- pinsExtras:::sf_published_index(
    sf_fixture_listing(paste0("cars/", sf_fixture_version(), "/cars.rds"))
  )
  expect_length(out$name, 0)
})

test_that("sf_published_index drops the manifest file", {
  out <- pinsExtras:::sf_published_index(
    sf_fixture_listing("_pins.yaml")
  )
  expect_length(out$name, 0)
})

test_that("sf_published_index drops a two-segment path", {
  out <- pinsExtras:::sf_published_index(
    sf_fixture_listing("cars/data.txt")
  )
  expect_length(out$name, 0)
})

test_that("sf_published_index drops a four-segment path", {
  out <- pinsExtras:::sf_published_index(
    sf_fixture_listing("a/b/c/data.txt")
  )
  expect_length(out$name, 0)
})

test_that("sf_published_index drops a version with one dash piece", {
  out <- pinsExtras:::sf_published_index(
    sf_fixture_listing("cars/v1/data.txt")
  )
  expect_length(out$name, 0)
})

test_that("sf_published_index drops an unparsed version", {
  out <- pinsExtras:::sf_published_index(
    sf_fixture_listing("cars/bogus-abc12/data.txt")
  )
  expect_length(out$name, 0)
})

test_that("sf_published_index drops a version with three dash pieces", {
  out <- pinsExtras:::sf_published_index(
    sf_fixture_listing("cars/20240101T000000Z-abc12-x/data.txt")
  )
  expect_length(out$name, 0)
})

test_that("sf_published_index deduplicates a repeated path", {
  out <- pinsExtras:::sf_published_index(
    sf_fixture_listing(
      paste0("cars/", sf_fixture_version(), "/data.txt"),
      paste0("cars/", sf_fixture_version(), "/data.txt")
    )
  )
  expect_length(out$name, 1)
  expect_identical(out$name, "cars")
})

test_that("sf_published_index keeps a version with payload and data.txt", {
  out <- pinsExtras:::sf_published_index(
    sf_fixture_listing(
      paste0("cars/", sf_fixture_version(), "/cars.rds"),
      paste0("cars/", sf_fixture_version(), "/data.txt")
    )
  )
  expect_length(out$name, 1)
  expect_identical(out$version, sf_fixture_version())
})

test_that("sf_published_index keeps the board path under a prefix", {
  v <- sf_fixture_version()
  out <- pinsExtras:::sf_published_index(
    sf_fixture_listing(paste0("team-data/cars/", v, "/data.txt")),
    "team-data"
  )
  expect_identical(out$name, "cars")
})

test_that("sf_published_index excludes a sibling board under a prefix", {
  v <- sf_fixture_version()
  out <- pinsExtras:::sf_published_index(
    sf_fixture_listing(paste0("team-data-archive/cars/", v, "/data.txt")),
    "team-data"
  )
  expect_length(out$name, 0)
})

test_that("sf_published_index drops a board-relative path under a prefix", {
  out <- pinsExtras:::sf_published_index(
    sf_fixture_listing(paste0("cars/", sf_fixture_version(), "/data.txt")),
    "team-data"
  )
  expect_length(out$name, 0)
})

test_that("sf_published_index drops a 4-seg path under an empty prefix", {
  v <- sf_fixture_version()
  out <- pinsExtras:::sf_published_index(
    sf_fixture_listing(paste0("team-data/cars/", v, "/data.txt"))
  )
  expect_length(out$name, 0)
})

test_that("sf_published_index sorts by created then version", {
  out <- pinsExtras:::sf_published_index(
    sf_fixture_listing(
      paste0("cars/20240101T000002Z-bbb/data.txt"),
      paste0("cars/20240101T000001Z-aaa/data.txt")
    )
  )
  expect_identical(
    out$version,
    c("20240101T000001Z-aaa", "20240101T000002Z-bbb")
  )
})

test_that("sf_published_index breaks equal timestamps by the version string", {
  out <- pinsExtras:::sf_published_index(
    sf_fixture_listing(
      paste0("cars/20240101T000000Z-bbb/data.txt"),
      paste0("cars/20240101T000000Z-aaa/data.txt")
    )
  )
  expect_identical(
    out$version,
    c("20240101T000000Z-aaa", "20240101T000000Z-bbb")
  )
})

test_that("sf_published_index sorts pins by name", {
  out <- pinsExtras:::sf_published_index(
    sf_fixture_listing(
      paste0("zebra/20240101T000000Z-z/data.txt"),
      paste0("alpha/20240101T000000Z-a/data.txt")
    )
  )
  expect_identical(out$name, c("alpha", "zebra"))
})

test_that("sf_index_pins returns the pin names in order", {
  idx <- pinsExtras:::sf_published_index(
    sf_fixture_listing(
      paste0("alpha/", sf_fixture_version(), "/data.txt"),
      paste0("zebra/", sf_fixture_version(), "/data.txt")
    )
  )
  expect_identical(pinsExtras:::sf_index_pins(idx), c("alpha", "zebra"))
})

test_that("sf_index_pins returns character(0) for an empty index", {
  expect_identical(
    pinsExtras:::sf_index_pins(
      pinsExtras:::sf_published_index(sf_fixture_listing())
    ),
    character(0)
  )
})

test_that("sf_index_has_pin is TRUE for a published pin", {
  idx <- pinsExtras:::sf_published_index(
    sf_fixture_listing(paste0("cars/", sf_fixture_version(), "/data.txt"))
  )
  expect_identical(pinsExtras:::sf_index_has_pin(idx, "cars"), TRUE)
})

test_that("sf_index_has_pin is FALSE for an absent pin", {
  idx <- pinsExtras:::sf_published_index(
    sf_fixture_listing(paste0("cars/", sf_fixture_version(), "/data.txt"))
  )
  expect_identical(pinsExtras:::sf_index_has_pin(idx, "cars_extra"), FALSE)
})

test_that("sf_index_has_pin is FALSE for an empty index", {
  expect_identical(
    pinsExtras:::sf_index_has_pin(
      pinsExtras:::sf_published_index(sf_fixture_listing()),
      "cars"
    ),
    FALSE
  )
})

test_that("sf_index_versions returns a pin's versions oldest first", {
  idx <- pinsExtras:::sf_published_index(
    sf_fixture_listing(
      paste0("cars/20240101T000001Z-aaa/data.txt"),
      paste0("cars/20240101T000002Z-bbb/data.txt")
    )
  )
  out <- pinsExtras:::sf_index_versions(idx, "cars")
  expect_s3_class(out, "tbl_df")
  expect_identical(
    out$version,
    c("20240101T000001Z-aaa", "20240101T000002Z-bbb")
  )
  expect_false(any(is.na(out$created)))
  expect_false(any(is.na(out$hash)))
})

test_that("sf_index_versions returns a zero-row tibble for an absent pin", {
  idx <- pinsExtras:::sf_published_index(
    sf_fixture_listing(paste0("cars/", sf_fixture_version(), "/data.txt"))
  )
  out <- pinsExtras:::sf_index_versions(idx, "nope")
  expect_equal(nrow(out), 0)
  expect_identical(names(out), c("version", "created", "hash"))
})

test_that("sf_check_pin_published succeeds for a published pin", {
  idx <- pinsExtras:::sf_published_index(
    sf_fixture_listing(paste0("cars/", sf_fixture_version(), "/data.txt"))
  )
  expect_identical(
    pinsExtras:::sf_check_pin_published(idx, "cars"),
    TRUE
  )
})

test_that("sf_check_pin_published aborts for an absent pin", {
  idx <- pinsExtras:::sf_published_index(
    sf_fixture_listing(paste0("cars/", sf_fixture_version(), "/data.txt"))
  )
  expect_error(
    pinsExtras:::sf_check_pin_published(idx, "nope"),
    "Can't find pin",
    fixed = TRUE
  )
})

test_that("sf_check_pin_published aborts for an empty index", {
  expect_error(
    pinsExtras:::sf_check_pin_published(
      pinsExtras:::sf_published_index(sf_fixture_listing()),
      "cars"
    ),
    "Can't find pin",
    fixed = TRUE
  )
})

test_that("sf_resolve_version returns the newest version", {
  idx <- pinsExtras:::sf_published_index(
    sf_fixture_listing(
      paste0("cars/20240101T000001Z-aaa/data.txt"),
      paste0("cars/20240101T000002Z-bbb/data.txt")
    )
  )
  expect_identical(
    pinsExtras:::sf_resolve_version(idx, "cars"),
    "20240101T000002Z-bbb"
  )
})

test_that("sf_resolve_version returns a specific published version", {
  idx <- pinsExtras:::sf_published_index(
    sf_fixture_listing(
      paste0("cars/20240101T000001Z-aaa/data.txt"),
      paste0("cars/20240101T000002Z-bbb/data.txt")
    )
  )
  expect_identical(
    pinsExtras:::sf_resolve_version(idx, "cars", "20240101T000001Z-aaa"),
    "20240101T000001Z-aaa"
  )
})

test_that("sf_resolve_version aborts on an unpublished version", {
  idx <- pinsExtras:::sf_published_index(
    sf_fixture_listing(paste0("cars/", sf_fixture_version(), "/data.txt"))
  )
  expect_error(
    pinsExtras:::sf_resolve_version(idx, "cars", "nope"),
    "Can't find version",
    fixed = TRUE
  )
})

test_that("sf_resolve_version aborts on an absent pin", {
  idx <- pinsExtras:::sf_published_index(
    sf_fixture_listing(paste0("cars/", sf_fixture_version(), "/data.txt"))
  )
  expect_error(
    pinsExtras:::sf_resolve_version(idx, "missing"),
    "Can't find pin",
    fixed = TRUE
  )
})

test_that("sf_resolve_version aborts when version is not a string", {
  idx <- pinsExtras:::sf_published_index(
    sf_fixture_listing(paste0("cars/", sf_fixture_version(), "/data.txt"))
  )
  expect_error(
    pinsExtras:::sf_resolve_version(idx, "cars", 123),
    "must be a string",
    fixed = TRUE
  )
})

test_that("sf_resolve_version aborts when version is a multi-element vector", {
  idx <- pinsExtras:::sf_published_index(
    sf_fixture_listing(paste0("cars/", sf_fixture_version(), "/data.txt"))
  )
  expect_error(
    pinsExtras:::sf_resolve_version(idx, "cars", c("a", "b")),
    "must be a string",
    fixed = TRUE
  )
})

test_that("sf_resolve_version picks the lexically greater version on ties", {
  idx <- pinsExtras:::sf_published_index(
    sf_fixture_listing(
      paste0("cars/20240101T000000Z-aaa/data.txt"),
      paste0("cars/20240101T000000Z-bbb/data.txt")
    )
  )
  expect_identical(
    pinsExtras:::sf_resolve_version(idx, "cars"),
    "20240101T000000Z-bbb"
  )
})
