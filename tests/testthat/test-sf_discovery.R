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

# ---- Read-side methods now answer from the published-pin index ----------

test_that("pin_list issues one board-scoped listing and returns pin names", {
  board <- sf_mock_board(stage = "@~")
  rec <- sf_mock_transport(
    list = sf_fixture_listing(
      paste0("cars/", "20240101T000002Z-bbb", "/data.txt"), board = board
    )
  )
  testthat::local_mocked_bindings(
    sf_stage_cmd = rec$responder, .package = "pinsExtras"
  )

  out <- pins::pin_list(board)

  expect_identical(rec$calls, "LIST '@~'")
  expect_identical(out, "cars")
})

test_that("pin_list scopes the LIST to the board path on a named stage", {
  board <- sf_mock_board(path = "team-data", stage = "@mystage")
  rec <- sf_mock_transport(
    list = sf_fixture_listing(
      paste0("cars/", "20240101T000002Z-bbb", "/data.txt"), board = board
    )
  )
  testthat::local_mocked_bindings(
    sf_stage_cmd = rec$responder, .package = "pinsExtras"
  )

  pins::pin_list(board)

  expect_identical(rec$calls, "LIST '@mystage/team-data/'")
})

test_that("pin_list returns character(0) for a zero-row listing", {
  board <- sf_mock_board(stage = "@~")
  rec <- sf_mock_transport(list = sf_fixture_listing(board = board))
  testthat::local_mocked_bindings(
    sf_stage_cmd = rec$responder, .package = "pinsExtras"
  )

  out <- pins::pin_list(board)

  expect_identical(rec$calls, "LIST '@~'")
  expect_identical(out, character())
})

test_that("pin_list returns character(0) when only the manifest exists", {
  board <- sf_mock_board(path = "team-data", stage = "@mystage")
  rec <- sf_mock_transport(list = sf_fixture_listing("_pins.yaml",
    board = board))
  testthat::local_mocked_bindings(
    sf_stage_cmd = rec$responder, .package = "pinsExtras"
  )

  out <- pins::pin_list(board)

  expect_identical(out, character())
})

test_that("pin_exists issues one pin-scoped listing and reports membership", {
  board <- sf_mock_board(stage = "@~")
  rec <- sf_mock_transport(
    list = sf_fixture_listing(
      paste0("cars/", "20240101T000002Z-bbb", "/data.txt"), board = board
    )
  )
  testthat::local_mocked_bindings(
    sf_stage_cmd = rec$responder, .package = "pinsExtras"
  )

  out <- pins::pin_exists(board, "cars")

  expect_identical(rec$calls, "LIST '@~/cars/'")
  expect_true(out)
})

test_that("pin_exists scopes the LIST to the board path on a named stage", {
  board <- sf_mock_board(path = "team-data", stage = "@mystage")
  rec <- sf_mock_transport(
    list = sf_fixture_listing(
      paste0("cars/", "20240101T000002Z-bbb", "/data.txt"), board = board
    )
  )
  testthat::local_mocked_bindings(
    sf_stage_cmd = rec$responder, .package = "pinsExtras"
  )

  pins::pin_exists(board, "cars")

  expect_identical(rec$calls, "LIST '@mystage/team-data/cars/'")
})

test_that("pin_exists is false for a pin that shares only a prefix", {
  board <- sf_mock_board(path = "team-data", stage = "@mystage")
  rec <- sf_mock_transport(
    list = sf_fixture_listing(
      paste0("cars/", "20240101T000002Z-bbb", "/data.txt"), board = board
    )
  )
  testthat::local_mocked_bindings(
    sf_stage_cmd = rec$responder, .package = "pinsExtras"
  )

  cars <- pins::pin_exists(board, "cars")
  extra <- pins::pin_exists(board, "cars_extra")

  expect_true(cars)
  expect_false(extra)
})

test_that("pin_exists is false for a payload-only directory", {
  board <- sf_mock_board(stage = "@~")
  rec <- sf_mock_transport(
    list = sf_fixture_listing(
      paste0("orphan/", "20240101T000002Z-bbb", "/orphan.rds"), board = board
    )
  )
  testthat::local_mocked_bindings(
    sf_stage_cmd = rec$responder, .package = "pinsExtras"
  )

  out <- pins::pin_exists(board, "orphan")

  expect_false(out)
})

test_that("pin_versions lists versions ascending by created", {
  board <- sf_mock_board(stage = "@~")
  rec <- sf_mock_transport(
    list = sf_fixture_listing(
      paste0("cars/", "20240101T000002Z-bbb", "/data.txt"),
      paste0("cars/", "20240101T000001Z-aaa", "/data.txt"),
      board = board
    )
  )
  testthat::local_mocked_bindings(
    sf_stage_cmd = rec$responder, .package = "pinsExtras"
  )

  versions <- pins::pin_versions(board, "cars")

  expect_identical(
    versions$version,
    c("20240101T000001Z-aaa", "20240101T000002Z-bbb")
  )
})

test_that("pin_versions scopes the LIST to the board path on a named stage", {
  board <- sf_mock_board(path = "team-data", stage = "@mystage")
  rec <- sf_mock_transport(
    list = sf_fixture_listing(
      paste0("cars/", "20240101T000002Z-bbb", "/data.txt"), board = board
    )
  )
  testthat::local_mocked_bindings(
    sf_stage_cmd = rec$responder, .package = "pinsExtras"
  )

  pins::pin_versions(board, "cars")

  expect_identical(rec$calls, "LIST '@mystage/team-data/cars/'")
})

test_that("pin_versions aborts for a payload-only directory", {
  board <- sf_mock_board(stage = "@~")
  rec <- sf_mock_transport(
    list = sf_fixture_listing(
      paste0("orphan/", "20240101T000002Z-bbb", "/orphan.rds"), board = board
    )
  )
  testthat::local_mocked_bindings(
    sf_stage_cmd = rec$responder, .package = "pinsExtras"
  )

  expect_error(
    pins::pin_versions(board, "orphan"),
    "Can't find pin called",
    fixed = TRUE
  )
})

test_that("pin_meta issues one listing, one GET, and resolves the newest", {
  board <- sf_mock_board(path = "team-data", stage = "@mystage")
  v <- "20240101T000002Z-bbb"
  meta <- list(
    api_version = 1L, file = "cars.rds", file_size = 12,
    created = "20240101", pin_hash = "abc1234567", type = "rds"
  )
  rec <- sf_mock_transport(
    list = sf_fixture_listing(paste0("cars/", v, "/data.txt"), board = board),
    get = sf_mock_get_files("data.txt" = yaml::as.yaml(meta))
  )
  testthat::local_mocked_bindings(
    sf_stage_cmd = rec$responder, .package = "pinsExtras"
  )

  out <- pins::pin_meta(board, "cars")

  expect_length(grep("^LIST ", rec$calls), 1L)
  expect_length(grep("^GET ", rec$calls), 1L)
  expect_identical(out$local$version, v)
  expect_identical(out$file, "cars.rds")
})

test_that("pin_meta resolves the newest version across an out-of-order list", {
  board <- sf_mock_board(stage = "@~")
  meta <- list(
    api_version = 1L, file = "cars.txt", file_size = 1,
    created = "20240101", pin_hash = "abc1234567", type = "txt"
  )
  rec <- sf_mock_transport(
    list = sf_fixture_listing(
      paste0("cars/", "20240101T000002Z-bbb", "/data.txt"),
      paste0("cars/", "20240101T000001Z-aaa", "/data.txt"),
      board = board
    ),
    get = sf_mock_get_files("data.txt" = yaml::as.yaml(meta))
  )
  testthat::local_mocked_bindings(
    sf_stage_cmd = rec$responder, .package = "pinsExtras"
  )

  out <- pins::pin_meta(board, "cars")

  expect_identical(out$local$version, "20240101T000002Z-bbb")
})

test_that("pin_meta picks the lexically last version on equal timestamps", {
  board <- sf_mock_board(stage = "@~")
  meta <- list(
    api_version = 1L, file = "cars.txt", file_size = 1,
    created = "20240101", pin_hash = "abc1234567", type = "txt"
  )
  v <- "20240101T000000Z"
  rec <- sf_mock_transport(
    list = sf_fixture_listing(
      paste0("cars/", paste0(v, "-aaa"), "/data.txt"),
      paste0("cars/", paste0(v, "-bbb"), "/data.txt"),
      board = board
    ),
    get = sf_mock_get_files("data.txt" = yaml::as.yaml(meta))
  )
  testthat::local_mocked_bindings(
    sf_stage_cmd = rec$responder, .package = "pinsExtras"
  )

  out <- pins::pin_meta(board, "cars")

  expect_identical(out$local$version, paste0(v, "-bbb"))
})

test_that("pin_meta aborts when the requested version is unknown", {
  board <- sf_mock_board(stage = "@~")
  rec <- sf_mock_transport(
    list = sf_fixture_listing(
      paste0("cars/", "20240101T000002Z-bbb", "/data.txt"), board = board
    )
  )
  testthat::local_mocked_bindings(
    sf_stage_cmd = rec$responder, .package = "pinsExtras"
  )

  expect_error(
    pins::pin_meta(board, "cars", version = "nope"),
    "Can't find version",
    fixed = TRUE
  )
})

test_that("pin_meta aborts when the pin has no published version", {
  board <- sf_mock_board(stage = "@~")
  rec <- sf_mock_transport(
    list = sf_fixture_listing(
      paste0("cars/", "20240101T000002Z-bbb", "/data.txt"), board = board
    )
  )
  testthat::local_mocked_bindings(
    sf_stage_cmd = rec$responder, .package = "pinsExtras"
  )

  expect_error(
    pins::pin_meta(board, "missing"),
    "Can't find pin called",
    fixed = TRUE
  )
})

test_that("pin_meta propagates a malformed-YAML download failure", {
  board <- sf_mock_board(stage = "@~")
  rec <- sf_mock_transport(
    list = sf_fixture_listing(
      paste0("cars/", "20240101T000002Z-bbb", "/data.txt"), board = board
    ),
    get = sf_mock_get_files("data.txt" = "not: valid: yaml: [")
  )
  testthat::local_mocked_bindings(
    sf_stage_cmd = rec$responder, .package = "pinsExtras"
  )

  expect_error(
    pins::pin_meta(board, "cars"),
    class = "pinsExtras_download_failed"
  )
})

test_that("a listing that raises propagates unchanged instead of an abort", {
  board <- sf_mock_board(stage = "@~")
  rec <- sf_mock_transport(list = function(sql, calls) stop("boom"))
  testthat::local_mocked_bindings(
    sf_stage_cmd = rec$responder, .package = "pinsExtras"
  )

  expect_error(pins::pin_list(board), "boom")
})

test_that("pin_meta is case-insensitive when the driver upper-cases columns", {
  board <- sf_mock_board(stage = "@~")
  meta <- list(
    api_version = 1L, file = "cars.rds", file_size = 12,
    created = "20240101", pin_hash = "abc1234567", type = "rds"
  )
  rec <- sf_mock_transport(
    list = sf_fixture_listing(
      paste0("cars/", "20240101T000002Z-bbb", "/data.txt"), board = board
    ),
    get = sf_mock_get_files("data.txt" = yaml::as.yaml(meta)),
    casing = "upper"
  )
  testthat::local_mocked_bindings(
    sf_stage_cmd = rec$responder, .package = "pinsExtras"
  )

  out <- pins::pin_meta(board, "cars")

  expect_identical(out$local$version, "20240101T000002Z-bbb")
})

test_that("pin_fetch issues one listing, two GETs, and fetches the payload", {
  board <- sf_mock_board(path = "team-data", stage = "@mystage")
  v <- "20240101T000002Z-bbb"
  meta <- list(
    api_version = 1L, file = "cars.rds", file_size = 12,
    created = "20240101", pin_hash = "abc1234567", type = "rds"
  )
  rec <- sf_mock_transport(
    list = sf_fixture_listing(paste0("cars/", v, "/data.txt"), board = board),
    get = sf_mock_get_files(
      "data.txt" = yaml::as.yaml(meta),
      "cars.rds" = "payload bytes"
    )
  )
  testthat::local_mocked_bindings(
    sf_stage_cmd = rec$responder, .package = "pinsExtras"
  )

  out <- pins::pin_fetch(board, "cars")

  expect_length(grep("^LIST ", rec$calls), 1L)
  expect_length(grep("^GET ", rec$calls), 2L)
  expect_identical(out$local$version, v)
  expect_identical(out$file, "cars.rds")
  expect_true(fs::file_exists(fs::path(out$local$dir, "data.txt")))
  expect_true(fs::file_exists(fs::path(out$local$dir, "cars.rds")))
})
