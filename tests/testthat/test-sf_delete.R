# Exact deletion boundaries: a trailing slash scopes the REMOVE, and a
# single-file delete adds an anchored PATTERN. Without the slash Snowflake
# matches by prefix and "cars" would also remove "cars_extra".


test_that("sf_stage_delete_dir targets the directory with a slash", {
  board <- sf_mock_board()
  rec <- sf_mock_transport()
  testthat::local_mocked_bindings(
    sf_stage_cmd = rec$responder,
    .package = "pinsExtras"
  )

  expect_true(pinsExtras:::sf_stage_delete_dir(board, "cars"))
  expect_identical(rec$calls, "REMOVE '@~/cars/'")
})

test_that("sf_stage_delete_dir targets a version directory", {
  board <- sf_mock_board()
  v <- sf_fixture_version()
  rec <- sf_mock_transport()
  testthat::local_mocked_bindings(
    sf_stage_cmd = rec$responder,
    .package = "pinsExtras"
  )

  expect_true(pinsExtras:::sf_stage_delete_dir(board, paste0("cars/", v)))
  expect_identical(
    rec$calls,
    "REMOVE '@~/cars/20240101T000000Z-abc12/'"
  )
})

test_that("sf_stage_delete_file anchors a PATTERN for a single file", {
  board <- sf_mock_board()
  v <- sf_fixture_version()
  rec <- sf_mock_transport()
  testthat::local_mocked_bindings(
    sf_stage_cmd = rec$responder,
    .package = "pinsExtras"
  )

  expect_true(
    pinsExtras:::sf_stage_delete_file(board, paste0("cars/", v), "data.txt")
  )
  expect_identical(
    rec$calls,
    paste0(
      "REMOVE '@~/cars/20240101T000000Z-abc12/' PATTERN = '",
      "^(.*/)?data",
      "\\\\.txt$'"
    )
  )
})

test_that("sf_stage_delete_dir honours the board path and stage", {
  board <- sf_mock_board(path = "team-data", stage = "@mystage")
  rec <- sf_mock_transport()
  testthat::local_mocked_bindings(
    sf_stage_cmd = rec$responder,
    .package = "pinsExtras"
  )

  expect_true(pinsExtras:::sf_stage_delete_dir(board, "cars"))
  expect_identical(
    rec$calls,
    "REMOVE '@mystage/team-data/cars/'"
  )
})

test_that("sf_stage_delete_dir allows the whole board path with dir ''", {
  board <- sf_mock_board(path = "team-data", stage = "@mystage")
  rec <- sf_mock_transport()
  testthat::local_mocked_bindings(
    sf_stage_cmd = rec$responder,
    .package = "pinsExtras"
  )

  expect_true(pinsExtras:::sf_stage_delete_dir(board, ""))
  expect_identical(
    rec$calls,
    "REMOVE '@mystage/team-data/'"
  )
})

test_that("sf_stage_delete_file removes the manifest from the board root", {
  board <- sf_mock_board(path = "team-data", stage = "@mystage")
  rec <- sf_mock_transport()
  testthat::local_mocked_bindings(
    sf_stage_cmd = rec$responder,
    .package = "pinsExtras"
  )

  expect_true(pinsExtras:::sf_stage_delete_file(board, "", "_pins.yaml"))
  expect_identical(
    rec$calls,
    paste0(
      "REMOVE '@mystage/team-data/' PATTERN = '",
      "^(.*/)?_pins",
      "\\\\.yaml$'"
    )
  )
})

test_that("sf_stage_delete_file scopes to one file on a board with a path", {
  board <- sf_mock_board(path = "team-data", stage = "@mystage")
  v <- sf_fixture_version()
  rec <- sf_mock_transport()
  testthat::local_mocked_bindings(
    sf_stage_cmd = rec$responder,
    .package = "pinsExtras"
  )

  expect_true(
    pinsExtras:::sf_stage_delete_file(board, paste0("cars/", v), "data.txt")
  )
  expect_identical(
    rec$calls,
    paste0(
      "REMOVE '@mystage/team-data/cars/20240101T000000Z-abc12/' PATTERN = '",
      "^(.*/)?data",
      "\\\\.txt$'"
    )
  )
})

test_that(
  "sf_stage_delete_file quotes a pin name containing a dot in the location",
  {
    board <- sf_mock_board()
    v <- sf_fixture_version()
    rec <- sf_mock_transport()
    testthat::local_mocked_bindings(
      sf_stage_cmd = rec$responder,
      .package = "pinsExtras"
    )

    expect_true(
      pinsExtras:::sf_stage_delete_file(board, paste0("my.pin/", v), "data.txt")
    )
    expect_identical(
      rec$calls,
      paste0(
        "REMOVE '@~/my.pin/20240101T000000Z-abc12/' PATTERN = '",
        "^(.*/)?data",
        "\\\\.txt$'"
      )
    )
  }
)

test_that("sf_stage_delete_dir refuses an empty directory on the stage root", {
  board <- sf_mock_board()
  rec <- sf_mock_transport()
  testthat::local_mocked_bindings(
    sf_stage_cmd = rec$responder,
    .package = "pinsExtras"
  )

  expect_error(
    pinsExtras:::sf_stage_delete_dir(board, ""),
    class = "pinsExtras_invalid_delete_target"
  )
  expect_length(rec$calls, 0L)
})

test_that("sf_stage_delete_dir refuses a root slash on the stage root", {
  board <- sf_mock_board()
  rec <- sf_mock_transport()
  testthat::local_mocked_bindings(
    sf_stage_cmd = rec$responder,
    .package = "pinsExtras"
  )

  expect_error(
    pinsExtras:::sf_stage_delete_dir(board, "/"),
    class = "pinsExtras_invalid_delete_target"
  )
  expect_length(rec$calls, 0L)
})

test_that("sf_stage_delete_dir refuses a non-string directory", {
  board <- sf_mock_board()
  rec <- sf_mock_transport()
  testthat::local_mocked_bindings(
    sf_stage_cmd = rec$responder,
    .package = "pinsExtras"
  )

  expect_error(
    pinsExtras:::sf_stage_delete_dir(board, 123),
    class = "pinsExtras_invalid_delete_target"
  )
  expect_length(rec$calls, 0L)
})

test_that("sf_stage_delete_dir refuses a multi-value directory", {
  board <- sf_mock_board()
  rec <- sf_mock_transport()
  testthat::local_mocked_bindings(
    sf_stage_cmd = rec$responder,
    .package = "pinsExtras"
  )

  expect_error(
    pinsExtras:::sf_stage_delete_dir(board, c("a", "b")),
    class = "pinsExtras_invalid_delete_target"
  )
  expect_length(rec$calls, 0L)
})

test_that("sf_stage_delete_file refuses an empty file name", {
  board <- sf_mock_board()
  v <- sf_fixture_version()
  rec <- sf_mock_transport()
  testthat::local_mocked_bindings(
    sf_stage_cmd = rec$responder,
    .package = "pinsExtras"
  )

  expect_error(
    pinsExtras:::sf_stage_delete_file(board, paste0("cars/", v), ""),
    class = "pinsExtras_invalid_delete_target"
  )
  expect_length(rec$calls, 0L)
})

test_that("sf_stage_delete_file refuses a file name containing a slash", {
  board <- sf_mock_board()
  v <- sf_fixture_version()
  rec <- sf_mock_transport()
  testthat::local_mocked_bindings(
    sf_stage_cmd = rec$responder,
    .package = "pinsExtras"
  )

  expect_error(
    pinsExtras:::sf_stage_delete_file(board, paste0("cars/", v), "a/b"),
    class = "pinsExtras_invalid_delete_target"
  )
  expect_length(rec$calls, 0L)
})

test_that("sf_stage_delete_file refuses a non-string file name", {
  board <- sf_mock_board()
  v <- sf_fixture_version()
  rec <- sf_mock_transport()
  testthat::local_mocked_bindings(
    sf_stage_cmd = rec$responder,
    .package = "pinsExtras"
  )

  expect_error(
    pinsExtras:::sf_stage_delete_file(board, paste0("cars/", v), 123),
    class = "pinsExtras_invalid_delete_target"
  )
  expect_length(rec$calls, 0L)
})

test_that("the delete PATTERN matches only the intended file", {
  pattern <- pinsExtras:::sf_remove_pattern("data.txt")

  expect_true(
    grepl(pattern, "mystage/cars/20240101T000000Z-abc12/data.txt")
  )
  expect_true(
    grepl(pattern, "cars/20240101T000000Z-abc12/data.txt")
  )
  expect_false(
    grepl(pattern, "mystage/cars/20240101T000000Z-abc12/data.txt.bak")
  )
  expect_false(
    grepl(pattern, "mystage/cars/20240101T000000Z-abc12/cars.rds")
  )
})

test_that("sf_stage_delete_dir refuses an NA directory with zero commands", {
  board <- sf_mock_board()
  rec <- sf_mock_transport()
  testthat::local_mocked_bindings(
    sf_stage_cmd = rec$responder,
    .package = "pinsExtras"
  )

  expect_error(
    pinsExtras:::sf_stage_delete_dir(board, NA_character_),
    class = "pinsExtras_invalid_delete_target"
  )
  expect_length(rec$calls, 0L)
})

test_that("sf_stage_delete_file refuses an NA file name with zero commands", {
  board <- sf_mock_board()
  v <- sf_fixture_version()
  rec <- sf_mock_transport()
  testthat::local_mocked_bindings(
    sf_stage_cmd = rec$responder,
    .package = "pinsExtras"
  )

  expect_error(
    pinsExtras:::sf_stage_delete_file(board, paste0("cars/", v), NA_character_),
    class = "pinsExtras_invalid_delete_target"
  )
  expect_length(rec$calls, 0L)
})

# ---- the three board methods, dispatched through the generics -------------

test_that("pin_delete() lists then removes one published pin", {
  board <- sf_mock_board()
  rec <- sf_mock_transport(
    list = sf_fixture_listing(
      "cars/20240101T000000Z-abc12/data.txt"
    )
  )
  testthat::local_mocked_bindings(
    sf_stage_cmd = rec$responder,
    .package = "pinsExtras"
  )

  out <- pins::pin_delete(board, "cars")
  expect_identical(out, board)
  expect_identical(rec$calls, c("LIST '@~/cars/'", "REMOVE '@~/cars/'"))
})

test_that("pin_delete() reports a payload-only pin as not found", {
  board <- sf_mock_board()
  rec <- sf_mock_transport(
    list = sf_fixture_listing(
      "cars/20240101T000000Z-abc12/payload.rds"
    )
  )
  testthat::local_mocked_bindings(
    sf_stage_cmd = rec$responder,
    .package = "pinsExtras"
  )

  expect_error(
    pins::pin_delete(board, "cars"),
    "find pin called"
  )
  expect_length(grep("^LIST ", rec$calls), 1L)
  expect_length(grep("^REMOVE ", rec$calls), 0L)
})

test_that("pin_delete() lists and removes each name in order", {
  board <- sf_mock_board()
  rec <- sf_mock_transport(
    list = sf_fixture_listing(
      "a/20240101T000000Z-abc12/data.txt",
      "b/20240101T000000Z-abc12/data.txt"
    )
  )
  testthat::local_mocked_bindings(
    sf_stage_cmd = rec$responder,
    .package = "pinsExtras"
  )

  pins::pin_delete(board, c("a", "b"))
  expect_identical(
    rec$calls,
    c(
      "LIST '@~/a/'", "REMOVE '@~/a/'",
      "LIST '@~/b/'", "REMOVE '@~/b/'"
    )
  )
})

test_that("pin_delete(character(0)) issues nothing and returns the board", {
  board <- sf_mock_board()
  rec <- sf_mock_transport()
  testthat::local_mocked_bindings(
    sf_stage_cmd = rec$responder,
    .package = "pinsExtras"
  )

  out <- pins::pin_delete(board, character(0))
  expect_identical(out, board)
  expect_length(rec$calls, 0L)
})

test_that("pin_delete() aborts on an empty name with nothing issued", {
  board <- sf_mock_board()
  rec <- sf_mock_transport()
  testthat::local_mocked_bindings(
    sf_stage_cmd = rec$responder,
    .package = "pinsExtras"
  )

  expect_error(
    pins::pin_delete(board, ""),
    "must be non-empty strings"
  )
  expect_length(rec$calls, 0L)
})

test_that("pin_delete() removes the pin directory, never a sibling", {
  board <- sf_mock_board()
  rec <- sf_mock_transport(
    list = sf_fixture_listing(
      "cars/20240101T000000Z-abc12/data.txt",
      "cars_extra/20240101T000000Z-abc12/data.txt"
    )
  )
  testthat::local_mocked_bindings(
    sf_stage_cmd = rec$responder,
    .package = "pinsExtras"
  )

  pins::pin_delete(board, "cars")
  expect_identical(
    grep("^REMOVE ", rec$calls, value = TRUE),
    "REMOVE '@~/cars/'"
  )
})

test_that("pin_version_delete() removes a raw directory without listing", {
  board <- sf_mock_board()
  rec <- sf_mock_transport()
  testthat::local_mocked_bindings(
    sf_stage_cmd = rec$responder,
    .package = "pinsExtras"
  )

  out <- pins::pin_version_delete(board, "cars", sf_fixture_version())
  expect_identical(out, board)
  expect_identical(
    rec$calls,
    "REMOVE '@~/cars/20240101T000000Z-abc12/'"
  )
  expect_length(grep("^LIST ", rec$calls), 0L)
})

test_that("pin_version_delete() aborts on an empty name or version", {
  board <- sf_mock_board()
  rec <- sf_mock_transport()
  testthat::local_mocked_bindings(
    sf_stage_cmd = rec$responder,
    .package = "pinsExtras"
  )

  expect_error(
    pins::pin_version_delete(board, "", sf_fixture_version()),
    "must be a non-empty string"
  )
  expect_error(
    pins::pin_version_delete(board, "cars", ""),
    "must be a non-empty string"
  )
  expect_length(rec$calls, 0L)
})

test_that("write_board_manifest_yaml() overwrites the manifest file", {
  board <- sf_mock_board()
  rec <- sf_mock_transport()
  testthat::local_mocked_bindings(
    sf_stage_cmd = rec$responder,
    .package = "pinsExtras"
  )

  pins::write_board_manifest_yaml(board, list(pins = "v1"))
  put <- grep("^PUT ", rec$calls, value = TRUE)
  expect_length(put, 1L)
  expect_match(put, "OVERWRITE=TRUE", fixed = TRUE)
  expect_length(grep("^LIST ", rec$calls), 0L)
  expect_length(grep("^REMOVE ", rec$calls), 0L)
})

# ---- sf_stage_exists(), the rewritten file-existence check (3.4) ---------

test_that("sf_stage_exists() is TRUE when the file is listed", {
  board <- sf_mock_board()
  rec <- sf_mock_transport(
    list = sf_fixture_listing(
      "cars/20240101T000000Z-abc12/data.txt"
    )
  )
  testthat::local_mocked_bindings(
    sf_stage_cmd = rec$responder,
    .package = "pinsExtras"
  )

  expect_true(
    pinsExtras:::sf_stage_exists(board, "cars/20240101T000000Z-abc12/data.txt")
  )
  expect_identical(
    rec$calls,
    "LIST '@~/cars/20240101T000000Z-abc12/'"
  )
})

test_that("sf_stage_exists() is FALSE when the file is not listed", {
  board <- sf_mock_board()
  rec <- sf_mock_transport()
  testthat::local_mocked_bindings(
    sf_stage_cmd = rec$responder,
    .package = "pinsExtras"
  )

  expect_false(
    pinsExtras:::sf_stage_exists(board, "cars/20240101T000000Z-abc12/data.txt")
  )
  expect_identical(
    rec$calls,
    "LIST '@~/cars/20240101T000000Z-abc12/'"
  )
})

test_that("sf_stage_exists() does not match a sibling named after the file", {
  board <- sf_mock_board()
  rec <- sf_mock_transport(
    list = sf_fixture_listing(
      "cars/20240101T000000Z-abc12/data.txt.bak"
    )
  )
  testthat::local_mocked_bindings(
    sf_stage_cmd = rec$responder,
    .package = "pinsExtras"
  )

  expect_false(
    pinsExtras:::sf_stage_exists(board, "cars/20240101T000000Z-abc12/data.txt")
  )
  expect_identical(
    rec$calls,
    "LIST '@~/cars/20240101T000000Z-abc12/'"
  )
})

test_that("sf_stage_exists() checks the board root for _pins.yaml", {
  board <- sf_mock_board()
  rec <- sf_mock_transport(
    list = sf_fixture_listing("_pins.yaml")
  )
  testthat::local_mocked_bindings(
    sf_stage_cmd = rec$responder,
    .package = "pinsExtras"
  )

  expect_true(pinsExtras:::sf_stage_exists(board, "_pins.yaml"))
  expect_identical(rec$calls, "LIST '@~'")
})

test_that("sf_stage_exists() is FALSE for _pins.yaml when it is absent", {
  board <- sf_mock_board()
  rec <- sf_mock_transport()
  testthat::local_mocked_bindings(
    sf_stage_cmd = rec$responder,
    .package = "pinsExtras"
  )

  expect_false(pinsExtras:::sf_stage_exists(board, "_pins.yaml"))
  expect_identical(rec$calls, "LIST '@~'")
})

test_that("sf_stage_exists() honours a named stage and a board path", {
  board <- sf_mock_board(path = "team-data", stage = "@mystage")
  rec <- sf_mock_transport(
    list = sf_fixture_listing(
      "cars/20240101T000000Z-abc12/data.txt",
      board = board
    )
  )
  testthat::local_mocked_bindings(
    sf_stage_cmd = rec$responder,
    .package = "pinsExtras"
  )

  expect_true(
    pinsExtras:::sf_stage_exists(board, "cars/20240101T000000Z-abc12/data.txt")
  )
  expect_identical(
    rec$calls,
    "LIST '@mystage/team-data/cars/20240101T000000Z-abc12/'"
  )
})

test_that("sf_stage_exists() checks _pins.yaml at a named stage root", {
  board <- sf_mock_board(path = "team-data", stage = "@mystage")
  rec <- sf_mock_transport(
    list = sf_fixture_listing("_pins.yaml", board = board)
  )
  testthat::local_mocked_bindings(
    sf_stage_cmd = rec$responder,
    .package = "pinsExtras"
  )

  expect_true(pinsExtras:::sf_stage_exists(board, "_pins.yaml"))
  expect_identical(rec$calls, "LIST '@mystage/team-data/'")
})

test_that("sf_stage_exists() is FALSE for a directory that does not exist", {
  board <- sf_mock_board()
  rec <- sf_mock_transport()
  testthat::local_mocked_bindings(
    sf_stage_cmd = rec$responder,
    .package = "pinsExtras"
  )

  expect_false(pinsExtras:::sf_stage_exists(board, "nope/x/y"))
  expect_identical(rec$calls, "LIST '@~/nope/x/'")
})
