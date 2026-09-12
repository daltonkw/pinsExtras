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
      "^(.*/)?cars/20240101T000000Z-abc12/data",
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
      "^(.*/)?team-data/_pins",
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
      "^(.*/)?team-data/cars/20240101T000000Z-abc12/data",
      "\\\\.txt$'"
    )
  )
})

test_that("sf_stage_delete_file escapes a dot in the pin name", {
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
      "^(.*/)?my\\\\.pin/20240101T000000Z-abc12/data",
      "\\\\.txt$'"
    )
  )
})

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
  board <- sf_mock_board()
  v <- sf_fixture_version()
  # The pattern sf_stage_delete_file issues, resolved against the board.
  pattern <- pinsExtras:::sf_remove_pattern(
    pinsExtras:::sf_normalize_path(board, paste0("cars/", v)),
    "data.txt"
  )

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
    grepl(
      pattern,
      "mystage/cars_extra/20240101T000000Z-abc12/data.txt"
    )
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
