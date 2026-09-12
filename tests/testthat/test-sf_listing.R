# Offline tests for the scoped sf_stage_list() LIST. Mock only the transport.

test_that("sf_stage_list issues a bare LIST for an empty board and dir", {
  board <- sf_mock_board()
  rec <- sf_mock_transport(list = sf_fixture_listing("cars/v/data.txt"))
  testthat::local_mocked_bindings(
    sf_stage_cmd = rec$responder,
    .package = "pinsExtras"
  )

  out <- pinsExtras:::sf_stage_list(board)

  expect_identical(rec$calls, "LIST '@~'")
  expect_identical(out$name, "cars/v/data.txt")
})

test_that("sf_stage_list scopes the LIST to a pinned directory", {
  board <- sf_mock_board()
  rec <- sf_mock_transport(
    list = sf_fixture_listing(
      "cars/v/data.txt",
      "cars_extra/v/data.txt"
    )
  )
  testthat::local_mocked_bindings(
    sf_stage_cmd = rec$responder,
    .package = "pinsExtras"
  )

  out <- pinsExtras:::sf_stage_list(board, "cars")

  expect_identical(rec$calls, "LIST '@~/cars/'")
  expect_identical(out$name, "cars/v/data.txt")
})

test_that("sf_stage_list scopes a named stage to the board path", {
  board <- sf_mock_board(path = "team-data", stage = "@mystage")
  rec <- sf_mock_transport(
    list = sf_fixture_listing("cars/v/data.txt", board = board)
  )
  testthat::local_mocked_bindings(
    sf_stage_cmd = rec$responder,
    .package = "pinsExtras"
  )

  out <- pinsExtras:::sf_stage_list(board)

  expect_identical(rec$calls, "LIST '@mystage/team-data/'")
  expect_identical(out$name, "team-data/cars/v/data.txt")
})

test_that("sf_stage_list scopes a named stage to a directory too", {
  board <- sf_mock_board(path = "team-data", stage = "@mystage")
  rec <- sf_mock_transport(
    list = sf_fixture_listing(
      "cars/v/data.txt",
      board = board
    )
  )
  testthat::local_mocked_bindings(
    sf_stage_cmd = rec$responder,
    .package = "pinsExtras"
  )

  out <- pinsExtras:::sf_stage_list(board, "cars")

  expect_identical(rec$calls, "LIST '@mystage/team-data/cars/'")
  expect_identical(out$name, "team-data/cars/v/data.txt")
})

test_that("sf_stage_list lists a dotted stage root verbatim", {
  board <- sf_mock_board(stage = "@db.schema.st")
  rec <- sf_mock_transport(
    list = sf_fixture_listing("cars/v/data.txt", board = board)
  )
  testthat::local_mocked_bindings(
    sf_stage_cmd = rec$responder,
    .package = "pinsExtras"
  )

  out <- pinsExtras:::sf_stage_list(board)

  # stage name is "st", so the path is not prefixed with "st/"
  expect_identical(rec$calls, "LIST '@db.schema.st'")
  expect_identical(out$name, "cars/v/data.txt")
})

test_that("sf_stage_list scopes a directory containing a space", {
  board <- sf_mock_board()
  rec <- sf_mock_transport(
    list = sf_fixture_listing("my pins/v/data.txt")
  )
  testthat::local_mocked_bindings(
    sf_stage_cmd = rec$responder,
    .package = "pinsExtras"
  )

  out <- pinsExtras:::sf_stage_list(board, "my pins")

  expect_identical(rec$calls, "LIST '@~/my pins/'")
  expect_identical(out$name, "my pins/v/data.txt")
})

test_that("sf_stage_list keeps the exact path and drops the neighbour", {
  board <- sf_mock_board()
  rec <- sf_mock_transport(
    list = sf_fixture_listing(
      "cars/v/data.txt",
      "cars_extra/v/data.txt"
    )
  )
  testthat::local_mocked_bindings(
    sf_stage_cmd = rec$responder,
    .package = "pinsExtras"
  )

  out <- pinsExtras:::sf_stage_list(board, "cars")

  expect_identical(out$name, "cars/v/data.txt")
})

test_that("sf_stage_list keeps a response whose name equals the prefix", {
  board <- sf_mock_board()
  rec <- sf_mock_transport(
    list = sf_fixture_listing("cars")
  )
  testthat::local_mocked_bindings(
    sf_stage_cmd = rec$responder,
    .package = "pinsExtras"
  )

  out <- pinsExtras:::sf_stage_list(board, "cars")

  expect_identical(out$name, "cars")
})

test_that("sf_stage_list strips the stage name but keeps the board path", {
  board <- sf_mock_board(path = "team-data", stage = "@mystage")
  rec <- sf_mock_transport(
    list = sf_fixture_listing(
      "cars/v/data.txt",
      board = board
    )
  )
  testthat::local_mocked_bindings(
    sf_stage_cmd = rec$responder,
    .package = "pinsExtras"
  )

  out <- pinsExtras:::sf_stage_list(board, "cars")

  expect_identical(out$name, "team-data/cars/v/data.txt")
})

test_that("sf_stage_list strips the stage name with an empty directory", {
  board <- sf_mock_board(path = "team-data", stage = "@mystage")
  rec <- sf_mock_transport(
    list = sf_fixture_listing(
      "cars/v/data.txt",
      board = board
    )
  )
  testthat::local_mocked_bindings(
    sf_stage_cmd = rec$responder,
    .package = "pinsExtras"
  )

  out <- pinsExtras:::sf_stage_list(board)

  expect_identical(out$name, "team-data/cars/v/data.txt")
})

test_that("sf_stage_list leaves a partially prefixed response untouched", {
  board <- sf_mock_board(stage = "@mystage")
  rec <- sf_mock_transport(
    list = data.frame(
      name = c("mystage/a/x", "b/y"),
      size = 1, md5 = "m", last_modified = "L",
      stringsAsFactors = FALSE
    )
  )
  testthat::local_mocked_bindings(
    sf_stage_cmd = rec$responder,
    .package = "pinsExtras"
  )

  out <- pinsExtras:::sf_stage_list(board)

  expect_identical(out$name, c("mystage/a/x", "b/y"))
})

test_that("sf_stage_list strips the stage name only when every row has it", {
  board <- sf_mock_board(stage = "@mystage")
  rec <- sf_mock_transport(
    list = sf_fixture_listing("a/x", board = board)
  )
  testthat::local_mocked_bindings(
    sf_stage_cmd = rec$responder,
    .package = "pinsExtras"
  )

  out <- pinsExtras:::sf_stage_list(board)

  # every row carries the "mystage/" prefix, so it is stripped
  expect_identical(out$name, "a/x")
})

test_that("sf_stage_list strips the stage name for a metacharacter stage", {
  board <- sf_mock_board(stage = "@x+y")
  rec <- sf_mock_transport(
    list = sf_fixture_listing("cars/v/data.txt", board = board)
  )
  testthat::local_mocked_bindings(
    sf_stage_cmd = rec$responder,
    .package = "pinsExtras"
  )

  out <- pinsExtras:::sf_stage_list(board)

  # substr (not sub) is what makes "x+y/" strip correctly
  expect_identical(out$name, "cars/v/data.txt")
})

test_that("sf_stage_list returns a lower-cased zero-row response", {
  board <- sf_mock_board()
  rec <- sf_mock_transport(
    list = sf_fixture_listing(),
    casing = "upper"
  )
  testthat::local_mocked_bindings(
    sf_stage_cmd = rec$responder,
    .package = "pinsExtras"
  )

  out <- pinsExtras:::sf_stage_list(board, "cars")

  expect_identical(rec$calls, "LIST '@~/cars/'")
  expect_length(out$name, 0)
  expect_identical(names(out), c("name", "size", "md5", "last_modified"))
})

test_that("sf_stage_list lower-cases the columns of a non-empty response", {
  board <- sf_mock_board()
  rec <- sf_mock_transport(
    list = sf_fixture_listing("cars/v/data.txt"),
    casing = "upper"
  )
  testthat::local_mocked_bindings(
    sf_stage_cmd = rec$responder,
    .package = "pinsExtras"
  )

  out <- pinsExtras:::sf_stage_list(board, "cars")

  expect_identical(names(out), c("name", "size", "md5", "last_modified"))
  expect_identical(out$name, "cars/v/data.txt")
})

test_that("sf_stage_list upper- and lower-driver responses are identical", {
  build <- function(casing) {
    board <- sf_mock_board()
    rec <- sf_mock_transport(
      list = sf_fixture_listing(
        "cars/v/data.txt",
        "cars_extra/v/data.txt"
      ),
      casing = casing
    )
    testthat::local_mocked_bindings(
      sf_stage_cmd = rec$responder,
      .package = "pinsExtras"
    )
    out <- pinsExtras:::sf_stage_list(board, "cars")
    c(rec$calls, out$name)
  }

  upper <- build("upper")
  lower <- build("lower")

  expect_identical(upper, lower)
  expect_identical(upper[1], "LIST '@~/cars/'")
  expect_identical(upper[2], "cars/v/data.txt")
})

test_that("sf_stage_list passes through an apostrophe in the directory", {
  board <- sf_mock_board()
  rec <- sf_mock_transport(
    list = sf_fixture_listing("my'pin/v/data.txt")
  )
  testthat::local_mocked_bindings(
    sf_stage_cmd = rec$responder,
    .package = "pinsExtras"
  )

  out <- pinsExtras:::sf_stage_list(board, "my'pin")

  expect_identical(
    rec$calls,
    paste0("LIST ", pinsExtras:::sf_quote_stage_path(paste0("@~/my'pin", "/")))
  )
  expect_identical(out$name, "my'pin/v/data.txt")
})

test_that("sf_stage_list issues exactly one command per call", {
  board <- sf_mock_board()
  rec <- sf_mock_transport(
    list = sf_fixture_listing("cars/v/data.txt")
  )
  testthat::local_mocked_bindings(
    sf_stage_cmd = rec$responder,
    .package = "pinsExtras"
  )

  pinsExtras:::sf_stage_list(board, "cars")

  expect_length(rec$calls, 1L)
  expect_match(rec$calls[[1]], "LIST '@~/cars/'", fixed = TRUE)
})
