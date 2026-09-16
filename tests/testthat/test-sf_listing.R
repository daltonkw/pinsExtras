# Offline tests for the scoped sf_stage_list() LIST. Mock only the transport.

test_that("sf_stage_list emits the scoped LIST and returns board-rooted names", {
  # One table over (board shape, dir): the exact SQL issued and the exact
  # names returned. The trailing slash on a non-empty prefix is the whole
  # anti-prefix design, and the stage root deliberately carries none.
  cases <- list(
    list(
      name = "stage root, no dir",
      board = list(),
      dir = NULL,
      listed = "cars/v/data.txt",
      sql = "LIST '@~'",
      want = "cars/v/data.txt"
    ),
    list(
      name = "pin directory drops the sibling",
      board = list(),
      dir = "cars",
      listed = c("cars/v/data.txt", "cars_extra/v/data.txt"),
      sql = "LIST '@~/cars/'",
      want = "cars/v/data.txt"
    ),
    list(
      name = "named stage, board path, root dir",
      board = list(path = "team-data", stage = "@mystage"),
      dir = NULL,
      listed = "cars/v/data.txt",
      sql = "LIST '@mystage/team-data/'",
      want = "team-data/cars/v/data.txt"
    ),
    list(
      name = "named stage, board path, pin dir",
      board = list(path = "team-data", stage = "@mystage"),
      dir = "cars",
      listed = "cars/v/data.txt",
      sql = "LIST '@mystage/team-data/cars/'",
      want = "team-data/cars/v/data.txt"
    ),
    list(
      # The stage name is the last dotted component, "st", so the listing
      # carries no prefix and nothing is stripped.
      name = "dotted stage name",
      board = list(stage = "@db.schema.st"),
      dir = NULL,
      listed = "cars/v/data.txt",
      sql = "LIST '@db.schema.st'",
      want = "cars/v/data.txt"
    ),
    list(
      name = "directory containing a space",
      board = list(),
      dir = "my pins",
      listed = "my pins/v/data.txt",
      sql = "LIST '@~/my pins/'",
      want = "my pins/v/data.txt"
    ),
    list(
      # An apostrophe in a directory is escaped into the SQL literal. A
      # quoting regression here is an injection, so the expected SQL is
      # written out rather than computed with the quoting helper.
      name = "directory containing an apostrophe",
      board = list(),
      dir = "my'pin",
      listed = "my'pin/v/data.txt",
      sql = "LIST '@~/my\\'pin/'",
      want = "my'pin/v/data.txt"
    )
  )

  for (case in cases) {
    board <- do.call(sf_mock_board, case$board)
    rec <- sf_mock_transport(
      list = sf_fixture_listing(case$listed, board = board)
    )
    testthat::local_mocked_bindings(
      sf_stage_cmd = rec$responder,
      .package = "pinsExtras"
    )

    out <- if (is.null(case$dir)) {
      pinsExtras:::sf_stage_list(board)
    } else {
      pinsExtras:::sf_stage_list(board, case$dir)
    }

    expect_identical(rec$calls, case$sql, info = case$name)
    expect_identical(out$name, case$want, info = case$name)
  }
})

test_that("sf_stage_list answers identically for both driver casings", {
  # The ODBC driver's column casing is not guaranteed across versions, so
  # both casings must produce the same SQL, the same rows and the same
  # lower-case column names.
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
    list(calls = rec$calls, names = names(out), name = out$name)
  }

  upper <- build("upper")
  lower <- build("lower")

  expect_identical(upper, lower)
  expect_identical(upper$calls, "LIST '@~/cars/'")
  expect_identical(upper$name, "cars/v/data.txt")
  expect_identical(upper$names, c("name", "size", "md5", "last_modified"))
})
