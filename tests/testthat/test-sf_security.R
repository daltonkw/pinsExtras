# Security hardening: a path segment can never delete the whole board, and a
# discovered name carrying a separator or dot-segment is never trusted.

# ---- sf_check_path_segment(): the validator -------------------------------

test_that("sf_check_path_segment accepts an ordinary name", {
  expect_identical(pinsExtras:::sf_check_path_segment("cars"), TRUE)
})

test_that("sf_check_path_segment accepts a non-timestamp name (escape hatch)", {
  expect_identical(
    pinsExtras:::sf_check_path_segment("bogus-def12"),
    TRUE
  )
})

test_that("sf_check_path_segment accepts a well-formed version id", {
  expect_identical(
    pinsExtras:::sf_check_path_segment(sf_fixture_version()),
    TRUE
  )
})

test_that("sf_check_path_segment accepts hyphen, underscore and dot", {
  expect_identical(pinsExtras:::sf_check_path_segment("my.pin"), TRUE)
  expect_identical(pinsExtras:::sf_check_path_segment("a_b-c.d"), TRUE)
})

test_that("sf_check_path_segment rejects an empty string, classed", {
  expect_error(
    pinsExtras:::sf_check_path_segment(""),
    class = "pinsExtras_invalid_path_segment"
  )
})

test_that("sf_check_path_segment rejects NA_character_ and NA, classed", {
  expect_error(
    pinsExtras:::sf_check_path_segment(NA_character_),
    class = "pinsExtras_invalid_path_segment"
  )
  expect_error(
    pinsExtras:::sf_check_path_segment(NA),
    class = "pinsExtras_invalid_path_segment"
  )
})

test_that("sf_check_path_segment rejects a zero-length vector, classed", {
  expect_error(
    pinsExtras:::sf_check_path_segment(character(0)),
    class = "pinsExtras_invalid_path_segment"
  )
})

test_that("sf_check_path_segment rejects a multi-element vector, classed", {
  expect_error(
    pinsExtras:::sf_check_path_segment(c("a", "b")),
    class = "pinsExtras_invalid_path_segment"
  )
})

test_that("sf_check_path_segment rejects a non-string, classed", {
  expect_error(
    pinsExtras:::sf_check_path_segment(123),
    class = "pinsExtras_invalid_path_segment"
  )
})

test_that(
  "sf_check_path_segment rejects slash, dotdot, dot, nested, hidden,
  dot-embedded and backslash, all classed",
  {
    for (bad in c(
      "/", "a/b", "..", ".", "a/../b", "..hidden", "a..b", "a\\b"
    )) {
      expect_error(
        pinsExtras:::sf_check_path_segment(bad),
        class = "pinsExtras_invalid_path_segment"
      )
    }
  }
)

test_that(
  "the abort message carries the class but never repeats the value",
  {
    cnd <- tryCatch(
      pinsExtras:::sf_check_path_segment("SENTINEL/../x"),
      error = function(e) e
    )
    expect_true(
      "pinsExtras_invalid_path_segment" %in% class(cnd)
    )
    expect_false(
      grepl("SENTINEL", conditionMessage(cnd), fixed = TRUE)
    )
  }
)

# ---- pin_version_delete(): supplied arguments abort before any command ----

test_that("pin_version_delete(board, \"cars\", \"\") issues nothing", {
  board <- sf_mock_board()
  rec <- sf_mock_transport()
  testthat::local_mocked_bindings(
    sf_stage_cmd = rec$responder,
    .package = "pinsExtras"
  )

  expect_error(
    pins::pin_version_delete(board, "cars", "/"),
    class = "pinsExtras_invalid_path_segment"
  )
  expect_length(rec$calls, 0L)
})

test_that("pin_version_delete(board, \"/\", \"/\") issues nothing", {
  board <- sf_mock_board()
  rec <- sf_mock_transport()
  testthat::local_mocked_bindings(
    sf_stage_cmd = rec$responder,
    .package = "pinsExtras"
  )

  expect_error(
    pins::pin_version_delete(board, "/", "/"),
    class = "pinsExtras_invalid_path_segment"
  )
  expect_length(rec$calls, 0L)
})

test_that("pin_version_delete(board, \"cars\", \"..\") issues nothing", {
  board <- sf_mock_board()
  rec <- sf_mock_transport()
  testthat::local_mocked_bindings(
    sf_stage_cmd = rec$responder,
    .package = "pinsExtras"
  )

  expect_error(
    pins::pin_version_delete(board, "cars", ".."),
    class = "pinsExtras_invalid_path_segment"
  )
  expect_length(rec$calls, 0L)
})

test_that(
  "pin_version_delete still removes a malformed-but-valid name (escape hatch)",
  {
    board <- sf_mock_board()
    rec <- sf_mock_transport()
    testthat::local_mocked_bindings(
      sf_stage_cmd = rec$responder,
      .package = "pinsExtras"
    )

    out <- pins::pin_version_delete(board, "cars", "bogus-def12")
    expect_identical(out, board)
    expect_identical(rec$calls, "REMOVE '@~/cars/bogus-def12/'")
  }
)

# ---- pin_delete(): a supplied ".." aborts before any command --------------

test_that("pin_delete(board, \"..\") issues nothing", {
  board <- sf_mock_board()
  rec <- sf_mock_transport()
  testthat::local_mocked_bindings(
    sf_stage_cmd = rec$responder,
    .package = "pinsExtras"
  )

  expect_error(
    pins::pin_delete(board, ".."),
    class = "pinsExtras_invalid_path_segment"
  )
  expect_length(rec$calls, 0L)
})

# ---- sf_published_index(): discovered names are filtered ------------------

test_that("sf_published_index drops a discovered name of \"..\"", {
  out <- pinsExtras:::sf_published_index(
    sf_fixture_listing(
      "../20240101T000000Z-abc12/data.txt",
      "cars/20240101T000000Z-abc12/data.txt"
    )
  )
  expect_identical(out$name, "cars")
})

test_that("sf_published_index drops a discovered version with a backslash", {
  out <- pinsExtras:::sf_published_index(
    sf_fixture_listing(
      paste0("cars/20240101T000000Z-abc12/data.txt"),
      paste0("bad/20240101T000000Z-a\\\\b/data.txt")
    )
  )
  expect_identical(out$name, "cars")
})

# ---- read-side methods refuse a traversal name instead of escaping --------

test_that("pin_list omits a traversal name from the listing", {
  board <- sf_mock_board(stage = "@~")
  rec <- sf_mock_transport(
    list = sf_fixture_listing(
      "../20240101T000000Z-abc12/data.txt",
      "cars/20240101T000000Z-abc12/data.txt",
      board = board
    )
  )
  testthat::local_mocked_bindings(
    sf_stage_cmd = rec$responder,
    .package = "pinsExtras"
  )

  out <- pins::pin_list(board)
  expect_identical(out, "cars")
})

test_that(
  "pin_meta on a board whose only row is a traversal name is not found",
  {
    board <- sf_mock_board(stage = "@~")
    rec <- sf_mock_transport(
      list = sf_fixture_listing(
        "../20240101T000000Z-abc12/data.txt",
        board = board
      )
    )
    testthat::local_mocked_bindings(
      sf_stage_cmd = rec$responder,
      .package = "pinsExtras"
    )

    expect_error(
      pins::pin_meta(board, ".."),
      "Can't find pin called",
      fixed = TRUE
    )
    # Only the LIST issued; no GET, and no path was built outside the cache.
    expect_length(grep("^LIST ", rec$calls), 1L)
    expect_length(grep("^GET ", rec$calls), 0L)
  }
)
