# Exact deletion boundaries: a trailing slash scopes the REMOVE, and a
# single-file delete adds an anchored PATTERN. Without the slash Snowflake
# matches by prefix and "cars" would also remove "cars_extra".

# ---- the two REMOVE builders -------------------------------------------

test_that("sf_stage_delete_dir scopes the REMOVE with a trailing slash", {
  v <- sf_fixture_version()
  cases <- list(
    list(
      name = "pin directory",
      board = list(), dir = "cars",
      sql = "REMOVE '@~/cars/'"
    ),
    list(
      name = "version directory",
      board = list(), dir = paste0("cars/", v),
      sql = "REMOVE '@~/cars/20240101T000000Z-abc12/'"
    ),
    list(
      name = "named stage and board path",
      board = list(path = "team-data", stage = "@mystage"), dir = "cars",
      sql = "REMOVE '@mystage/team-data/cars/'"
    )
  )
  for (case in cases) {
    board <- do.call(sf_mock_board, case$board)
    rec <- sf_mock_bind()
    expect_true(
      pinsExtras:::sf_stage_delete_dir(board, case$dir),
      info = case$name
    )
    expect_identical(rec$calls, case$sql, info = case$name)
  }
})

test_that("sf_stage_delete_dir allows a pathed board to delete its own root", {
  # The guard is against the STAGE root, not the board root, so a board
  # with a path may delete that whole path with dir = "". The integration
  # suite's cleanup depends on exactly this case.
  board <- sf_mock_board(path = "team-data", stage = "@mystage")
  rec <- sf_mock_bind()

  expect_true(pinsExtras:::sf_stage_delete_dir(board, ""))
  expect_identical(rec$calls, "REMOVE '@mystage/team-data/'")
})

test_that("sf_stage_delete_file anchors a PATTERN to exactly one file", {
  v <- sf_fixture_version()
  cases <- list(
    list(
      name = "version directory",
      board = list(), dir = paste0("cars/", v), file = "data.txt",
      sql = paste0(
        "REMOVE '@~/cars/20240101T000000Z-abc12/' PATTERN = '",
        "^(cars/20240101T000000Z-abc12/)?data\\\\.txt$'"
      )
    ),
    list(
      # dir == "" has no parent to scope against, so the leading-path
      # group is dropped. This is how the manifest is removed.
      name = "board root",
      board = list(path = "team-data", stage = "@mystage"),
      dir = "", file = "_pins.yaml",
      sql = paste0(
        "REMOVE '@mystage/team-data/' PATTERN = '",
        "^_pins\\\\.yaml$'"
      )
    ),
    list(
      name = "named stage and board path",
      board = list(path = "team-data", stage = "@mystage"),
      dir = paste0("cars/", v), file = "data.txt",
      sql = paste0(
        "REMOVE '@mystage/team-data/cars/20240101T000000Z-abc12/' PATTERN = '",
        "^(cars/20240101T000000Z-abc12/)?data\\\\.txt$'"
      )
    ),
    list(
      # The LOCATION carries the dot verbatim; the PATTERN escapes it.
      name = "dotted pin name",
      board = list(), dir = paste0("my.pin/", v), file = "data.txt",
      sql = paste0(
        "REMOVE '@~/my.pin/20240101T000000Z-abc12/' PATTERN = '",
        "^(my\\\\.pin/20240101T000000Z-abc12/)?data\\\\.txt$'"
      )
    )
  )
  for (case in cases) {
    board <- do.call(sf_mock_board, case$board)
    rec <- sf_mock_bind()
    expect_true(
      pinsExtras:::sf_stage_delete_file(board, case$dir, case$file),
      info = case$name
    )
    expect_identical(rec$calls, case$sql, info = case$name)
  }
})

# ---- the guards: nothing is issued at all ------------------------------

test_that("sf_stage_delete_dir refuses to wipe the stage, issuing nothing", {
  board <- sf_mock_board()
  cases <- list(
    list(name = "empty string",   dir = ""),
    # "/" normalises to "" and is the same refusal.
    list(name = "root slash",     dir = "/"),
    list(name = "NA",             dir = NA_character_),
    list(name = "non-string",     dir = 123),
    list(name = "multi-element",  dir = c("a", "b"))
  )
  for (case in cases) {
    rec <- sf_mock_bind()
    expect_error(
      pinsExtras:::sf_stage_delete_dir(board, case$dir),
      class = "pinsExtras_invalid_delete_target",
      info = case$name
    )
    expect_identical(length(rec$calls), 0L, info = case$name)
  }
})

test_that("sf_stage_delete_file refuses a bad file name, issuing nothing", {
  board <- sf_mock_board()
  dir <- paste0("cars/", sf_fixture_version())
  cases <- list(
    list(name = "empty string",  file = ""),
    # A separator would widen the delete beyond the one named file.
    list(name = "contains a slash", file = "a/b"),
    list(name = "NA",            file = NA_character_),
    list(name = "non-string",    file = 123)
  )
  for (case in cases) {
    rec <- sf_mock_bind()
    expect_error(
      pinsExtras:::sf_stage_delete_file(board, dir, case$file),
      class = "pinsExtras_invalid_delete_target",
      info = case$name
    )
    expect_identical(length(rec$calls), 0L, info = case$name)
  }
})

# ---- the three board methods, dispatched through the generics -----------

test_that("pin_delete() lists then removes each name in order", {
  board <- sf_mock_board()
  rec <- sf_mock_bind(
    list = sf_fixture_listing(
      "cars/20240101T000000Z-abc12/data.txt"
    )
  )

  out <- pins::pin_delete(board, "cars")
  expect_identical(out, board)
  expect_identical(rec$calls, c("LIST '@~/cars/'", "REMOVE '@~/cars/'"))

  # The vectorised form interleaves one LIST and one REMOVE per name,
  # in the order given.
  board2 <- sf_mock_board()
  rec2 <- sf_mock_bind(
    list = sf_fixture_listing(
      "a/20240101T000000Z-abc12/data.txt",
      "b/20240101T000000Z-abc12/data.txt"
    )
  )
  pins::pin_delete(board2, c("a", "b"))
  expect_identical(
    rec2$calls,
    c(
      "LIST '@~/a/'", "REMOVE '@~/a/'",
      "LIST '@~/b/'", "REMOVE '@~/b/'"
    )
  )
})

test_that("pin_delete() reports a payload-only pin as not found", {
  # data.txt is the publication marker, so a directory holding only a
  # payload is absent and nothing is removed.
  board <- sf_mock_board()
  rec <- sf_mock_bind(
    list = sf_fixture_listing(
      "cars/20240101T000000Z-abc12/payload.rds"
    )
  )

  expect_error(
    pins::pin_delete(board, "cars"),
    "find pin called"
  )
  expect_length(grep("^LIST ", rec$calls), 1L)
  expect_length(grep("^REMOVE ", rec$calls), 0L)
})

test_that("pin_delete() validates each name before it lists anything", {
  # A supplied ".." or separator would delete the whole board, so the
  # validator runs before the first LIST.
  board <- sf_mock_board()
  for (name in c("", "..", "/", "a/b", "a\\b", ".")) {
    rec <- sf_mock_bind()
    expect_error(
      pins::pin_delete(board, name),
      class = "pinsExtras_invalid_path_segment",
      info = name
    )
    expect_identical(length(rec$calls), 0L, info = name)
  }
})

test_that("pin_delete(character(0)) issues nothing and returns the board", {
  board <- sf_mock_board()
  rec <- sf_mock_bind()

  out <- pins::pin_delete(board, character(0))
  expect_identical(out, board)
  expect_length(rec$calls, 0L)
})

test_that("pin_delete() removes the pin directory, never a sibling", {
  board <- sf_mock_board()
  rec <- sf_mock_bind(
    list = sf_fixture_listing(
      "cars/20240101T000000Z-abc12/data.txt",
      "cars_extra/20240101T000000Z-abc12/data.txt"
    )
  )

  pins::pin_delete(board, "cars")
  expect_identical(
    grep("^REMOVE ", rec$calls, value = TRUE),
    "REMOVE '@~/cars/'"
  )
})

test_that("pin_version_delete() removes a raw directory without listing", {
  # No listing and no existence check: this is the escape hatch for an
  # incomplete version directory that discovery cannot see, so even a
  # version id that will never parse is still removable.
  cases <- list(
    list(name = "well-formed version", version = sf_fixture_version(),
         sql = "REMOVE '@~/cars/20240101T000000Z-abc12/'"),
    list(name = "unparseable version", version = "bogus-def12",
         sql = "REMOVE '@~/cars/bogus-def12/'")
  )
  for (case in cases) {
    board <- sf_mock_board()
    rec <- sf_mock_bind()
    out <- pins::pin_version_delete(board, "cars", case$version)
    expect_identical(out, board, info = case$name)
    expect_identical(rec$calls, case$sql, info = case$name)
    expect_identical(length(grep("^LIST ", rec$calls)), 0L, info = case$name)
  }
})

test_that("pin_version_delete() validates both arguments, issuing nothing", {
  board <- sf_mock_board()
  v <- sf_fixture_version()
  cases <- list(
    list(name = "empty name",     args = list("", v)),
    list(name = "empty version",  args = list("cars", "")),
    list(name = "slash version",  args = list("cars", "/")),
    list(name = "both slashes",   args = list("/", "/")),
    list(name = "dotdot version", args = list("cars", "..")),
    list(name = "dotdot name",    args = list("..", v))
  )
  for (case in cases) {
    rec <- sf_mock_bind()
    expect_error(
      pins::pin_version_delete(board, case$args[[1]], case$args[[2]]),
      class = "pinsExtras_invalid_path_segment",
      info = case$name
    )
    expect_identical(length(rec$calls), 0L, info = case$name)
  }
})

test_that("write_board_manifest_yaml() overwrites the manifest at the root", {
  board <- sf_mock_board()
  rec <- sf_mock_bind()

  pins::write_board_manifest_yaml(board, list(pins = "v1"))
  put <- grep("^PUT ", rec$calls, value = TRUE)
  expect_length(put, 1L)
  # The manifest is the one upload that may replace what is already
  # there, and it targets the board root, whose location carries no
  # trailing slash.
  expect_match(put, "'@~' AUTO_COMPRESS=FALSE OVERWRITE=TRUE", fixed = TRUE)
  expect_match(put, "/_pins.yaml'", fixed = TRUE)
  expect_length(grep("^LIST ", rec$calls), 0L)
  expect_length(grep("^REMOVE ", rec$calls), 0L)
})
