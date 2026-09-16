# The published-pin index, and the read methods that answer from it.
#
# The pure helpers come first with no board and no Snowflake; the public
# methods follow, each driving the real read path through the mock
# transport.

V <- "20240101T000002Z-bbb"

# ---- sf_published_index -------------------------------------------------

test_that("sf_published_index drops every row that does not prove publication", {
  v <- sf_fixture_version()
  cases <- list(
    # The third segment is matched exactly, so a startsWith refactor
    # would silently accept these.
    list(name = "data.txt.bak",  listed = paste0("cars/", v, "/data.txt.bak"),
         prefix = ""),
    list(name = "payload only",  listed = paste0("cars/", v, "/cars.rds"),
         prefix = ""),
    # Segment-count matrix: one, two and four segments all fail.
    list(name = "one segment",   listed = "_pins.yaml",          prefix = ""),
    list(name = "two segments",  listed = "cars/data.txt",       prefix = ""),
    list(name = "four segments", listed = "a/b/c/data.txt",      prefix = ""),
    # Version-parse matrix.
    list(name = "one dash piece", listed = "cars/v1/data.txt",   prefix = ""),
    # "bogus-abc12" splits into two pieces, so the hash parses but the
    # timestamp does not: checking the hash alone is not enough.
    list(name = "unparsed timestamp",
         listed = "cars/bogus-abc12/data.txt", prefix = ""),
    list(name = "three dash pieces",
         listed = "cars/20240101T000000Z-abc12-x/data.txt", prefix = ""),
    # Prefix matrix.
    list(name = "sibling board under a prefix",
         listed = paste0("team-data-archive/cars/", v, "/data.txt"),
         prefix = "team-data"),
    list(name = "board-relative path under a prefix",
         listed = paste0("cars/", v, "/data.txt"), prefix = "team-data")
  )
  for (case in cases) {
    out <- pinsExtras:::sf_published_index(
      sf_fixture_listing(case$listed), case$prefix
    )
    expect_identical(length(out$name), 0L, info = case$name)
  }
})

test_that("sf_published_index sorts by name, then created, then version", {
  # sf_resolve_version()'s "newest" and pin_versions()' order both rest on
  # this single order() call, so all three keys are asserted together.
  by_created <- pinsExtras:::sf_published_index(
    sf_fixture_listing(
      "cars/20240101T000002Z-bbb/data.txt",
      "cars/20240101T000001Z-aaa/data.txt"
    )
  )
  expect_identical(
    by_created$version,
    c("20240101T000001Z-aaa", "20240101T000002Z-bbb")
  )

  # Two writers in the same second produce equal timestamps; the version
  # string breaks the tie.
  by_version <- pinsExtras:::sf_published_index(
    sf_fixture_listing(
      "cars/20240101T000000Z-bbb/data.txt",
      "cars/20240101T000000Z-aaa/data.txt"
    )
  )
  expect_identical(
    by_version$version,
    c("20240101T000000Z-aaa", "20240101T000000Z-bbb")
  )

  by_name <- pinsExtras:::sf_published_index(
    sf_fixture_listing(
      "zebra/20240101T000000Z-z/data.txt",
      "alpha/20240101T000000Z-a/data.txt"
    )
  )
  expect_identical(by_name$name, c("alpha", "zebra"))
})

# ---- the read methods, answering from the index -------------------------

# Metadata of the shape pins writes, for a GET that must parse.
sf_discovery_meta <- function(file = "cars.rds", type = "rds") {
  list(
    api_version = 1L, file = file, file_size = 12,
    created = "20240101", pin_hash = "abc1234567", type = type
  )
}

test_that("each read method issues exactly one scoped LIST", {
  # A board path must not cost an extra request, and the scope differs
  # between the board-wide listing pin_list() needs and the pin-scoped one
  # everything else uses.
  shapes <- list(
    list(
      name = "user stage", args = list(stage = "@~"),
      board_sql = "LIST '@~'", pin_sql = "LIST '@~/cars/'"
    ),
    list(
      name = "named stage",
      args = list(path = "team-data", stage = "@mystage"),
      board_sql = "LIST '@mystage/team-data/'",
      pin_sql = "LIST '@mystage/team-data/cars/'"
    )
  )
  methods <- list(
    list(name = "pin_list", scope = "board",
         run = function(b) pins::pin_list(b), want = "cars"),
    list(name = "pin_exists", scope = "pin",
         run = function(b) pins::pin_exists(b, "cars"), want = TRUE),
    list(name = "pin_versions", scope = "pin",
         run = function(b) pins::pin_versions(b, "cars")$version, want = V)
  )
  for (shape in shapes) {
    for (method in methods) {
      board <- do.call(sf_mock_board, shape$args)
      rec <- sf_mock_bind(
        list = sf_fixture_listing(paste0("cars/", V, "/data.txt"), board = board)
      )
      label <- paste(method$name, "on the", shape$name)

      value <- method$run(board)

      sql <- if (method$scope == "board") shape$board_sql else shape$pin_sql
      expect_identical(rec$calls, sql, info = label)
      expect_identical(value, method$want, info = label)
    }
  }
})

test_that("pin_list never reports the manifest as a pin", {
  board <- sf_mock_board(path = "team-data", stage = "@mystage")
  rec <- sf_mock_bind(list = sf_fixture_listing("_pins.yaml", board = board))

  expect_identical(pins::pin_list(board), character())
})

test_that("pin_versions lists versions ascending with parsed created and hash", {
  board <- sf_mock_board(stage = "@~")
  rec <- sf_mock_bind(
    list = sf_fixture_listing(
      paste0("cars/", V, "/data.txt"),
      paste0("cars/", "20240101T000001Z-aaa", "/data.txt"),
      board = board
    )
  )

  versions <- pins::pin_versions(board, "cars")

  expect_s3_class(versions, "tbl_df")
  expect_identical(versions$version, c("20240101T000001Z-aaa", V))
  expect_false(any(is.na(versions$created)))
  expect_false(any(is.na(versions$hash)))
})

test_that("pin_meta resolves the newest version from one LIST and one GET", {
  # The listing is deliberately out of order: the sort, not the listing,
  # decides which version is newest.
  board <- sf_mock_board(path = "team-data", stage = "@mystage")
  meta <- sf_discovery_meta()
  rec <- sf_mock_bind(
    list = sf_fixture_listing(
      paste0("cars/", V, "/data.txt"),
      paste0("cars/", "20240101T000001Z-aaa", "/data.txt"),
      board = board
    ),
    get = sf_mock_get_files("data.txt" = yaml::as.yaml(meta))
  )

  out <- pins::pin_meta(board, "cars")

  expect_length(grep("^LIST ", rec$calls), 1L)
  expect_length(grep("^GET ", rec$calls), 1L)
  expect_length(grep("^PUT |^REMOVE ", rec$calls), 0L)
  expect_identical(out$local$version, V)
  expect_identical(out$file, "cars.rds")
})

test_that("pin_meta reports an absent pin, an unknown version and a bad one", {
  board <- sf_mock_board(stage = "@~")
  rec <- sf_mock_bind(
    list = sf_fixture_listing(paste0("cars/", V, "/data.txt"), board = board)
  )

  expect_error(
    pins::pin_meta(board, "missing"),
    "Can't find pin called",
    fixed = TRUE
  )
  expect_error(
    pins::pin_meta(board, "cars", version = "nope"),
    "Can't find version",
    fixed = TRUE
  )
  # version is checked with rlang::is_string(), so a number and a
  # multi-element vector share one branch.
  for (bad in list(123, c("a", "b"))) {
    expect_error(
      pins::pin_meta(board, "cars", version = bad),
      "must be a string",
      fixed = TRUE,
      info = paste(class(bad), length(bad))
    )
  }
  expect_length(grep("^GET ", rec$calls), 0L)
})

test_that("pin_meta propagates a malformed-YAML download failure", {
  board <- sf_mock_board(stage = "@~")
  rec <- sf_mock_bind(
    list = sf_fixture_listing(paste0("cars/", V, "/data.txt"), board = board),
    get = sf_mock_get_files("data.txt" = "not: valid: yaml: [")
  )

  expect_error(
    pins::pin_meta(board, "cars"),
    class = "pinsExtras_download_failed"
  )
})

test_that("a listing that raises propagates unchanged instead of an abort", {
  # A transport error must never be swallowed and reported as
  # "Can't find pin".
  board <- sf_mock_board(stage = "@~")
  rec <- sf_mock_bind(list = function(sql, calls) stop("boom"))

  expect_error(pins::pin_list(board), "boom")
})

test_that("pin_fetch issues one LIST and two GETs, and both files land", {
  shapes <- list(
    list(name = "user stage",  args = list(stage = "@~")),
    list(name = "named stage", args = list(path = "team-data", stage = "@mystage"))
  )
  for (shape in shapes) {
    board <- do.call(sf_mock_board, shape$args)
    rec <- sf_mock_bind(
      list = sf_fixture_listing(paste0("cars/", V, "/data.txt"), board = board),
      get = sf_mock_get_files(
        "data.txt" = yaml::as.yaml(sf_discovery_meta()),
        "cars.rds" = "payload bytes"
      )
    )

    out <- pins::pin_fetch(board, "cars")

    expect_identical(length(grep("^LIST ", rec$calls)), 1L, info = shape$name)
    expect_identical(length(grep("^GET ", rec$calls)), 2L, info = shape$name)
    expect_identical(out$local$version, V, info = shape$name)
    expect_identical(out$file, "cars.rds", info = shape$name)
    expect_true(
      fs::file_exists(fs::path(out$local$dir, "data.txt")), info = shape$name
    )
    expect_true(
      fs::file_exists(fs::path(out$local$dir, "cars.rds")), info = shape$name
    )
  }
})

test_that("pin_read round-trips a payload through pins' own entry point", {
  # The only offline test that drives upstream pin_read() end to end and
  # proves the fetched payload actually deserialises. A board path must
  # not cost an extra request, so both shapes are measured.
  shapes <- list(
    list(name = "user stage",  args = list(stage = "@~")),
    list(name = "named stage",
         args = list(path = "team-data", stage = "@mystage"))
  )
  meta <- list(
    api_version = 1L, file = "cars.json", type = "json",
    file_size = 1L, created = "20240101", pin_hash = "abc12"
  )
  withr::local_options(pins.quiet = TRUE)

  for (shape in shapes) {
    board <- do.call(sf_mock_board, shape$args)
    rec <- sf_mock_bind(
      list = sf_fixture_listing(paste0("cars/", V, "/data.txt"), board = board),
      get = sf_mock_get_files(
        "data.txt" = yaml::as.yaml(meta),
        "cars.json" = '[{"a":1,"b":2}]'
      )
    )

    out <- pins::pin_read(board, "cars", version = V)

    expect_identical(length(grep("^LIST ", rec$calls)), 1L, info = shape$name)
    expect_identical(length(grep("^GET ", rec$calls)), 2L, info = shape$name)
    expect_identical(length(grep("^PUT ", rec$calls)), 0L, info = shape$name)
    expect_identical(length(grep("^REMOVE ", rec$calls)), 0L, info = shape$name)
    expect_s3_class(out, "data.frame")
    expect_identical(out$a, 1L, info = shape$name)
    expect_identical(out$b, 2L, info = shape$name)
  }
})
