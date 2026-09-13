# How many Snowflake commands each operation makes.
#
# Every command the adapter issues goes through sf_stage_cmd(); the mock
# transport records each call's SQL and these tests count the verbs. The
# numbers are measured against the current tree, not assumed, and they are
# the same for both board shapes because a board path must not cost an extra
# request. One behaviour per test: the budget for one operation, checked for
# both shapes through the loop, and that the call also returned a sane value.

# Count the Snowflake verbs of one recorded command sequence. Fixed strings
# only: startsWith matches the verb in the first column of the SQL.
budget_counts <- function(calls) {
  c(
    LIST   = sum(startsWith(calls, "LIST")),
    GET    = sum(startsWith(calls, "GET")),
    PUT    = sum(startsWith(calls, "PUT")),
    REMOVE = sum(startsWith(calls, "REMOVE"))
  )
}

# Bind the recording transport, silence the progress messages pin_store() and
# pin_write() print, run one operation, and return both what it produced and
# the SQL it issued.
budget_call <- function(board, rec, fn) {
  testthat::local_mocked_bindings(
    sf_stage_cmd = rec$responder,
    .package = "pinsExtras"
  )
  withr::local_options(pins.quiet = TRUE)
  value <- fn()
  list(value = value, calls = rec$calls)
}

# Board shapes the budget is checked against. A board path must not cost an
# extra request, so every operation must make the same number of each verb
# against the default board and against one with a path and a named stage.
budget_shapes <- function(...) {
  list(
    "`@~` board"                 = sf_mock_board(),
    "`team-data` / `@mystage`"  = sf_mock_board(
      path = "team-data", stage = "@mystage"
    )
  )
}

test_that("pin_list() makes one LIST, zero other verbs, and returns the pins", {
  shapes <- budget_shapes()
  for (nm in names(shapes)) {
    board <- shapes[[nm]]
    rec <- sf_mock_transport(
      list = sf_fixture_listing(
        paste0("cars/", "20240101T000002Z-bbb", "/data.txt"),
        board = board
      )
    )
    res <- budget_call(board, rec, function() pins::pin_list(board))
    expect_identical(budget_counts(res$calls),
      c(LIST = 1L, GET = 0L, PUT = 0L, REMOVE = 0L), info = nm)
    expect_identical(res$value, "cars", info = nm)
  }
})

test_that("pin_exists() makes one LIST and reports a present pin", {
  shapes <- budget_shapes()
  for (nm in names(shapes)) {
    board <- shapes[[nm]]
    rec <- sf_mock_transport(
      list = sf_fixture_listing(
        paste0("cars/", "20240101T000002Z-bbb", "/data.txt"),
        board = board
      )
    )
    res <- budget_call(board, rec, function() pins::pin_exists(board, "cars"))
    expect_identical(budget_counts(res$calls),
      c(LIST = 1L, GET = 0L, PUT = 0L, REMOVE = 0L), info = nm)
    expect_identical(res$value, TRUE, info = nm)
  }
})

test_that("pin_exists() makes one LIST and reports an absent pin", {
  shapes <- budget_shapes()
  for (nm in names(shapes)) {
    board <- shapes[[nm]]
    rec <- sf_mock_transport(list = sf_fixture_listing(board = board))
    res <- budget_call(board, rec, function() pins::pin_exists(board, "cars"))
    expect_identical(budget_counts(res$calls),
      c(LIST = 1L, GET = 0L, PUT = 0L, REMOVE = 0L), info = nm)
    expect_identical(res$value, FALSE, info = nm)
  }
})

test_that("pin_versions() makes one LIST and returns the published versions", {
  shapes <- budget_shapes()
  for (nm in names(shapes)) {
    board <- shapes[[nm]]
    rec <- sf_mock_transport(
      list = sf_fixture_listing(
        paste0("cars/", "20240101T000002Z-bbb", "/data.txt"),
        board = board
      )
    )
    res <- budget_call(board, rec, function() pins::pin_versions(board, "cars"))
    expect_identical(budget_counts(res$calls),
      c(LIST = 1L, GET = 0L, PUT = 0L, REMOVE = 0L), info = nm)
    expect_identical(res$value$version, "20240101T000002Z-bbb", info = nm)
  }
})

test_that("pin_meta() makes one LIST and one GET, and resolves the version", {
  shapes <- budget_shapes()
  for (nm in names(shapes)) {
    board <- shapes[[nm]]
    meta <- list(
      api_version = 1L, file = "cars.rds", file_size = 12L,
      created = "20240101", pin_hash = "abc1234567", type = "rds"
    )
    rec <- sf_mock_transport(
      list = sf_fixture_listing(
        paste0("cars/", "20240101T000002Z-bbb", "/data.txt"),
        board = board
      ),
      get = sf_mock_get_files(
        "data.txt" = yaml::as.yaml(meta)
      )
    )
    res <- budget_call(board, rec, function() pins::pin_meta(board, "cars"))
    expect_identical(budget_counts(res$calls),
      c(LIST = 1L, GET = 1L, PUT = 0L, REMOVE = 0L), info = nm)
    expect_identical(res$value$local$version, "20240101T000002Z-bbb", info = nm)
  }
})

test_that(
  "pin_meta(version = ...) makes one LIST and one GET, and that version",
  {
    shapes <- budget_shapes()
    for (nm in names(shapes)) {
      board <- shapes[[nm]]
      meta <- list(
        api_version = 1L, file = "cars.rds", file_size = 12L,
        created = "20240101", pin_hash = "abc1234567", type = "rds"
      )
      rec <- sf_mock_transport(
        list = sf_fixture_listing(
          paste0("cars/", "20240101T000001Z-aaa", "/data.txt"),
          paste0("cars/", "20240101T000002Z-bbb", "/data.txt"),
          board = board
        ),
        get = sf_mock_get_files(
          "data.txt" = yaml::as.yaml(meta)
        )
      )
      res <- budget_call(
        board, rec,
        function() {
          pins::pin_meta(
            board, "cars", version = "20240101T000002Z-bbb"
          )
        }
      )
      expect_identical(budget_counts(res$calls),
        c(LIST = 1L, GET = 1L, PUT = 0L, REMOVE = 0L), info = nm)
      expect_identical(res$value$local$version,
        "20240101T000002Z-bbb", info = nm)
    }
  }
)

test_that(
  "pin_fetch() makes one LIST and two GETs, and fetches both files",
  {
    shapes <- budget_shapes()
    for (nm in names(shapes)) {
      board <- shapes[[nm]]
      meta <- list(
        api_version = 1L, file = "cars.rds", file_size = 12L,
        created = "20240101", pin_hash = "abc1234567", type = "rds"
      )
      rec <- sf_mock_transport(
        list = sf_fixture_listing(
          paste0("cars/", "20240101T000002Z-bbb", "/data.txt"),
          board = board
        ),
        get = sf_mock_get_files(
          "data.txt" = yaml::as.yaml(meta),
          "cars.rds" = "payload bytes"
        )
      )
      res <- budget_call(board, rec, function() pins::pin_fetch(board, "cars"))
      expect_identical(budget_counts(res$calls),
        c(LIST = 1L, GET = 2L, PUT = 0L, REMOVE = 0L), info = nm)
      expect_identical(res$value$local$version,
        "20240101T000002Z-bbb", info = nm)
    }
  }
)

test_that(
  "pin_read() makes one LIST and two GETs, and reads the payload back",
  {
    shapes <- budget_shapes()
    for (nm in names(shapes)) {
      board <- shapes[[nm]]
      meta <- list(
        api_version = 1L, file = "cars.json", type = "json",
        file_size = 1L, created = "20240101", pin_hash = "abc12"
      )
      rec <- sf_mock_transport(
        list = sf_fixture_listing(
          paste0("cars/", "20240101T000002Z-bbb", "/data.txt"),
          board = board
        ),
        get = sf_mock_get_files(
          "data.txt" = yaml::as.yaml(meta),
          "cars.json" = '[{"a":1,"b":2}]'
        )
      )
      res <- budget_call(
        board, rec,
        function() {
          pins::pin_read(
            board, "cars", version = "20240101T000002Z-bbb"
          )
        }
      )
      expect_identical(budget_counts(res$calls),
        c(LIST = 1L, GET = 2L, PUT = 0L, REMOVE = 0L), info = nm)
      expect_s3_class(res$value, "data.frame")
      expect_identical(res$value$a, 1L, info = nm)
      expect_identical(res$value$b, 2L, info = nm)
    }
  }
)

test_that("pin_delete() makes one LIST and one REMOVE, and returns the board", {
  shapes <- budget_shapes()
  for (nm in names(shapes)) {
    board <- shapes[[nm]]
    rec <- sf_mock_transport(
      list = sf_fixture_listing(
        "cars/20240101T000000Z-abc12/data.txt",
        board = board
      )
    )
    res <- budget_call(
      board, rec,
      function() pins::pin_delete(board, "cars")
    )
    expect_identical(budget_counts(res$calls),
      c(LIST = 1L, GET = 0L, PUT = 0L, REMOVE = 1L), info = nm)
    expect_identical(res$value, board, info = nm)
  }
})

test_that(
  "pin_version_delete() makes one REMOVE and no listing, and returns the board",
  {
    shapes <- budget_shapes()
    for (nm in names(shapes)) {
      board <- shapes[[nm]]
      rec <- sf_mock_transport()
      res <- budget_call(
        board, rec,
        function() {
          pins::pin_version_delete(
            board, "cars", "20240101T000000Z-abc12"
          )
        }
      )
      expect_identical(budget_counts(res$calls),
        c(LIST = 0L, GET = 0L, PUT = 0L, REMOVE = 1L), info = nm)
      expect_identical(res$value, board, info = nm)
    }
  }
)

test_that(
  "write_board_manifest_yaml() makes one PUT and no other verbs",
  {
    shapes <- budget_shapes()
    for (nm in names(shapes)) {
      board <- shapes[[nm]]
      rec <- sf_mock_transport()
      res <- budget_call(
        board, rec,
        function() pins::write_board_manifest_yaml(board, list(pins = "v1"))
      )
      expect_identical(budget_counts(res$calls),
        c(LIST = 0L, GET = 0L, PUT = 1L, REMOVE = 0L), info = nm)
      expect_identical(res$value, TRUE, info = nm)
    }
  }
)

test_that(
  "pin_store() writes a brand-new versioned pin with one LIST and two PUT",
  {
    meta <- list(
      api_version = 1L, file = "cars.rds", file_size = 12L,
      created = "20240102T000000Z", pin_hash = "abcdef0123456789",
      type = "json"
    )
    path <- file.path(withr::local_tempdir(), "cars.rds")
    writeLines("payload", path)

    shapes <- budget_shapes()
    for (nm in names(shapes)) {
      board <- shapes[[nm]]
      rec <- sf_mock_transport(list = sf_fixture_listing(board = board))
      res <- budget_call(
        board, rec,
        function() {
          pinsExtras:::pin_store.pins_board_sf_stage(
            board, "cars", path, meta, versioned = TRUE, x = NULL
          )
        }
      )
      expect_identical(budget_counts(res$calls),
        c(LIST = 1L, GET = 0L, PUT = 2L, REMOVE = 0L), info = nm)
      expect_identical(res$value, "cars", info = nm)
    }
  }
)

test_that(
  "pin_store() on an existing versioned pin still makes one LIST and two PUT",
  {
    meta <- list(
      api_version = 1L, file = "cars.rds", file_size = 12L,
      created = "20240102T000000Z", pin_hash = "abcdef0123456789",
      type = "json"
    )
    path <- file.path(withr::local_tempdir(), "cars.rds")
    writeLines("payload", path)

    shapes <- budget_shapes()
    for (nm in names(shapes)) {
      board <- shapes[[nm]]
      rec <- sf_mock_transport(
        list = sf_fixture_listing(
          "cars/20240101T000001Z-aaa/data.txt",
          board = board
        )
      )
      res <- budget_call(
        board, rec,
        function() {
          pinsExtras:::pin_store.pins_board_sf_stage(
            board, "cars", path, meta, versioned = TRUE, x = NULL
          )
        }
      )
      expect_identical(budget_counts(res$calls),
        c(LIST = 1L, GET = 0L, PUT = 2L, REMOVE = 0L), info = nm)
      expect_identical(res$value, "cars", info = nm)
    }
  }
)

test_that(
  "pin_store() unversioned rewrite that cannot confirm cleanup makes 3 LIST, 2 PUT, 1 REMOVE and warns",
  {
    meta <- list(
      api_version = 1L, file = "cars.rds", file_size = 12L,
      created = "20240102T000000Z", pin_hash = "abcdef0123456789",
      type = "json"
    )
    path <- file.path(withr::local_tempdir(), "cars.rds")
    writeLines("payload", path)

    shapes <- budget_shapes()
    for (nm in names(shapes)) {
      board <- shapes[[nm]]
      # Every listing still shows the old version, so the cleanup check cannot
      # confirm the old payload is gone and pin_store() warns.
      rec <- sf_mock_transport(
        list = function(sql, calls) {
          sf_fixture_listing(
            paste0("cars/", "20240101T000001Z-oldv", "/data.txt"),
            board = board
          )
        }
      )
      expect_warning(
        res <- budget_call(
          board, rec,
          function() {
            pinsExtras:::pin_store.pins_board_sf_stage(
              board, "cars", path, meta, versioned = FALSE, x = NULL
            )
          }
        ),
        class = "pinsExtras_cleanup_incomplete"
      )
      expect_identical(res$value, "cars", info = nm)
      expect_identical(budget_counts(res$calls),
        c(LIST = 3L, GET = 0L, PUT = 2L, REMOVE = 1L), info = nm)
      # The block below measures the successful path, where the listing
      # reflects the REMOVE and cleanup is confirmed, so no warning fires.
    }
  }
)

test_that(
  "pin_store() unversioned rewrite that confirms cleanup makes 3 LIST, 2 PUT, 2 REMOVE with no warning",
  {
    meta <- list(
      api_version = 1L, file = "cars.rds", file_size = 12L,
      created = "20240102T000000Z", pin_hash = "abcdef0123456789",
      type = "json"
    )
    oldv <- "20240101T000001Z-oldv"
    path <- file.path(withr::local_tempdir(), "cars.rds")
    writeLines("payload", path)

    shapes <- budget_shapes()
    for (nm in names(shapes)) {
      board <- shapes[[nm]]
      # The responder drops the old version's rows once the marker REMOVE has
      # gone out, so the cleanup check can confirm the deletion and no warning
      # fires. Only the REMOVE that deletes the marker carries a PATTERN clause;
      # the bare directory REMOVE does not, so PATTERN is the trigger.
      rec <- sf_mock_transport(
        list = function(sql, calls) {
          marker_gone <- any(
            grepl("^REMOVE ", calls) &
              grepl("PATTERN ", calls, fixed = TRUE)
          )
          if (marker_gone) {
            sf_fixture_listing(board = board)
          } else {
            sf_fixture_listing(
              paste0("cars/", oldv, "/data.txt"), board = board
            )
          }
        }
      )
      expect_no_warning(
        res <- budget_call(
          board, rec,
          function() {
            pinsExtras:::pin_store.pins_board_sf_stage(
              board, "cars", path, meta, versioned = FALSE, x = NULL
            )
          }
        )
      )
      expect_identical(res$value, "cars", info = nm)
      expect_identical(budget_counts(res$calls),
        c(LIST = 3L, GET = 0L, PUT = 2L, REMOVE = 2L), info = nm)
    }
  }
)

test_that(
  "pin_write() on a new pin makes 2 LIST and 2 PUT (pins' own lookup included)",
  {
    shapes <- budget_shapes()
    for (nm in names(shapes)) {
      board <- shapes[[nm]]
      rec <- sf_mock_transport(list = sf_fixture_listing(board = board))
      res <- budget_call(
        board, rec,
        function() pins::pin_write(board, data.frame(x = 1), "cars")
      )
      expect_identical(budget_counts(res$calls),
        c(LIST = 2L, GET = 0L, PUT = 2L, REMOVE = 0L), info = nm)
      expect_identical(res$value, "cars", info = nm)
    }
  }
)

test_that(
  "pin_write() on an existing pin makes 2 LIST, 1 GET, and 2 PUT",
  {
    shapes <- budget_shapes()
    for (nm in names(shapes)) {
      board <- shapes[[nm]]
      rec <- sf_mock_transport(
        list = sf_fixture_listing(
          "cars/20240101T000001Z-aaa/data.txt",
          board = board
        )
      )
      res <- budget_call(
        board, rec,
        function() pins::pin_write(board, data.frame(x = 1), "cars")
      )
      expect_identical(budget_counts(res$calls),
        c(LIST = 2L, GET = 1L, PUT = 2L, REMOVE = 0L), info = nm)
      expect_identical(res$value, "cars", info = nm)
    }
  }
)

test_that(
  "pin_write(force_identical_write = TRUE) makes one LIST and 2 PUT",
  {
    shapes <- budget_shapes()
    for (nm in names(shapes)) {
      board <- shapes[[nm]]
      rec <- sf_mock_transport(list = sf_fixture_listing(board = board))
      res <- budget_call(
        board, rec,
        function() {
          pins::pin_write(
            board, data.frame(x = 1), "cars",
            force_identical_write = TRUE
          )
        }
      )
      expect_identical(budget_counts(res$calls),
        c(LIST = 1L, GET = 0L, PUT = 2L, REMOVE = 0L), info = nm)
      expect_identical(res$value, "cars", info = nm)
    }
  }
)
