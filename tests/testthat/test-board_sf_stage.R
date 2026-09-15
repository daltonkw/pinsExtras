# Live integration tests against a real Snowflake stage.
#
# Opt-in only: every block starts with skip_if_no_sf_stage(), which refuses
# to run unless PINS_SF_RUN_INTEGRATION=true. These create and delete real
# objects on a real stage.
#
# Every board comes from sf_stage_test_board(), which gives it a unique
# top-level prefix and registers the centralised teardown on the calling
# frame, so nothing is left behind even if a block fails partway.
#
# Two parts:
#
#   1. pins' internal conformance helpers (test_api_basic, test_api_
#      versioning, test_api_meta and test_api_manifest, verified against
#      pins 1.4.2) cannot run against this board until the package raises
#      pins' classed conditions -- pins_pin_missing,
#      pins_pin_version_missing and pins_check_name -- and until the
#      helpers' local_mocked_bindings(version_name) call, which testthat
#      resolves against TESTTHAT_PKG and so looks for the binding in
#      pinsExtras, is handled. When that lands, call them here behind an
#      expect_identical(packageVersion("pins"), "1.4.2") pin.
#   2. the Snowflake-specific checks no generic suite can make, which are
#      all about whether the exact SQL the offline suite asserts textually
#      is actually accepted by Snowflake, and whether its prefix and
#      pattern semantics match what the code assumes.

# =============================================================================
# Part 2: Snowflake-specific behaviour
# =============================================================================

test_that("the full write/read/version/delete cycle is accepted live", {
  # Not a payload test: the conformance suite already proves round-trips.
  # This proves Snowflake accepts the exact LIST, PUT, GET and REMOVE
  # shapes the offline suite asserts as text, including the manifest's
  # OVERWRITE=TRUE PUT at the board root.
  skip_if_no_sf_stage()

  b <- sf_stage_test_board(sf_stage_test_prefix("cycle"))

  expect_equal(pin_list(b), character(0))

  pin_write(b, 1:3, "numbers", description = "ints")
  expect_equal(pin_list(b), "numbers")
  expect_equal(pin_read(b, "numbers"), 1:3)

  pin_write(b, 4:6, "numbers")
  vers <- pin_versions(b, "numbers")
  expect_equal(nrow(vers), 2)
  expect_equal(pin_read(b, "numbers", version = vers$version[[1]]), 1:3)

  # The raw directory delete: no listing, no existence check, and the
  # surviving version must still resolve as the latest.
  pin_version_delete(b, "numbers", vers$version[[1]])
  after <- pin_versions(b, "numbers")
  expect_equal(nrow(after), 1)
  expect_equal(after$version[[1]], vers$version[[2]])
  expect_equal(pin_read(b, "numbers"), 4:6)

  f <- withr::local_tempfile(fileext = ".txt")
  writeLines("hello-file", f)
  pin_upload(b, paths = f, name = "filepin")
  expect_true(pin_exists(b, "filepin"))
  expect_equal(readLines(pin_download(b, "filepin")), "hello-file")

  write_board_manifest(b)
  listed <- fs::path_file(pinsExtras:::sf_stage_list(b)$name)
  expect_true("_pins.yaml" %in% listed)

  pin_delete(b, "filepin")
  expect_false(pin_exists(b, "filepin"))
})

test_that("a named stage works at its root and under a board path", {
  # The user stage and a named stage are prefixed differently in LIST
  # output, so a named stage is the only place the stage-name strip runs
  # for real.
  skip_if_no_sf_stage()

  stage <- Sys.getenv("PINS_SF_STAGE", "@~")
  if (stage == "@~" || stage == "~") {
    skip("Test requires a named stage, not the user stage")
  }

  # A board at the stage root must never delete its own root, so it owns
  # exactly one uniquely named pin and cleans up only that.
  conn <- sf_stage_test_conn()
  withr::defer(try(DBI::dbDisconnect(conn), silent = TRUE))
  root_board <- board_sf_stage(
    conn = conn,
    stage = stage,
    path = "",
    connect_args = sf_stage_test_args()
  )
  pin_name <- sf_stage_test_prefix("stage-root")
  withr::defer(sf_stage_test_cleanup_pin(root_board, pin_name))

  pin_write(root_board, 1:5, pin_name)
  expect_true(pin_exists(root_board, pin_name))
  expect_true(pin_name %in% pin_list(root_board))
  expect_equal(pin_read(root_board, pin_name), 1:5)
  pin_delete(root_board, pin_name)
  expect_false(pin_exists(root_board, pin_name))

  # The same stage with a non-empty board path: the path is stripped from
  # every listed name, and the board deletes only its own prefix.
  pathed <- sf_stage_test_board(sf_stage_test_prefix("stage-path"))
  pin_write(pathed, 6:10, "pathed")
  expect_equal(pin_list(pathed), "pathed")
  expect_equal(pin_read(pathed, "pathed"), 6:10)
})

test_that("metadata round-trips through a real stage", {
  # Everything pins stores beside the payload, through one real write and
  # one real read: the standard fields, and the caller's own metadata
  # under $user. The URL carries a query string and a fragment, which are
  # the characters most likely to be mangled in transit.
  skip_if_no_sf_stage()

  b <- sf_stage_test_board(sf_stage_test_prefix("metadata"))

  pin_write(b, iris, "meta-pin",
    title = "Iris with complete metadata",
    description = "Quotes \"like this\" and symbols & more",
    tags = c("a", "b", "c", "d", "e"),
    urls = c(
      "https://example.com/x?y=1#z",
      "https://example.com/second"
    ),
    metadata = list(owner = "team", n = 3L)
  )

  meta <- pin_meta(b, "meta-pin")

  expect_equal(meta$title, "Iris with complete metadata")
  expect_true(grepl("quotes \"like this\"", meta$description, ignore.case = TRUE))
  expect_equal(meta$tags, c("a", "b", "c", "d", "e"))
  expect_equal(
    meta$urls,
    c("https://example.com/x?y=1#z", "https://example.com/second")
  )
  expect_equal(meta$user$owner, "team")
  expect_equal(meta$user$n, 3L)

  expect_equal(nrow(pin_read(b, "meta-pin")), 150)
})

test_that("a pin name containing a dot reads back", {
  # A dot is a regex metacharacter, and the GET PATTERN is a Java regex.
  # An unescaped dot would still match here, so this only fails if the
  # escaping is wrong in a way that breaks the match entirely.
  skip_if_no_sf_stage()

  b <- sf_stage_test_board(sf_stage_test_prefix("dotted"))

  pin_write(b, 1:3, "my.pin.name")
  pin_write(b, 4:6, "version.2.0")

  expect_equal(pin_read(b, "my.pin.name"), 1:3)
  expect_equal(pin_read(b, "version.2.0"), 4:6)
  expect_true(all(c("my.pin.name", "version.2.0") %in% pin_list(b)))
})

test_that("deleting cars leaves cars_extra untouched", {
  # The whole reason LIST and REMOVE locations carry a trailing slash.
  # Snowflake matches stage paths by prefix, so without it "cars" would
  # also list and remove its sibling. Only a real stage can prove this.
  skip_if_no_sf_stage()

  b <- sf_stage_test_board(sf_stage_test_prefix("siblings"))

  pin_write(b, 1:3, "cars")
  pin_write(b, 4:6, "cars_extra")
  expect_setequal(pin_list(b), c("cars", "cars_extra"))

  # The pin-scoped listing must not see the sibling either: cars has one
  # version, not two.
  expect_equal(nrow(pin_versions(b, "cars")), 1)

  pin_delete(b, "cars")

  expect_false(pin_exists(b, "cars"))
  expect_true(pin_exists(b, "cars_extra"))
  expect_equal(pin_read(b, "cars_extra"), 4:6)
})

test_that("an unversioned replace leaves exactly one version", {
  # The replace path uploads the new version before removing the old one,
  # so the pin is never absent. Live, this also proves the cleanup REMOVE
  # and its confirming LIST agree with Snowflake's own view.
  skip_if_no_sf_stage()

  b <- sf_stage_test_board(sf_stage_test_prefix("replace"))

  pin_write(b, 1:3, "unversioned-pin", versioned = FALSE)
  first <- pin_versions(b, "unversioned-pin")
  expect_equal(nrow(first), 1)

  Sys.sleep(1) # the version id is timestamped to the second

  pin_write(b, 4:6, "unversioned-pin", versioned = FALSE)
  second <- pin_versions(b, "unversioned-pin")

  expect_equal(nrow(second), 1)
  expect_false(first$version[[1]] == second$version[[1]])
  expect_equal(pin_read(b, "unversioned-pin"), 4:6)
})

test_that("a stage that does not exist fails on the first operation", {
  # Nothing is created, so this board registers no cleanup beyond its own
  # disconnect: the stage it names cannot exist.
  skip_if_no_sf_stage()

  conn <- sf_stage_test_conn()
  withr::defer(try(DBI::dbDisconnect(conn), silent = TRUE))

  stage <- paste0("@nonexistent_stage_", as.integer(Sys.time()))
  b <- board_sf_stage(conn = conn, stage = stage, path = "test")

  cond <- expect_error(pin_list(b), "stage|Stage|does not exist|not exist")
  msg <- cli::ansi_strip(conditionMessage(cond))
  # The message must name the stage that was asked for, so the user can
  # see which one is missing.
  expect_true(grepl(sub("^@", "", stage), msg, fixed = TRUE))
})

test_that("a closed connection fails with reconnection guidance", {
  skip_if_no_sf_stage()

  path_base <- sf_stage_test_prefix("conn-closed")

  # The board's own connection is deliberately closed below, so cleanup
  # cannot use it. The replacement connection and its teardown are
  # registered HERE, before anything can fail: a failing assertion below
  # must not leave the written pin on the stage.
  #
  # Handlers run last in, first out, so the disconnect is registered first
  # in order to run last: cleanup needs the connection still open.
  cleanup_conn <- sf_stage_test_conn()
  cleanup_board <- board_sf_stage(
    conn = cleanup_conn,
    stage = Sys.getenv("PINS_SF_STAGE", "@~"),
    path = path_base,
    connect_args = sf_stage_test_args()
  )
  withr::defer(try(DBI::dbDisconnect(cleanup_conn), silent = TRUE))
  withr::defer(sf_stage_test_cleanup(cleanup_board))

  conn <- sf_stage_test_conn()
  # This test closes `conn` itself, so the deferred disconnect finds it
  # already closed and the driver warns. That is expected, not a failure.
  withr::defer(suppressWarnings(try(DBI::dbDisconnect(conn), silent = TRUE)))
  b <- board_sf_stage(
    conn = conn,
    stage = Sys.getenv("PINS_SF_STAGE", "@~"),
    path = path_base,
    connect_args = sf_stage_test_args()
  )

  pin_write(b, 1:3, "test-pin")
  expect_true(pin_exists(b, "test-pin"))

  DBI::dbDisconnect(conn)

  # Every verb checks connection health before issuing SQL, and the
  # message carries a runnable board_sf_stage() call.
  ops <- list(
    pin_write = function() pin_write(b, 4:6, "another-pin"),
    pin_read = function() pin_read(b, "test-pin"),
    pin_list = function() pin_list(b)
  )
  for (nm in names(ops)) {
    cond <- expect_error(
      ops[[nm]](), "connection|Connection|invalid|closed", info = nm
    )
    msg <- cli::ansi_strip(conditionMessage(cond))
    expect_true(grepl("board_sf_stage(", msg, fixed = TRUE), info = nm)
  }
})
