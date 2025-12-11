# =============================================================================
# Basic CRUD Operations (Happy Path)
# =============================================================================

test_that("Snowflake stage board end-to-end", {
  skip_if_no_sf_stage()

  path_base <- paste0("pins-sf-testthat-", as.integer(Sys.time()))
  b <- sf_stage_test_board(path_base)
  withr::defer(pinsExtras:::sf_stage_delete(b, path_base))

  expect_equal(pin_list(b), character(0))

  pin_write(b, 1:3, "numbers", description = "ints")
  expect_equal(pin_list(b), "numbers")
  expect_equal(pin_read(b, "numbers"), 1:3)

  pin_write(b, 4:6, "numbers")
  vers <- pin_versions(b, "numbers")
  expect_equal(nrow(vers), 2)
  expect_equal(pin_read(b, "numbers", version = vers$version[[1]]), 1:3)

  f <- withr::local_tempfile(fileext = ".txt")
  writeLines("hello-file", f)
  pin_upload(b, paths = f, name = "filepin")
  expect_true(pin_exists(b, "filepin"))
  dl <- pin_download(b, "filepin")
  expect_equal(readLines(dl), "hello-file")

  write_board_manifest(b)
  expect_true(pinsExtras:::sf_stage_exists(b, "_pins.yaml"))

  pin_delete(b, "filepin")
  expect_false(pin_exists(b, "filepin"))
})

# =============================================================================
# Error Handling
# =============================================================================

test_that("pin_read errors on non-existent pin", {
  skip_if_no_sf_stage()

  path_base <- paste0("pins-sf-err-", as.integer(Sys.time()))
  b <- sf_stage_test_board(path_base)
  withr::defer(pinsExtras:::sf_stage_delete(b, path_base))

  expect_error(
    pin_read(b, "nonexistent-pin"),
    "Can't find pin"
  )
})

test_that("pin_read errors on non-existent version", {
  skip_if_no_sf_stage()

  path_base <- paste0("pins-sf-err-ver-", as.integer(Sys.time()))
  b <- sf_stage_test_board(path_base)
  withr::defer(pinsExtras:::sf_stage_delete(b, path_base))

  pin_write(b, 1:3, "mypin")

  expect_error(
    pin_read(b, "mypin", version = "20991231T235959Z-zzzzz"),
    "Can't find version"
  )
})

test_that("pin_delete errors on non-existent pin", {
  skip_if_no_sf_stage()

  path_base <- paste0("pins-sf-err-del-", as.integer(Sys.time()))
  b <- sf_stage_test_board(path_base)
  withr::defer(pinsExtras:::sf_stage_delete(b, path_base))

  expect_error(
    pin_delete(b, "nonexistent-pin"),
    "Can't find pin"
  )
})

test_that("pin_versions errors on non-existent pin", {
  skip_if_no_sf_stage()

  path_base <- paste0("pins-sf-err-vers-", as.integer(Sys.time()))
  b <- sf_stage_test_board(path_base)
  withr::defer(pinsExtras:::sf_stage_delete(b, path_base))

  expect_error(
    pin_versions(b, "nonexistent-pin"),
    "Can't find pin"
  )
})

test_that("pin_meta errors on non-existent pin", {
  skip_if_no_sf_stage()

  path_base <- paste0("pins-sf-err-meta-", as.integer(Sys.time()))
  b <- sf_stage_test_board(path_base)
  withr::defer(pinsExtras:::sf_stage_delete(b, path_base))

  expect_error(
    pin_meta(b, "nonexistent-pin"),
    "Can't find pin"
  )
})

# =============================================================================
# Versioning Behavior
# =============================================================================

test_that("versioned=TRUE creates multiple versions", {
  skip_if_no_sf_stage()

  path_base <- paste0("pins-sf-ver-true-", as.integer(Sys.time()))
  b <- sf_stage_test_board(path_base)
  withr::defer(pinsExtras:::sf_stage_delete(b, path_base))

  # Write three versions
  pin_write(b, 1:3, "versioned-pin", versioned = TRUE)
  Sys.sleep(1)  # Ensure different timestamps
  pin_write(b, 4:6, "versioned-pin", versioned = TRUE)
  Sys.sleep(1)
  pin_write(b, 7:9, "versioned-pin", versioned = TRUE)

  vers <- pin_versions(b, "versioned-pin")
  expect_equal(nrow(vers), 3)

  # Versions should be ordered (most recent first based on created time)
  expect_true(all(!is.na(vers$created)))

  # Can read each version
  expect_equal(pin_read(b, "versioned-pin", version = vers$version[[1]]), 1:3)
  expect_equal(pin_read(b, "versioned-pin", version = vers$version[[2]]), 4:6)
  expect_equal(pin_read(b, "versioned-pin", version = vers$version[[3]]), 7:9)

  # Default read gets most recent (last written)
  expect_equal(pin_read(b, "versioned-pin"), 7:9)
})

test_that("versioned=FALSE replaces existing pin", {
  skip_if_no_sf_stage()

  path_base <- paste0("pins-sf-ver-false-", as.integer(Sys.time()))
  b <- sf_stage_test_board(path_base)
  withr::defer(pinsExtras:::sf_stage_delete(b, path_base))

  # Write initial version
 pin_write(b, 1:3, "unversioned-pin", versioned = FALSE)
  vers1 <- pin_versions(b, "unversioned-pin")
  expect_equal(nrow(vers1), 1)

  Sys.sleep(1)  # Ensure different timestamp

  # Write again with versioned=FALSE - should replace
  pin_write(b, 4:6, "unversioned-pin", versioned = FALSE)
  vers2 <- pin_versions(b, "unversioned-pin")
  expect_equal(nrow(vers2), 1)

  # Should have the new data
  expect_equal(pin_read(b, "unversioned-pin"), 4:6)

  # Version should be different
  expect_false(vers1$version[[1]] == vers2$version[[1]])
})

test_that("pin_version_delete removes specific version", {
  skip_if_no_sf_stage()

  path_base <- paste0("pins-sf-ver-del-", as.integer(Sys.time()))
  b <- sf_stage_test_board(path_base)
  withr::defer(pinsExtras:::sf_stage_delete(b, path_base))

  # Create two versions
  pin_write(b, 1:3, "multi-ver")
  Sys.sleep(1)
  pin_write(b, 4:6, "multi-ver")

  vers <- pin_versions(b, "multi-ver")
  expect_equal(nrow(vers), 2)

  # Delete the first (older) version
  pin_version_delete(b, "multi-ver", vers$version[[1]])

  vers_after <- pin_versions(b, "multi-ver")
  expect_equal(nrow(vers_after), 1)
  expect_equal(vers_after$version[[1]], vers$version[[2]])

  # Can still read the remaining version
  expect_equal(pin_read(b, "multi-ver"), 4:6)
})

test_that("pin_exists returns correct boolean", {
  skip_if_no_sf_stage()

  path_base <- paste0("pins-sf-exists-", as.integer(Sys.time()))
  b <- sf_stage_test_board(path_base)
  withr::defer(pinsExtras:::sf_stage_delete(b, path_base))

  expect_false(pin_exists(b, "not-yet"))

  pin_write(b, 1:3, "not-yet")
  expect_true(pin_exists(b, "not-yet"))

  pin_delete(b, "not-yet")
  expect_false(pin_exists(b, "not-yet"))
})

# =============================================================================
# Metadata
# =============================================================================

test_that("pin_meta returns correct structure", {
  skip_if_no_sf_stage()

  path_base <- paste0("pins-sf-meta-", as.integer(Sys.time()))
  b <- sf_stage_test_board(path_base)
  withr::defer(pinsExtras:::sf_stage_delete(b, path_base))

  pin_write(b, 1:3, "meta-test",
    title = "Test Pin",
    description = "A test pin for metadata",
    tags = c("test", "example")
  )

  meta <- pin_meta(b, "meta-test")

  expect_s3_class(meta, "pins_meta")
  expect_equal(meta$name, "meta-test")
  expect_equal(meta$title, "Test Pin")
  expect_equal(meta$description, "A test pin for metadata")
  expect_equal(meta$type, "rds")
  expect_true(!is.null(meta$pin_hash))
  expect_true(!is.null(meta$file))
  expect_true(!is.null(meta$local$dir))
  expect_true(!is.null(meta$local$version))
})

test_that("custom metadata round-trips correctly", {
  skip_if_no_sf_stage()

  path_base <- paste0("pins-sf-custom-meta-", as.integer(Sys.time()))
  b <- sf_stage_test_board(path_base)
  withr::defer(pinsExtras:::sf_stage_delete(b, path_base))

  custom_meta <- list(
    author = "Test Author",
    source = "Unit Test",
    numeric_field = 42,
    nested = list(a = 1, b = 2)
  )

  pin_write(b, 1:3, "custom-meta-test", metadata = custom_meta)
  meta <- pin_meta(b, "custom-meta-test")

  expect_equal(meta$user$author, "Test Author")
  expect_equal(meta$user$source, "Unit Test")
  expect_equal(meta$user$numeric_field, 42)
  expect_equal(meta$user$nested$a, 1)
  expect_equal(meta$user$nested$b, 2)
})

# =============================================================================
# board_deparse
# =============================================================================

test_that("board_deparse errors without connect_args", {
  skip_if_no_sf_stage()

  conn <- sf_stage_test_conn()
  withr::defer(DBI::dbDisconnect(conn))

  # Create board without connect_args
  b <- board_sf_stage(
    conn = conn,
    stage = "@~",
    path = "test-no-deparse",
    connect_args = NULL
  )

  expect_error(
    board_deparse(b),
    "connect_args"
  )
})

test_that("board_deparse returns valid expression", {
  skip_if_no_sf_stage()

  path_base <- paste0("pins-sf-deparse-", as.integer(Sys.time()))
  b <- sf_stage_test_board(path_base)
  withr::defer(pinsExtras:::sf_stage_delete(b, path_base))

  expr <- board_deparse(b)

  expect_true(is.call(expr))
  expect_true(grepl("board_sf_stage", deparse(expr)[[1]]))
})

# =============================================================================
# Named Stage with Empty Path
# =============================================================================

test_that("board works with named stage and empty path", {
  skip_if_no_sf_stage()

 # Skip if using user stage (@~) - this test is for named stages
  stage <- Sys.getenv("PINS_SF_STAGE", "@~")
  if (stage == "@~" || stage == "~") {
    skip("Test requires a named stage, not user stage")
  }

  conn <- sf_stage_test_conn()
  withr::defer(DBI::dbDisconnect(conn))

  # Create board with empty path (pins stored at stage root)
  b <- board_sf_stage(
    conn = conn,
    stage = stage,
    path = "",
    connect_args = sf_stage_test_args()
  )

  pin_name <- paste0("empty-path-test-", as.integer(Sys.time()))
  withr::defer(pinsExtras:::sf_stage_delete(b, pin_name))

  # Write and read should work with empty path
  pin_write(b, 1:5, pin_name)
  expect_true(pin_exists(b, pin_name))
  expect_true(pin_name %in% pin_list(b))
  expect_equal(pin_read(b, pin_name), 1:5)

  # Clean up
  pin_delete(b, pin_name)
  expect_false(pin_exists(b, pin_name))
})
