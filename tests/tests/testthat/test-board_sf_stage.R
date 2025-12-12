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

# =============================================================================
# Edge Cases: Pin Names with Special Characters
# =============================================================================

test_that("pin names with hyphens and underscores work", {
  skip_if_no_sf_stage()

  path_base <- paste0("pins-sf-names-", as.integer(Sys.time()))
  b <- sf_stage_test_board(path_base)
  withr::defer(pinsExtras:::sf_stage_delete(b, path_base))

  # Hyphens and underscores are common and should work fine
  pin_write(b, 1:3, "my-pin-name")
  pin_write(b, 4:6, "my_pin_name")
  pin_write(b, 7:9, "my-pin_mixed-name_123")

  expect_true(all(c("my-pin-name", "my_pin_name", "my-pin_mixed-name_123") %in% pin_list(b)))
  expect_equal(pin_read(b, "my-pin-name"), 1:3)
  expect_equal(pin_read(b, "my_pin_name"), 4:6)
  expect_equal(pin_read(b, "my-pin_mixed-name_123"), 7:9)
})

test_that("pin names with dots work", {
  skip_if_no_sf_stage()

  path_base <- paste0("pins-sf-dots-", as.integer(Sys.time()))
  b <- sf_stage_test_board(path_base)
  withr::defer(pinsExtras:::sf_stage_delete(b, path_base))

  # Dots should work (but not "data.txt" which is reserved)
  pin_write(b, 1:3, "my.pin.name")
  pin_write(b, 4:6, "version.2.0")

  expect_true(all(c("my.pin.name", "version.2.0") %in% pin_list(b)))
  expect_equal(pin_read(b, "my.pin.name"), 1:3)
  expect_equal(pin_read(b, "version.2.0"), 4:6)
})

test_that("pin names starting with numbers work", {
  skip_if_no_sf_stage()

  path_base <- paste0("pins-sf-numbers-", as.integer(Sys.time()))
  b <- sf_stage_test_board(path_base)
  withr::defer(pinsExtras:::sf_stage_delete(b, path_base))

  pin_write(b, 1:3, "123-data")
  pin_write(b, 4:6, "2024_results")

  expect_true(all(c("123-data", "2024_results") %in% pin_list(b)))
  expect_equal(pin_read(b, "123-data"), 1:3)
  expect_equal(pin_read(b, "2024_results"), 4:6)
})

# =============================================================================
# Edge Cases: Empty Data and NULL Values
# =============================================================================

test_that("empty data frame can be pinned", {
  skip_if_no_sf_stage()

  path_base <- paste0("pins-sf-empty-", as.integer(Sys.time()))
  b <- sf_stage_test_board(path_base)
  withr::defer(pinsExtras:::sf_stage_delete(b, path_base))

  # Empty data frame (0 rows)
  empty_df <- data.frame(x = integer(), y = character())
  pin_write(b, empty_df, "empty-df")

  result <- pin_read(b, "empty-df")
  expect_s3_class(result, "data.frame")
  expect_equal(nrow(result), 0)
  expect_equal(names(result), c("x", "y"))
})

test_that("data frame with NA values can be pinned", {
  skip_if_no_sf_stage()

  path_base <- paste0("pins-sf-na-", as.integer(Sys.time()))
  b <- sf_stage_test_board(path_base)
  withr::defer(pinsExtras:::sf_stage_delete(b, path_base))

  df_with_na <- data.frame(
    x = c(1, NA, 3),
    y = c("a", "b", NA)
  )
  pin_write(b, df_with_na, "na-df")

  result <- pin_read(b, "na-df")
  expect_equal(nrow(result), 3)
  expect_true(is.na(result$x[[2]]))
  expect_true(is.na(result$y[[3]]))
})

test_that("zero-length vectors can be pinned", {
  skip_if_no_sf_stage()

  path_base <- paste0("pins-sf-zero-", as.integer(Sys.time()))
  b <- sf_stage_test_board(path_base)
  withr::defer(pinsExtras:::sf_stage_delete(b, path_base))

  pin_write(b, integer(), "empty-int")
  pin_write(b, character(), "empty-char")

  expect_equal(pin_read(b, "empty-int"), integer())
  expect_equal(pin_read(b, "empty-char"), character())
})

# =============================================================================
# Edge Cases: Empty Board Operations
# =============================================================================

test_that("empty board operations work correctly", {
  skip_if_no_sf_stage()

  path_base <- paste0("pins-sf-empty-board-", as.integer(Sys.time()))
  b <- sf_stage_test_board(path_base)
  withr::defer(pinsExtras:::sf_stage_delete(b, path_base))

  # Operations on brand new empty board
  expect_equal(pin_list(b), character(0))
  expect_false(pin_exists(b, "nonexistent"))

  # Add and remove a pin - board should be empty again
  pin_write(b, 1:3, "temp")
  expect_equal(pin_list(b), "temp")

  pin_delete(b, "temp")
  expect_equal(pin_list(b), character(0))
  expect_false(pin_exists(b, "temp"))
})

# =============================================================================
# Edge Cases: Single Value Data
# =============================================================================

test_that("single value data can be pinned", {
  skip_if_no_sf_stage()

  path_base <- paste0("pins-sf-single-", as.integer(Sys.time()))
  b <- sf_stage_test_board(path_base)
  withr::defer(pinsExtras:::sf_stage_delete(b, path_base))

  # Single numeric value
  pin_write(b, 42, "single-number")
  expect_equal(pin_read(b, "single-number"), 42)

  # Single string
  pin_write(b, "hello", "single-string")
  expect_equal(pin_read(b, "single-string"), "hello")

  # Single logical
  pin_write(b, TRUE, "single-bool")
  expect_equal(pin_read(b, "single-bool"), TRUE)
})

# =============================================================================
# Edge Cases: Complex Nested Structures
# =============================================================================

test_that("complex nested structures can be pinned", {
  skip_if_no_sf_stage()

  path_base <- paste0("pins-sf-nested-", as.integer(Sys.time()))
  b <- sf_stage_test_board(path_base)
  withr::defer(pinsExtras:::sf_stage_delete(b, path_base))

  # Deeply nested list with mixed types
  complex_data <- list(
    level1 = list(
      level2 = list(
        level3 = list(
          numbers = 1:5,
          strings = c("a", "b", "c"),
          df = data.frame(x = 1:3, y = letters[1:3]),
          logical = c(TRUE, FALSE, TRUE)
        ),
        another = "value"
      ),
      simple = 42
    ),
    top_level = "data"
  )

  pin_write(b, complex_data, "complex-nested")
  result <- pin_read(b, "complex-nested")

  # Verify structure is preserved
  expect_equal(result$level1$level2$level3$numbers, 1:5)
  expect_equal(result$level1$level2$level3$strings, c("a", "b", "c"))
  expect_s3_class(result$level1$level2$level3$df, "data.frame")
  expect_equal(result$level1$simple, 42)
  expect_equal(result$top_level, "data")
})

# =============================================================================
# Edge Cases: Metadata with Special Characters
# =============================================================================

test_that("metadata with special characters works", {
  skip_if_no_sf_stage()

  path_base <- paste0("pins-sf-meta-special-", as.integer(Sys.time()))
  b <- sf_stage_test_board(path_base)
  withr::defer(pinsExtras:::sf_stage_delete(b, path_base))

  # Unicode, quotes, and special characters in metadata
  pin_write(b, 1:3, "meta-special",
    title = "Test with 'quotes' and \"double quotes\"",
    description = "Unicode: \u00e9\u00e8\u00ea (accents) and emoji: \U0001f44d",
    tags = c("tag-with-hyphen", "tag_with_underscore", "tag.with.dot")
  )

  meta <- pin_meta(b, "meta-special")
  expect_true(grepl("'quotes'", meta$title))
  expect_true(grepl("\"double quotes\"", meta$title))
  expect_true(grepl("\u00e9\u00e8\u00ea", meta$description))
  expect_true(all(c("tag-with-hyphen", "tag_with_underscore", "tag.with.dot") %in% meta$tags))
})

# =============================================================================
# Edge Cases: Many Versions
# =============================================================================

test_that("pins with many versions work correctly", {
  skip_if_no_sf_stage()

  path_base <- paste0("pins-sf-many-vers-", as.integer(Sys.time()))
  b <- sf_stage_test_board(path_base)
  withr::defer(pinsExtras:::sf_stage_delete(b, path_base))

  # Create 12 versions
  for (i in 1:12) {
    pin_write(b, i * 10 + (1:3), "many-versions", versioned = TRUE)
    if (i < 12) Sys.sleep(1)  # Ensure different timestamps
  }

  vers <- pin_versions(b, "many-versions")
  expect_equal(nrow(vers), 12)

  # All versions should have valid timestamps
  expect_true(all(!is.na(vers$created)))

  # Can read first and last versions
  expect_equal(pin_read(b, "many-versions", version = vers$version[[1]]), 1:3)
  expect_equal(pin_read(b, "many-versions", version = vers$version[[12]]), 121:123)

  # Delete a middle version (version 6)
  pin_version_delete(b, "many-versions", vers$version[[6]])

  vers_after <- pin_versions(b, "many-versions")
  expect_equal(nrow(vers_after), 11)
  expect_false(vers$version[[6]] %in% vers_after$version)
})

# =============================================================================
# Edge Cases: Very Long Pin Names
# =============================================================================

test_that("very long pin names work", {
  skip_if_no_sf_stage()

  path_base <- paste0("pins-sf-long-", as.integer(Sys.time()))
  b <- sf_stage_test_board(path_base)
  withr::defer(pinsExtras:::sf_stage_delete(b, path_base))

  # Test increasingly long names to find limits
  # Snowflake typically handles 255 character identifiers
  long_name_100 <- paste(rep("a", 100), collapse = "")
  long_name_200 <- paste(rep("b", 200), collapse = "")

  # 100 character name
  pin_write(b, 1:3, long_name_100)
  expect_true(long_name_100 %in% pin_list(b))
  expect_equal(pin_read(b, long_name_100), 1:3)

  # 200 character name
  pin_write(b, 4:6, long_name_200)
  expect_true(long_name_200 %in% pin_list(b))
  expect_equal(pin_read(b, long_name_200), 4:6)
})

# =============================================================================
# Edge Cases: Concurrent Writes
# =============================================================================

test_that("concurrent writes with versioned=TRUE both succeed", {
  skip_if_no_sf_stage()

  path_base <- paste0("pins-sf-concurrent-", as.integer(Sys.time()))
  b <- sf_stage_test_board(path_base)
  withr::defer(pinsExtras:::sf_stage_delete(b, path_base))

  # With versioned=TRUE, both writes should create separate versions
  # We can't truly run them concurrently in a single R session,
  # but we can write them in quick succession to test the behavior
  pin_write(b, 1:3, "concurrent-pin", versioned = TRUE)
  pin_write(b, 4:6, "concurrent-pin", versioned = TRUE)

  vers <- pin_versions(b, "concurrent-pin")
  expect_equal(nrow(vers), 2)

  # Both versions should be readable
  expect_equal(pin_read(b, "concurrent-pin", version = vers$version[[1]]), 1:3)
  expect_equal(pin_read(b, "concurrent-pin", version = vers$version[[2]]), 4:6)
})

test_that("concurrent writes with versioned=FALSE last write wins", {
  skip_if_no_sf_stage()

  path_base <- paste0("pins-sf-concurrent-false-", as.integer(Sys.time()))
  b <- sf_stage_test_board(path_base)
  withr::defer(pinsExtras:::sf_stage_delete(b, path_base))

  # With versioned=FALSE, second write should replace the first
  pin_write(b, 1:3, "replace-pin", versioned = FALSE)
  Sys.sleep(1)  # Small delay to ensure different timestamps
  pin_write(b, 4:6, "replace-pin", versioned = FALSE)

  vers <- pin_versions(b, "replace-pin")
  expect_equal(nrow(vers), 1)

  # Should have the last written data
  expect_equal(pin_read(b, "replace-pin"), 4:6)
})

# =============================================================================
# Error Handling: Connection Failures
# =============================================================================

test_that("operations fail gracefully when connection is closed", {
  skip_if_no_sf_stage()

  path_base <- paste0("pins-sf-conn-closed-", as.integer(Sys.time()))

  # Create board and write a pin
  conn <- sf_stage_test_conn()
  b <- board_sf_stage(
    conn = conn,
    stage = Sys.getenv("PINS_SF_STAGE", "@~"),
    path = path_base,
    connect_args = sf_stage_test_args()
  )

  pin_write(b, 1:3, "test-pin")
  expect_true(pin_exists(b, "test-pin"))

  # Close the connection
  DBI::dbDisconnect(conn)

  # Operations should fail with meaningful errors
  expect_error(
    pin_write(b, 4:6, "another-pin"),
    "connection|Connection|invalid|closed",
    ignore.case = TRUE
  )

  expect_error(
    pin_read(b, "test-pin"),
    "connection|Connection|invalid|closed",
    ignore.case = TRUE
  )

  expect_error(
    pin_list(b),
    "connection|Connection|invalid|closed",
    ignore.case = TRUE
  )

  # Cleanup: create new connection to clean up the test data
  conn2 <- sf_stage_test_conn()
  b2 <- board_sf_stage(
    conn = conn2,
    stage = Sys.getenv("PINS_SF_STAGE", "@~"),
    path = path_base,
    connect_args = sf_stage_test_args()
  )
  withr::defer({
    pinsExtras:::sf_stage_delete(b2, path_base)
    DBI::dbDisconnect(conn2)
  })
})

# =============================================================================
# Error Handling: Invalid Stage Names
# =============================================================================

test_that("non-existent stage gives meaningful error on first operation", {
  skip_if_no_sf_stage()

  conn <- sf_stage_test_conn()
  withr::defer(DBI::dbDisconnect(conn))

  # Create board with a stage that definitely doesn't exist
  nonexistent_stage <- paste0("@nonexistent_stage_", as.integer(Sys.time()))
  b <- board_sf_stage(
    conn = conn,
    stage = nonexistent_stage,
    path = "test"
  )

  # First operation should fail with a stage-related error
  expect_error(
    pin_write(b, 1:3, "test"),
    "stage|Stage|does not exist|not exist",
    ignore.case = TRUE
  )

  expect_error(
    pin_list(b),
    "stage|Stage|does not exist|not exist",
    ignore.case = TRUE
  )
})

# =============================================================================
# Error Handling: Malformed Stage Names
# =============================================================================

test_that("board_sf_stage validates stage name format", {
  skip_if_no_sf_stage()

  conn <- sf_stage_test_conn()
  withr::defer(DBI::dbDisconnect(conn))

  # NULL stage name should error
  expect_error(
    board_sf_stage(conn = conn, stage = NULL),
    "stage.*must be a string"
  )

  # Non-string stage name should error
  expect_error(
    board_sf_stage(conn = conn, stage = 123),
    "stage.*must be a string"
  )

  # Empty string stage - should error or handle gracefully
  # The @ prefix will be added, resulting in just "@"
  b_empty <- board_sf_stage(conn = conn, stage = "")
  expect_error(
    pin_list(b_empty),
    # Will likely fail with SQL error or stage not found
    ".*"
  )
})

test_that("board_sf_stage requires valid DBI connection", {
  skip_if_no_sf_stage()

  # NULL connection should error
  expect_error(
    board_sf_stage(conn = NULL, stage = "@~"),
    "conn.*must be a DBI connection"
  )

  # Non-DBI object should error
  expect_error(
    board_sf_stage(conn = "not a connection", stage = "@~"),
    "conn.*must be a DBI connection"
  )

  expect_error(
    board_sf_stage(conn = list(), stage = "@~"),
    "conn.*must be a DBI connection"
  )
})

# =============================================================================
# Metadata Preservation: Tags
# =============================================================================

test_that("tags are preserved through write/read cycle", {
  skip_if_no_sf_stage()

  path_base <- paste0("pins-sf-tags-", as.integer(Sys.time()))
  b <- sf_stage_test_board(path_base)
  withr::defer(pinsExtras:::sf_stage_delete(b, path_base))

  # Multiple tags with various characters
  tags <- c("analysis", "production", "v2.0", "team-data", "2024_Q4")
  pin_write(b, 1:10, "tagged-pin", tags = tags)

  meta <- pin_meta(b, "tagged-pin")
  expect_equal(sort(meta$tags), sort(tags))
})

test_that("empty tags vector is preserved", {
  skip_if_no_sf_stage()

  path_base <- paste0("pins-sf-empty-tags-", as.integer(Sys.time()))
  b <- sf_stage_test_board(path_base)
  withr::defer(pinsExtras:::sf_stage_delete(b, path_base))

  pin_write(b, 1:5, "no-tags", tags = character(0))

  meta <- pin_meta(b, "no-tags")
  expect_true(is.null(meta$tags) || length(meta$tags) == 0)
})

test_that("NULL tags are handled correctly", {
  skip_if_no_sf_stage()

  path_base <- paste0("pins-sf-null-tags-", as.integer(Sys.time()))
  b <- sf_stage_test_board(path_base)
  withr::defer(pinsExtras:::sf_stage_delete(b, path_base))

  pin_write(b, 1:5, "null-tags", tags = NULL)

  meta <- pin_meta(b, "null-tags")
  expect_true(is.null(meta$tags) || length(meta$tags) == 0)
})

test_that("tags with special characters are preserved", {
  skip_if_no_sf_stage()

  path_base <- paste0("pins-sf-special-tags-", as.integer(Sys.time()))
  b <- sf_stage_test_board(path_base)
  withr::defer(pinsExtras:::sf_stage_delete(b, path_base))

  # Tags with hyphens, underscores, dots, and unicode
  special_tags <- c(
    "tag-with-hyphens",
    "tag_with_underscores",
    "tag.with.dots",
    "tag with spaces",
    "tag-\u00e9-unicode"  # tag-é-unicode
  )
  pin_write(b, 1:5, "special-tags", tags = special_tags)

  meta <- pin_meta(b, "special-tags")
  expect_equal(sort(meta$tags), sort(special_tags))
})

test_that("duplicate tags are preserved as-is", {
  skip_if_no_sf_stage()

  path_base <- paste0("pins-sf-dup-tags-", as.integer(Sys.time()))
  b <- sf_stage_test_board(path_base)
  withr::defer(pinsExtras:::sf_stage_delete(b, path_base))

  # Duplicate tags - test whether they're preserved or deduplicated
  dup_tags <- c("alpha", "beta", "alpha", "gamma", "beta")
  pin_write(b, 1:5, "dup-tags", tags = dup_tags)

  meta <- pin_meta(b, "dup-tags")
  # Just verify tags are present - behavior may vary
  expect_true("alpha" %in% meta$tags)
  expect_true("beta" %in% meta$tags)
  expect_true("gamma" %in% meta$tags)
})

test_that("many tags are preserved", {
  skip_if_no_sf_stage()

  path_base <- paste0("pins-sf-many-tags-", as.integer(Sys.time()))
  b <- sf_stage_test_board(path_base)
  withr::defer(pinsExtras:::sf_stage_delete(b, path_base))

  # Test with 50 tags
  many_tags <- paste0("tag", 1:50)
  pin_write(b, 1:5, "many-tags", tags = many_tags)

  meta <- pin_meta(b, "many-tags")
  expect_equal(sort(meta$tags), sort(many_tags))
  expect_equal(length(meta$tags), 50)
})

# =============================================================================
# Metadata Preservation: URLs
# =============================================================================

test_that("single URL is preserved", {
  skip_if_no_sf_stage()

  path_base <- paste0("pins-sf-url-single-", as.integer(Sys.time()))
  b <- sf_stage_test_board(path_base)
  withr::defer(pinsExtras:::sf_stage_delete(b, path_base))

  url <- "https://example.com/data"
  pin_write(b, 1:5, "single-url", urls = url)

  meta <- pin_meta(b, "single-url")
  expect_equal(meta$urls, url)
})

test_that("multiple URLs are preserved", {
  skip_if_no_sf_stage()

  path_base <- paste0("pins-sf-urls-multi-", as.integer(Sys.time()))
  b <- sf_stage_test_board(path_base)
  withr::defer(pinsExtras:::sf_stage_delete(b, path_base))

  urls <- c(
    "https://example.com/data1",
    "https://example.com/data2",
    "https://api.example.org/v1/resource"
  )
  pin_write(b, 1:5, "multi-urls", urls = urls)

  meta <- pin_meta(b, "multi-urls")
  expect_equal(sort(meta$urls), sort(urls))
})

test_that("URLs with query parameters and fragments are preserved", {
  skip_if_no_sf_stage()

  path_base <- paste0("pins-sf-urls-complex-", as.integer(Sys.time()))
  b <- sf_stage_test_board(path_base)
  withr::defer(pinsExtras:::sf_stage_delete(b, path_base))

  complex_urls <- c(
    "https://example.com/data?param1=value1&param2=value2",
    "https://example.com/page#section-anchor",
    "https://api.example.com/v2/data?filter=active&sort=date#results"
  )
  pin_write(b, 1:5, "complex-urls", urls = complex_urls)

  meta <- pin_meta(b, "complex-urls")
  expect_equal(sort(meta$urls), sort(complex_urls))
})

test_that("empty URLs vector is preserved", {
  skip_if_no_sf_stage()

  path_base <- paste0("pins-sf-empty-urls-", as.integer(Sys.time()))
  b <- sf_stage_test_board(path_base)
  withr::defer(pinsExtras:::sf_stage_delete(b, path_base))

  pin_write(b, 1:5, "no-urls", urls = character(0))

  meta <- pin_meta(b, "no-urls")
  expect_true(is.null(meta$urls) || length(meta$urls) == 0)
})

test_that("NULL URLs are handled correctly", {
  skip_if_no_sf_stage()

  path_base <- paste0("pins-sf-null-urls-", as.integer(Sys.time()))
  b <- sf_stage_test_board(path_base)
  withr::defer(pinsExtras:::sf_stage_delete(b, path_base))

  pin_write(b, 1:5, "null-urls", urls = NULL)

  meta <- pin_meta(b, "null-urls")
  expect_true(is.null(meta$urls) || length(meta$urls) == 0)
})

# =============================================================================
# Metadata Preservation: Combined Tags and URLs
# =============================================================================

test_that("tags and URLs work together", {
  skip_if_no_sf_stage()

  path_base <- paste0("pins-sf-tags-urls-", as.integer(Sys.time()))
  b <- sf_stage_test_board(path_base)
  withr::defer(pinsExtras:::sf_stage_delete(b, path_base))

  tags <- c("production", "validated", "v3.0")
  urls <- c(
    "https://example.com/source",
    "https://docs.example.com/spec"
  )

  pin_write(b, 1:10, "tags-and-urls",
    title = "Combined Metadata Test",
    description = "Testing tags and URLs together",
    tags = tags,
    urls = urls
  )

  meta <- pin_meta(b, "tags-and-urls")

  # Verify all metadata is preserved
  expect_equal(meta$title, "Combined Metadata Test")
  expect_equal(meta$description, "Testing tags and URLs together")
  expect_equal(sort(meta$tags), sort(tags))
  expect_equal(sort(meta$urls), sort(urls))
})

test_that("comprehensive metadata preservation", {
  skip_if_no_sf_stage()

  path_base <- paste0("pins-sf-comprehensive-meta-", as.integer(Sys.time()))
  b <- sf_stage_test_board(path_base)
  withr::defer(pinsExtras:::sf_stage_delete(b, path_base))

  # Kitchen sink test: everything at once
  pin_write(b, iris, "comprehensive-meta",
    title = "Iris Dataset with Complete Metadata",
    description = "A comprehensive test of metadata preservation including special characters: quotes \"like this\" and symbols & more",
    tags = c("dataset", "iris", "classification", "r-datasets", "example-data"),
    urls = c(
      "https://en.wikipedia.org/wiki/Iris_flower_data_set",
      "https://archive.ics.uci.edu/ml/datasets/iris"
    ),
    metadata = list(
      source = "R datasets package",
      rows = 150,
      columns = 5,
      custom_field = "custom_value"
    )
  )

  meta <- pin_meta(b, "comprehensive-meta")

  # Verify standard metadata
  expect_equal(meta$title, "Iris Dataset with Complete Metadata")
  expect_true(grepl("quotes \"like this\"", meta$description))
  expect_equal(length(meta$tags), 5)
  expect_true(all(c("dataset", "iris", "classification") %in% meta$tags))
  expect_equal(length(meta$urls), 2)
  expect_true("https://en.wikipedia.org/wiki/Iris_flower_data_set" %in% meta$urls)

  # Verify custom metadata
  expect_equal(meta$user$source, "R datasets package")
  expect_equal(meta$user$rows, 150)
  expect_equal(meta$user$columns, 5)
  expect_equal(meta$user$custom_field, "custom_value")

  # Verify data is correct
  data <- pin_read(b, "comprehensive-meta")
  expect_s3_class(data, "data.frame")
  expect_equal(nrow(data), 150)
  expect_equal(ncol(data), 5)
})
