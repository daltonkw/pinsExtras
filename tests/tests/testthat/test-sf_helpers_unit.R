# Unit tests for helper functions - no Snowflake connection required

test_that("sf_normalize_path handles edge cases", {
  # Create a mock board object for testing
  mock_board <- list(path = "base/path")

  # Basic path joining (use as.character to strip fs_path class)
  expect_equal(
    as.character(pinsExtras:::sf_normalize_path(mock_board, "subdir")),
    "base/path/subdir"
  )

  # Empty board path with subdir
  mock_board_empty <- list(path = "")
  expect_equal(
    as.character(pinsExtras:::sf_normalize_path(mock_board_empty, "subdir")),
    "subdir"
  )

  # Empty board path AND empty dir should return "" not "/"

  expect_equal(
    as.character(pinsExtras:::sf_normalize_path(mock_board_empty, "")),
    ""
  )

  # Leading slash should be stripped
  mock_board_slash <- list(path = "/leading")
  result <- pinsExtras:::sf_normalize_path(mock_board_slash, "subdir")
  expect_false(startsWith(result, "//"))

  # Double slashes should be collapsed
  mock_board_double <- list(path = "path//with")
  result <- pinsExtras:::sf_normalize_path(mock_board_double, "//double")
  expect_false(grepl("//", result))
})

test_that("sf_end_with_slash adds trailing slash correctly", {
  # Without slash
expect_equal(pinsExtras:::sf_end_with_slash("path"), "path/")

  # Already has slash
  expect_equal(pinsExtras:::sf_end_with_slash("path/"), "path/")

  # Empty string
  expect_equal(pinsExtras:::sf_end_with_slash(""), "/")

  # Vector input
  expect_equal(
    pinsExtras:::sf_end_with_slash(c("a", "b/", "c")),
    c("a/", "b/", "c/")
  )
})

test_that("sf_version_from_path parses valid versions", {
  versions <- c("20231215T103045Z-abc12", "20240101T000000Z-xyz99")
  result <- pinsExtras:::sf_version_from_path(versions)

  expect_s3_class(result, "tbl_df")
  expect_equal(nrow(result), 2)
  expect_equal(result$version, versions)
  expect_equal(result$hash, c("abc12", "xyz99"))
  expect_false(any(is.na(result$created)))
})

test_that("sf_version_from_path handles malformed versions", {
  # Missing hash
  result <- pinsExtras:::sf_version_from_path("20231215T103045Z")
  expect_true(is.na(result$hash))
  expect_true(is.na(result$created))

  # Completely invalid
  result <- pinsExtras:::sf_version_from_path("not-a-version")
  expect_true(is.na(result$hash))
  expect_true(is.na(result$created))

  # Empty vector
  result <- pinsExtras:::sf_version_from_path(character(0))
  expect_equal(nrow(result), 0)
})

test_that("sf_parse_8601_compact parses dates correctly", {
  result <- pinsExtras:::sf_parse_8601_compact("20231215T103045Z")

  expect_s3_class(result, "POSIXct")
  expect_equal(format(result, "%Y-%m-%d %H:%M:%S", tz = "UTC"), "2023-12-15 10:30:45")
})

test_that("sf_check_pin_name validates correctly", {
  # Valid names
  expect_silent(pinsExtras:::sf_check_pin_name("valid-name"))
  expect_silent(pinsExtras:::sf_check_pin_name("valid_name"))
  expect_silent(pinsExtras:::sf_check_pin_name("valid.name"))

  # Reserved name
  expect_error(
    pinsExtras:::sf_check_pin_name("data.txt"),
    "data.txt"
  )

  # Non-string
  expect_error(
    pinsExtras:::sf_check_pin_name(123),
    "must be a string"
  )

  expect_error(
    pinsExtras:::sf_check_pin_name(c("a", "b")),
    "must be a string"
  )
})

test_that("sf_manifest_pin_yaml_filename is correct", {
  expect_equal(pinsExtras:::sf_manifest_pin_yaml_filename, "_pins.yaml")
})

test_that("board_sf_stage validates inputs", {
  # NULL connection
  expect_error(
    board_sf_stage(conn = NULL, stage = "@~"),
    "DBI connection"
  )

  # Non-DBI connection
  expect_error(
    board_sf_stage(conn = "not-a-connection", stage = "@~"),
    "DBI connection"
  )

  # Non-string stage
  expect_error(
    board_sf_stage(conn = structure(list(), class = "DBIConnection"), stage = 123),
    "must be a string"
  )
})

test_that("sf_stage_list filters by exact directory match", {
 # This tests that "mtcars" doesn't match "mtcars_pqt"
  # We can't do a full integration test without Snowflake, but we can
  # test the filtering logic by checking the conditions used

  # Simulate the filtering logic from sf_stage_list
  test_names <- c(
    "mtcars/v1/data.txt",
    "mtcars/v1/mtcars.rds",
    "mtcars_pqt/v1/data.txt",
    "mtcars_pqt/v1/mtcars_pqt.parquet"
  )

  prefix <- "mtcars"
  is_exact <- test_names == prefix
  is_child <- startsWith(test_names, paste0(prefix, "/"))
  filtered <- test_names[is_exact | is_child]

  # Should only match mtcars/, not mtcars_pqt/
  expect_equal(filtered, c("mtcars/v1/data.txt", "mtcars/v1/mtcars.rds"))

  # Test with mtcars_pqt prefix
  prefix2 <- "mtcars_pqt"
  is_exact2 <- test_names == prefix2
  is_child2 <- startsWith(test_names, paste0(prefix2, "/"))
  filtered2 <- test_names[is_exact2 | is_child2]

  expect_equal(filtered2, c("mtcars_pqt/v1/data.txt", "mtcars_pqt/v1/mtcars_pqt.parquet"))
})

test_that("sf_extract_stage_name extracts stage name correctly", {
  # Simple stage with @ prefix
  expect_equal(pinsExtras:::sf_extract_stage_name("@mystage"), "mystage")

  # User stage
  expect_equal(pinsExtras:::sf_extract_stage_name("@~"), "~")

  # Fully qualified stage name (db.schema.stage)
  expect_equal(
    pinsExtras:::sf_extract_stage_name("@mydb.myschema.mystage"),
    "mystage"
  )

  # Two-part name (schema.stage)
  expect_equal(
    pinsExtras:::sf_extract_stage_name("@myschema.mystage"),
    "mystage"
  )

  # Without @ prefix (shouldn't happen but handle gracefully)
  expect_equal(pinsExtras:::sf_extract_stage_name("mystage"), "mystage")
})
