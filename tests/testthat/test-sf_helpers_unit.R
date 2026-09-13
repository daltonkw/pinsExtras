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

test_that("sf_quote_sql_literal wraps text in single quotes", {
  expect_identical(pinsExtras:::sf_quote_sql_literal("abc"), "'abc'")
})

test_that("sf_quote_sql_literal escapes a single quote with a backslash", {
  expect_identical(
    pinsExtras:::sf_quote_sql_literal("bob's data"),
    "'bob\\'s data'"
  )
})

test_that("sf_quote_sql_literal doubles each backslash", {
  expect_identical(pinsExtras:::sf_quote_sql_literal("a\\b"), "'a\\\\b'")
})

test_that("sf_quote_sql_literal quotes an empty string", {
  expect_identical(pinsExtras:::sf_quote_sql_literal(""), "''")
})

test_that("sf_quote_stage_path quotes a stage location verbatim", {
  expect_identical(pinsExtras:::sf_quote_stage_path("@~"), "'@~'")
  expect_identical(
    pinsExtras:::sf_quote_stage_path("@~/team-data/cars/"),
    "'@~/team-data/cars/'"
  )
  expect_identical(
    pinsExtras:::sf_quote_stage_path("@db.schema.stage/x"),
    "'@db.schema.stage/x'"
  )
})

test_that("sf_quote_file_uri prefixes file:// then quotes", {
  expect_identical(
    pinsExtras:::sf_quote_file_uri("/tmp/x/data.txt"),
    "'file:///tmp/x/data.txt'"
  )
  expect_identical(
    pinsExtras:::sf_quote_file_uri("/tmp/o'brien/data.txt"),
    "'file:///tmp/o\\'brien/data.txt'"
  )
})

test_that("sf_escape_regex escapes a dot", {
  expect_identical(pinsExtras:::sf_escape_regex("data.txt"), "data\\.txt")
})

test_that("sf_escape_regex does not escape a hyphen", {
  expect_identical(pinsExtras:::sf_escape_regex("a-b"), "a-b")
})

test_that("sf_escape_regex leaves a plain string untouched", {
  expect_identical(pinsExtras:::sf_escape_regex("plain"), "plain")
})

test_that("sf_escape_regex escapes a backslash", {
  expect_identical(pinsExtras:::sf_escape_regex("a\\b"), "a\\\\b")
})

test_that("sf_escape_regex returns empty for empty input", {
  expect_identical(pinsExtras:::sf_escape_regex(""), "")
})

test_that("sf_escape_regex does not escape a slash", {
  expect_identical(pinsExtras:::sf_escape_regex("a/b"), "a/b")
})

test_that("sf_escape_regex is vectorised over its input", {
  expect_identical(
    pinsExtras:::sf_escape_regex(c("a.b", "c")),
    c("a\\.b", "c")
  )
})

test_that("sf_escape_regex returns character(0) for empty input", {
  expect_identical(pinsExtras:::sf_escape_regex(character(0)), character(0))
})

test_that("sf_remove_pattern builds the pattern for one file", {
  expect_identical(
    pinsExtras:::sf_remove_pattern("data.txt"),
    "^(.*/)?data\\.txt$"
  )
  expect_identical(
    pinsExtras:::sf_remove_pattern("_pins.yaml"),
    "^(.*/)?_pins\\.yaml$"
  )
  expect_identical(
    pinsExtras:::sf_remove_pattern("cars.rds"),
    "^(.*/)?cars\\.rds$"
  )
})

test_that("sf_remove_pattern's grepl match is TRUE for the one file", {
  pat <- pinsExtras:::sf_remove_pattern("data.txt")
  # A bare relative name is the live semantics.
  expect_true(grepl(pat, "data.txt"))
  # A full staged path matches too, with or without a stage-name prefix.
  expect_true(grepl(pat, "cars/20240101T000000Z-abc12/data.txt"))
  expect_true(
    grepl(pat, "mystage/cars/20240101T000000Z-abc12/data.txt")
  )
  expect_true(
    grepl(pat, "mystage/team-data/cars/20240101T000000Z-abc12/data.txt")
  )
})

test_that("sf_remove_pattern's grepl match is FALSE for everything else", {
  pat <- pinsExtras:::sf_remove_pattern("data.txt")
  expect_false(grepl(pat, "data.txt.bak"))
  expect_false(grepl(pat, "cars.rds"))
  expect_false(
    grepl(pat, "mystage/cars/20240101T000000Z-abc12/data.txt.bak")
  )
  expect_false(
    grepl(pat, "mystage/cars/20240101T000000Z-abc12/cars.rds")
  )
})

test_that("sf_escape_regex escapes exactly the 14 Java metacharacters", {
  # Each Java metacharacter, on its own, comes back escaped with a single
  # leading backslash...
  java <- c("\\", "^", "$", ".", "|", "?", "*", "+",
    "(", ")", "[", "]", "{", "}")
  expect_identical(pinsExtras:::sf_escape_regex(java), paste0("\\", java))
  # ... while every character Java does not treat as special is untouched.
  expect_identical(
    pinsExtras:::sf_escape_regex("a=b!c<d>e:f-g"),
    "a=b!c<d>e:f-g"
  )
})

test_that("sf_get_pattern builds a different pattern from sf_remove_pattern",
{
  # GET and REMOVE share the file name but apply it to different Snowflake
  # engines, so they must not collapse to one helper.
  expect_false(
    identical(
      pinsExtras:::sf_get_pattern("data.txt"),
      pinsExtras:::sf_remove_pattern("data.txt")
    )
  )
})

test_that("sf_get_pattern builds the .*/...$ pattern for one file", {
  expect_identical(pinsExtras:::sf_get_pattern("data.txt"), ".*/data\\.txt$")
  expect_identical(pinsExtras:::sf_get_pattern("report"), ".*/report$")
  expect_identical(
    pinsExtras:::sf_get_pattern("report.pdf"),
    ".*/report\\.pdf$"
  )
  expect_identical(
    pinsExtras:::sf_get_pattern("my.pin.rds"),
    ".*/my\\.pin\\.rds$"
  )
})

test_that("sf_get_pattern's grepl match is TRUE for the one staged file", {
  pat <- pinsExtras:::sf_get_pattern("data.txt")
  # GET matches the whole staged path, which carries a leading prefix.
  expect_true(grepl(pat, "mystage/cars/v/data.txt"))
})

test_that("sf_get_pattern's grepl match is FALSE for a sibling", {
  pat <- pinsExtras:::sf_get_pattern("data.txt")
  expect_false(grepl(pat, "mystage/cars/v/data.txt.bak"))
  expect_false(grepl(pat, "mystage/cars/v/cars.rds"))
})
