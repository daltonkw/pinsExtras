library(pins)
library(pinsExtras)

# Create connection (env vars must be set - see Setup below)
conn <- DBI::dbConnect(
  odbc::odbc(),
  Driver = pinsExtras:::sf_odbc_driver(),
  Server = Sys.getenv("PINS_SF_SERVER"),
  UID = Sys.getenv("PINS_SF_USER"),
  Authenticator = Sys.getenv("PINS_SF_AUTHENTICATOR"),
  PRIV_KEY_FILE = Sys.getenv("PINS_SF_PRIVATE_KEY_FILE"),
  Warehouse = Sys.getenv("PINS_SF_WAREHOUSE")
)


board <- pinsExtras::board_sf_stage(
  conn = conn,
  stage = "@testing_db.testing_schema.testing_stage"
)

# Reading and Writing Data

mtcars <- tibble::as_tibble(mtcars)

board |> pin_write(mtcars, "mtcars", type = "rds")

mtcars_read <- board |> pin_read("mtcars")

pin_list(board)

pin_exists(board, "mtcars")

pinsExtras:::sf_stage_list(board, "")

board$stage

board$path

DBI::dbGetQuery(conn, "LIST @testing_db.testing_schema.testing_stage")


pinsExtras:::sf_extract_stage_name(board$stage)

pinsExtras:::sf_stage_list(board, "")

df <- DBI::dbGetQuery(conn, sprintf("LIST %s", board$stage))
df

identical(board$conn, conn)
DBI::dbGetQuery(board$conn, "SELECT 1")
DBI::dbGetQuery(board$conn, sprintf("LIST %s", board$stage))

prefix <- pinsExtras:::sf_normalize_path(board, "")
prefix

df <- pinsExtras:::sf_stage_cmd(board, sprintf("LIST %s", board$stage))
df
nrow(df)