# pinsExtras

> Additional board implementations for the [pins](https://pins.rstudio.com) package

## Overview

**pinsExtras** extends the [pins](https://pins.rstudio.com) package with additional storage backend implementations. The [pins](https://github.com/rstudio/pins-r) package makes it easy to share data, models, and other objects across projects and with colleagues by providing a common API for pinning (publishing) and retrieving objects from various storage backends.

Currently, pinsExtras provides `board_sf_stage()`, which enables pinning to **Snowflake internal stages** via ODBC.

## Why pinsExtras?

While the main [pins](https://pins.rstudio.com) package provides excellent support for Snowflake external stages (S3, Azure, GCS-backed) through `board_s3()`, `board_azure()`, and `board_gcs()`, **internal Snowflake stages** require direct interaction with Snowflake's `PUT`/`GET`/`LIST`/`REMOVE` commands.

`board_sf_stage()` fills this gap by providing native support for Snowflake internal stages, including: - User stages (`@~`) - Named stages (`@my_stage`) - Fully qualified stages (`@database.schema.stage`)

This is ideal for organizations that need to share pins within Snowflake without configuring external cloud storage.

## Features

-   ✅ Full [pins](https://pins.rstudio.com) API compatibility (`pin_write()`, `pin_read()`, `pin_list()`, etc.)
-   ✅ Automatic versioning with `pin_versions()`
-   ✅ Multiple data formats (RDS, CSV, JSON, Parquet, Arrow, qs)
-   ✅ Metadata preservation (tags, descriptions, URLs)
-   ✅ JWT authentication support for secure Snowflake connections
-   ✅ Connection health monitoring with helpful error messages
-   ✅ Comprehensive test suite

## Installation

### Prerequisites

You'll need:

1.  R packages: `pins`, `DBI`, `odbc`
2.  [Snowflake ODBC driver](https://docs.snowflake.com/en/developer-guide/odbc/odbc) installed on your system

``` r
# Install dependencies from CRAN
install.packages(c("pins", "DBI", "odbc"))
```

### Install pinsExtras

``` r
# Install from GitHub (once published)
remotes::install_github("daltonkw/pinsExtras")

# Or install from local source
install.packages("path/to/pinsExtras", repos = NULL, type = "source")
```

## Quick Start

``` r
library(pins)
library(pinsExtras)

# 1. Connect to Snowflake using JWT authentication
conn <- DBI::dbConnect(
  odbc::odbc(),
  Driver = "Snowflake",
  Server = Sys.getenv("PINS_SF_SERVER"),
  UID = Sys.getenv("PINS_SF_USER"),
  Authenticator = "SNOWFLAKE_JWT",
  PRIV_KEY_FILE = Sys.getenv("PINS_SF_PRIVATE_KEY_FILE"),
  Warehouse = Sys.getenv("PINS_SF_WAREHOUSE")
)

# 2. Create a board pointing to an internal Snowflake stage
board <- board_sf_stage(conn, stage = "@~", path = "my-pins")

# 3. Use the standard pins API
pin_write(board, mtcars, "cars-data", description = "Motor Trend car data")
pin_read(board, "cars-data")

# View versions
pin_versions(board, "cars-data")

# Read a specific version
pin_read(board, "cars-data", version = "20231215T120000-a1b2c")

# List all pins
pin_list(board)

# Delete when done
pin_delete(board, "cars-data")

# Clean up
DBI::dbDisconnect(conn)
```

## Setup Guide

Detailed setup instructions for Windows and Linux are available in the sections below.

<details>

<summary><strong>Windows Setup</strong></summary>

### Step 1: Install Snowflake ODBC Driver

1.  Download from [Snowflake ODBC Downloads](https://developers.snowflake.com/odbc/)
2.  Run the installer with default settings
3.  Verify installation:
    -   Press `Win + R` → type `odbcad32` → **Drivers** tab
    -   Confirm `SnowflakeDSIIDriver` appears in the list

### Step 2: Set Up JWT Authentication

Generate a key pair using OpenSSL (install via `winget install OpenSSL` if needed):

``` powershell
# Create directory
mkdir "$env:USERPROFILE\.snowflake" -Force

# Generate private key (no passphrase)
openssl genrsa 2048 | openssl pkcs8 -topk8 -nocrypt -out "$env:USERPROFILE\.snowflake\rsa_key.p8"

# Generate public key
openssl rsa -in "$env:USERPROFILE\.snowflake\rsa_key.p8" -pubout -out "$env:USERPROFILE\.snowflake\rsa_key.pub"

# Display public key
Get-Content "$env:USERPROFILE\.snowflake\rsa_key.pub"
```

Register the public key in Snowflake:

``` sql
ALTER USER your_username SET RSA_PUBLIC_KEY='MIIBIjANBgkq...your_public_key...';
```

### Step 3: Configure Environment Variables

1.  Press `Win + R` → type `sysdm.cpl` → **Advanced** → **Environment Variables**
2.  Add these **User variables**:

| Variable                   | Example Value                        |
|----------------------------|--------------------------------------|
| `PINS_SF_SERVER`           | `account.snowflakecomputing.com`     |
| `PINS_SF_USER`             | `YOUR_USERNAME`                      |
| `PINS_SF_AUTHENTICATOR`    | `SNOWFLAKE_JWT`                      |
| `PINS_SF_PRIVATE_KEY_FILE` | `C:\Users\You\.snowflake\rsa_key.p8` |
| `PINS_SF_WAREHOUSE`        | `COMPUTE_WH`                         |

3.  Restart R/RStudio for changes to take effect

</details>

<details>

<summary><strong>Linux Setup (Ubuntu/Debian)</strong></summary>

### Step 1: Install Snowflake ODBC Driver

``` bash
# Download and install (check Snowflake docs for latest version)
wget https://sfc-repo.snowflakecomputing.com/odbc/linux/latest/snowflake-odbc-3.13.0.x86_64.deb
sudo dpkg -i snowflake-odbc-3.13.0.x86_64.deb

# Fix library symlink if needed (Ubuntu 24.04)
sudo ln -sf /usr/lib/x86_64-linux-gnu/libodbcinst.so.2 /usr/lib/x86_64-linux-gnu/libodbcinst.so.1
```

### Step 2: Register the ODBC Driver

Create `odbcinst.ini` in your project directory or `/etc/`:

``` ini
[Snowflake]
Driver = /usr/lib/snowflake/odbc/lib/libSnowflake.so
```

If using a project-local file, set the `ODBCSYSINI` environment variable:

``` bash
export ODBCSYSINI=/path/to/your/project
```

### Step 3: Set Up JWT Authentication

``` bash
# Create directory
mkdir -p ~/.snowflake

# Generate private key (no passphrase)
openssl genrsa 2048 | openssl pkcs8 -topk8 -nocrypt -out ~/.snowflake/rsa_key.p8

# Generate public key
openssl rsa -in ~/.snowflake/rsa_key.p8 -pubout -out ~/.snowflake/rsa_key.pub

# Display public key for Snowflake registration
cat ~/.snowflake/rsa_key.pub
```

Register the public key in Snowflake:

``` sql
ALTER USER your_username SET RSA_PUBLIC_KEY='MIIBIjANBgkq...';
```

### Step 4: Configure Environment Variables

Add to `~/.bashrc` or `~/.profile`:

``` bash
export PINS_SF_SERVER="account.snowflakecomputing.com"
export PINS_SF_USER="YOUR_USERNAME"
export PINS_SF_AUTHENTICATOR="SNOWFLAKE_JWT"
export PINS_SF_PRIVATE_KEY_FILE="$HOME/.snowflake/rsa_key.p8"
export PINS_SF_WAREHOUSE="COMPUTE_WH"
```

Reload your shell configuration:

``` bash
source ~/.bashrc
```

</details>

## Environment Variables Reference

| Variable                   | Required | Description                                                    |
|----------------------|----------------------|----------------------------|
| `PINS_SF_SERVER`           | **Yes**  | Snowflake account URL (e.g., `account.snowflakecomputing.com`) |
| `PINS_SF_USER`             | **Yes**  | Snowflake username                                             |
| `PINS_SF_AUTHENTICATOR`    | **Yes**  | Authentication method (`SNOWFLAKE_JWT` recommended)            |
| `PINS_SF_PRIVATE_KEY_FILE` | **Yes**  | Path to JWT private key file                                   |
| `PINS_SF_WAREHOUSE`        | **Yes**  | Compute warehouse name                                         |
| `PINS_SF_DATABASE`         | No       | Default database                                               |
| `PINS_SF_SCHEMA`           | No       | Default schema                                                 |
| `PINS_SF_ROLE`             | No       | Snowflake role to use                                          |
| `PINS_SF_STAGE`            | No       | Default stage name (default: `@~`)                             |
| `PINS_SF_DRIVER`           | No       | Override ODBC driver name                                      |
| `ODBCSYSINI`               | No       | Linux: path to `odbcinst.ini` directory                        |

## Scope and Limitations

### Internal Stages Only

This package is designed **exclusively for Snowflake internal stages**: 
- ✅ User stages (`@~`) 
- ✅ Named stages (`@my_stage`) 
- ✅ Table stages 
- ✅ Fully qualified stages (`@database.schema.stage`)

### External Stages Not Supported

External stages backed by cloud storage (S3, Azure Blob Storage, Google Cloud Storage) are **out of scope**.

For external stages, use the native cloud board implementations in the [pins](https://pins.rstudio.com) package: 
- **S3-backed stages** → [`board_s3()`](https://pins.rstudio.com/reference/board_s3.html) 
- **Azure-backed stages** → [`board_azure()`](https://pins.rstudio.com/reference/board_azure.html) 
- **GCS-backed stages** → [`board_gcs()`](https://pins.rstudio.com/reference/board_gcs.html)

## Troubleshooting

<details>

<summary>"Driver not found" error</summary>

**Windows:** - Run `odbcad32` and verify `SnowflakeDSIIDriver` appears in the Drivers tab - Try setting `PINS_SF_DRIVER` environment variable explicitly

**Linux:** - Verify `ODBCSYSINI` points to the directory containing `odbcinst.ini` - Check that the driver path in `odbcinst.ini` is correct - Confirm the Snowflake ODBC driver is installed: `ls /usr/lib/snowflake/odbc/lib/libSnowflake.so`

</details>

<details>

<summary>"Authentication failed" error</summary>

-   Verify the public key is registered in Snowflake:

    ``` sql
    DESC USER your_username;
    ```

    Look for `RSA_PUBLIC_KEY` property

-   Check that `PINS_SF_PRIVATE_KEY_FILE` points to the correct file

-   Ensure the private key was generated **without a passphrase**

-   Verify your username matches exactly (case-sensitive)

</details>

<details>

<summary>"Warehouse does not exist" error</summary>

-   Verify the warehouse name matches exactly (case-sensitive)

-   Check you have `USAGE` privilege on the warehouse:

    ``` sql
    SHOW GRANTS ON WAREHOUSE your_warehouse;
    ```

</details>

<details>

<summary>Connection becomes invalid during use</summary>

Snowflake connections can time out or become invalid. The board automatically detects this and provides helpful reconnection guidance:

``` r
# If you see a connection error, reconnect:
conn <- DBI::dbConnect(odbc::odbc(), ...)
board <- board_sf_stage(conn, stage = "@~")
```

</details>

## Testing

The package attempts to provide comprehensive test coverage: 
- **unit tests** (no Snowflake connection required) 
- **integration tests** (require Snowflake credentials)

Integration tests are automatically skipped unless `PINS_SF_*` environment variables are set. They test full workflows (read/write/version/delete) and clean up after themselves.

Run tests locally:

``` r
devtools::test()
```

Run R CMD check:

``` r
devtools::check()
```

## Documentation

For detailed API documentation, see:

``` r
# After installation
?board_sf_stage
?pinsExtras
```

For general pins usage, see the [pins package documentation](https://pins.rstudio.com).

## Contributing

Contributions are welcome.  Please feel free to submit a Pull Request.

## License

MIT License - see [LICENSE](LICENSE) file for details.

## Acknowledgments

This package extends [pins](https://github.com/rstudio/pins-r) by Posit Software, PBC. The Snowflake stage board implementation attempts to follow the same design patterns as the cloud board implementations in the main pins package.  Any mistakes in the pinsExtras package are the author's alone.

------------------------------------------------------------------------

**Note:** This is a standalone package and is not affiliated with or endorsed by Posit Software, PBC or Snowflake Inc.