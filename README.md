# pinsExtras <img src="pinsExtras_hex.png" alt="pinsExtras logo" align="right" height="139"/>

> Additional board implementations for the [pins](https://pins.rstudio.com) package

## Overview

**pinsExtras** extends the [pins](https://pins.rstudio.com) package with additional storage backend implementations. The [pins](https://github.com/rstudio/pins-r) package makes it easy to share data, models, and other objects across projects and with colleagues by providing a common API for pinning (publishing) and retrieving objects from various storage backends.

Currently, pinsExtras provides `board_sf_stage()`, which enables pinning to **Snowflake internal stages** via ODBC.

## Why pinsExtras?

While the main [pins](https://pins.rstudio.com) package provides excellent support for Snowflake external stages (S3, Azure, GCS-backed) through `board_s3()`, `board_azure()`, and `board_gcs()`, **internal Snowflake stages** require direct interaction with Snowflake's `PUT`/`GET`/`LIST`/`REMOVE` commands.

`board_sf_stage()` fills this gap by providing native support for Snowflake internal stages, including: - User stages (`@~`) - Named stages (`@my_stage`) - Fully qualified stages (`@database.schema.stage`)

This is ideal for organizations that need to share pins within Snowflake without configuring external cloud storage.

## Features

- ✅ Full [pins](https://pins.rstudio.com) API compatibility (`pin_write()`, `pin_read()`, `pin_list()`, etc.)
- ✅ Automatic versioning with `pin_versions()`
- ✅ Every storage type `pins` supports, including `rds`, `csv`, `json`, `parquet`, `arrow` and `qs2` (`qs` is deprecated upstream in favour of `qs2`), plus multi-file pins via `pin_upload()`/`pin_download()`
- ✅ Metadata preservation (tags, descriptions, URLs)
- ✅ JWT authentication support for secure Snowflake connections
- ✅ Connection health monitoring with helpful error messages
- ✅ 589 offline tests that need no Snowflake connection, plus an opt-in integration suite that runs against a real stage

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
# Install from GitHub
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
    - Press `Win + R` → type `odbcad32` → **Drivers** tab
    - Confirm `SnowflakeDSIIDriver` appears in the list

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

| Variable | Required | Description |
|----------------------|----------------------|----------------------------|
| `PINS_SF_SERVER` | **Yes** | Snowflake account URL (e.g., `account.snowflakecomputing.com`) |
| `PINS_SF_USER` | **Yes** | Snowflake username |
| `PINS_SF_AUTHENTICATOR` | **Yes** | Authentication method (`SNOWFLAKE_JWT` recommended) |
| `PINS_SF_PRIVATE_KEY_FILE` | **Yes** | Path to JWT private key file |
| `PINS_SF_WAREHOUSE` | **Yes** | Compute warehouse name |
| `PINS_SF_DATABASE` | No | Default database |
| `PINS_SF_SCHEMA` | No | Default schema |
| `PINS_SF_ROLE` | No | Snowflake role to use |
| `PINS_SF_STAGE` | No | Default stage name (default: `@~`) |
| `PINS_SF_DRIVER` | No | Override ODBC driver name |
| `ODBCSYSINI` | No | Linux: path to `odbcinst.ini` directory |

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

## How publication works

A pin version is published in a specific order, and that order is the reason an interrupted write cannot corrupt a pin.

1.  Payload files are uploaded to a new version directory.
2.  The `data.txt` metadata marker is uploaded **last**.

A version counts as published only once its marker exists. Every discovery path -- `pin_list()`, `pin_exists()`, `pin_versions()`, `pin_meta()`, `pin_read()` -- applies that same definition, so a half-written version is invisible rather than broken. The previous version stays readable throughout.

Uploads never overwrite. Retrying into a directory that already holds a failed attempt's files fails loudly instead of mixing two attempts together.

**Mutation of one pin is serialized: one mutator at a time.** That covers writers *and* deleters, in any combination. Operations on different pins may run concurrently.

This is a prerequisite you have to arrange, not something the package enforces. Two writers can both pass preflight; two unversioned replacements can each capture the same old version and leave two new ones; a reader can resolve a version and have a concurrent replacement delete it mid-read. `OVERWRITE=FALSE` governs individual files, not ownership of a whole version. Where the same version id is produced twice for one pin that *is* reported as an error rather than merged — but collision detection is not promised for every interleaving.

### Unversioned replacement

`pin_write(board, x, "name", versioned = FALSE)` uploads the new version *before* removing the old one, so the pin is never absent. If the old version cannot be confirmed removed afterwards, the write still succeeds and you get a warning naming the exact version to read:

``` r
#> Warning: Published pin "cars" version "20250101T120001Z-bcdef", but cleanup is
#> incomplete.
#> i These old versions still have files: "20250101T120000Z-abcde".
#> i Read the new version explicitly with
#>   `pin_read(board, "cars", version = "20250101T120001Z-bcdef")`.
```

A successful publication is never turned into an error by a cleanup problem.

## Orphaned versions

Because a version without `data.txt` is not published, an interrupted write leaves files that discovery cannot see. `pin_delete()` will report the pin as missing:

``` r
pin_delete(board, "cars")
#> Error: Can't find pin called "cars"
```

That is deliberate. `pin_version_delete()` is the way to remove such a directory: it performs no listing and no existence check.

``` r
# Every version directory on the stage for this pin, published or not.
sf_all_version_dirs <- function(board, name) {
  listing <- pinsExtras:::sf_stage_list(board, name)
  rel <- pinsExtras:::sf_board_relative(
    listing, pinsExtras:::sf_normalize_path(board)
  )
  unique(basename(dirname(rel$name)))
}

# The orphans are the directories discovery cannot see.
all_dirs <- sf_all_version_dirs(board, "cars")
published <- pin_versions(board, "cars")$version
orphans   <- setdiff(all_dirs, published)
orphans
#> [1] "20250101T120000Z-abcde"

# Remove them:
for (v in orphans) pin_version_delete(board, "cars", v)
```

Version directories whose names are malformed are ignored by discovery rather than reported as versions, and `pin_version_delete()` removes those too.

## Deleting

Deletion is scoped exactly: a pin named `cars` cannot reach a pin named `cars_extra`, and removing one file cannot remove a similarly named sibling.

`pin_delete()` accepts several names and processes them **in order, deleting as it goes**, matching upstream `pins`. There is no all-or-nothing guarantee across the vector:

``` r
pin_delete(board, c("cars", ""))
#> cars is deleted, then:
#> Error: `names` must be non-empty strings
```

If you need all-or-nothing, validate the names yourself before calling.

## Caching

Reads cache locally under `pins::board_cache_path()`.

**The cache directory is keyed by the stage text and the board path, and by nothing else.** Not the account, not the user behind `@~`, not the database or schema an unqualified stage name resolves to. Two boards pointing at *different Snowflake accounts* with the same stage text therefore share one cache directory, and the second write of a given version id replaces the first one's files. An earlier version of this README claimed sessions are isolated from each other; that was wrong.

What follows from it:

- Do not share a cache directory across accounts. Pass a distinct `cache` to each board when the same stage text can mean different things.
- Cache directories are created with ambient permissions, not forced-private ones, so your umask decides who can read downloaded pin contents. If that matters, put the cache somewhere whose permissions you control.
- Within one account the cache is keyed by pin and version, and version ids are immutable, so a cached version is not stale.

Reducing repeat downloads, and giving the cache an identity that includes the connection, are follow-on work and are not part of this release.

## Security notes

**`connect_args` is reproduced verbatim.** Whatever you pass is stored on the board, and both `board_deparse()` and ordinary R serialization reproduce it. A password or token in `connect_args` will appear in deparsed reconstruction code, in saved sessions, in `.RData` files, and in anything else that serializes the board:

``` r
board <- board_sf_stage(conn, stage = "@~", connect_args = list(PWD = "hunter2"))
deparse(board_deparse(board))
#> ... PWD = "hunter2" ...
```

Prefer arguments that are not secret, or that name an environment variable rather than carrying its value. `PRIV_KEY_FILE` exposes the key's *location*, not its contents, which is usually the better trade. Ordinary board printing does not show these values; deparsing and serializing do.

**Anyone who can write to the stage can author what you read.** A metadata marker establishes publication under this protocol, not authenticity. Another writer to the same prefix can replace payloads or author version metadata. `pin_read()` does not verify an independently trusted content hash unless you supply one. Treat write access to a board's prefix as equivalent to trust in its contents.

**Publication uncertainty is not signalled for every failure.** If the metadata upload reaches Snowflake but its response is lost, you get an ordinary transport error rather than the "publication uncertain" guidance. Nothing is deleted automatically and old versions are preserved, but recovery code cannot always tell an uncertain publication from a definite failure. Check `pin_versions()` after an interrupted write.

## Troubleshooting

<details>

<summary>"Driver not found" error</summary>

**Windows:** - Run `odbcad32` and verify `SnowflakeDSIIDriver` appears in the Drivers tab - Try setting `PINS_SF_DRIVER` environment variable explicitly

**Linux:** - Verify `ODBCSYSINI` points to the directory containing `odbcinst.ini` - Check that the driver path in `odbcinst.ini` is correct - Confirm the Snowflake ODBC driver is installed: `ls /usr/lib/snowflake/odbc/lib/libSnowflake.so`

</details>

<details>

<summary>"Authentication failed" error</summary>

- Verify the public key is registered in Snowflake:

  ``` sql
  DESC USER your_username;
  ```

  Look for `RSA_PUBLIC_KEY` property

- Check that `PINS_SF_PRIVATE_KEY_FILE` points to the correct file

- Ensure the private key was generated **without a passphrase**

- Verify your username matches exactly (case-sensitive)

</details>

<details>

<summary>"Warehouse does not exist" error</summary>

- Verify the warehouse name matches exactly (case-sensitive)

- Check you have `USAGE` privilege on the warehouse:

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

Two suites:

- **offline tests** -- no Snowflake connection, no network, no credentials. These are the ones you run while developing.
- **integration tests** -- talk to a real Snowflake account, create objects and delete them again.

Run the offline suite:

``` sh
Rscript --vanilla -e '.libPaths(c("rv/library/4.5/x86_64/noble", .libPaths())); devtools::test()'
```

Integration tests are **opt-in**. They skip unless you set `PINS_SF_RUN_INTEGRATION=true`, in addition to the usual `PINS_SF_*` credentials:

``` sh
PINS_SF_RUN_INTEGRATION=true Rscript -e 'devtools::test()'
```

Having credentials in your environment is deliberately **not** enough to trigger them. R reads `.Renviron` on startup, so any R process launched from this directory has working Snowflake credentials in scope whether or not that was intended; these tests write to and delete from a real stage, so they require an explicit opt-in that nothing sets by accident. Pass the variable on the command line, as above, rather than exporting it into your shell.

Each integration test works inside its own unique stage prefix and deletes that prefix afterwards.

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

Contributions are welcome. Please feel free to submit a Pull Request.

## License

MIT License - see [LICENSE](LICENSE) file for details.

## Acknowledgments

This package extends the [pins](https://github.com/rstudio/pins-r) package by Posit Software, PBC. The Snowflake stage board implementation follows the design patterns established by the cloud board implementations in the main pins package. Any mistakes in the pinsExtras package are the author's alone.

### Citing pins

If you use pinsExtras in your work, please also cite the pins package:

> Silge J, Wickham H, Luraschi J (2025). *pins: Pin, Discover, and Share Resources*. R package version 1.4.1, <https://pins.rstudio.com/>.

BibTeX entry:

``` bibtex
@Manual{pins,
  title = {pins: Pin, Discover, and Share Resources},
  author = {Julia Silge and Hadley Wickham and Javier Luraschi},
  year = {2025},
  note = {R package version 1.4.1},
  url = {https://pins.rstudio.com/},
}
```

------------------------------------------------------------------------

**Note:** This is a standalone package and is not affiliated with or endorsed by Posit Software, PBC or Snowflake Inc. Hex image by Nano Banana!