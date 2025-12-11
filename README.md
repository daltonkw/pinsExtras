# pinsExtras

Extra boards for the [`pins`](https://pins.rstudio.com) package. Current focus: a Snowflake stage board that stores pins in a Snowflake internal stage via ODBC.

## Installation

```r
# Install dependencies
install.packages(c("DBI", "odbc", "pins"))
```
```r
# Install pinsExtras from local source
install.packages("path/to/pinsExtras", repos = NULL, type = "source")
```

## Quick Start

```r
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

# Create board
board <- board_sf_stage(conn, stage = "@~", path = "my-pins")

# Use standard pins verbs
pin_write(board, mtcars, "my-cars")
pin_read(board, "my-cars")
pin_versions(board, "my-cars")
pin_delete(board, "my-cars")

# Clean up
DBI::dbDisconnect(conn)
```

---

## Setup Instructions

### Windows 11

#### Step 1: Install Snowflake ODBC Driver

1. Download from https://developers.snowflake.com/odbc/ (Windows 64-bit `.msi`)
2. Run the installer with default settings
3. Verify: Press `Win + R` → type `odbcad32` → **Drivers** tab → look for `SnowflakeDSIIDriver`

#### Step 2: Set Up JWT Authentication

Generate a key pair (requires OpenSSL - install via `winget install OpenSSL` if needed):

```powershell
# Create directory
mkdir "$env:USERPROFILE\.snowflake" -Force

# Generate private key
openssl genrsa 2048 | openssl pkcs8 -topk8 -nocrypt -out "$env:USERPROFILE\.snowflake\rsa_key.p8"

# Generate public key
openssl rsa -in "$env:USERPROFILE\.snowflake\rsa_key.p8" -pubout -out "$env:USERPROFILE\.snowflake\rsa_key.pub"

# Display public key (copy this for Snowflake)
Get-Content "$env:USERPROFILE\.snowflake\rsa_key.pub"
```

Register the public key in Snowflake:

```sql
ALTER USER your_username SET RSA_PUBLIC_KEY='MIIBIjANBgkq...your_public_key...';
```

#### Step 3: Set Environment Variables

1. Press `Win + R` → type `sysdm.cpl` → **Advanced** → **Environment Variables**
2. Add these **User variables**:

| Variable | Example Value |
|----------|---------------|
| `PINS_SF_SERVER` | `acct-id.snowflakecomputing.com` |
| `PINS_SF_USER` | `MYUSERNAME` |
| `PINS_SF_AUTHENTICATOR` | `SNOWFLAKE_JWT` |
| `PINS_SF_PRIVATE_KEY_FILE` | `C:\Users\You\.snowflake\rsa_key.p8` |
| `PINS_SF_WAREHOUSE` | `COMPUTE_WH` |

Optional:

| Variable | Description |
|----------|-------------|
| `PINS_SF_DATABASE` | Default database |
| `PINS_SF_SCHEMA` | Default schema |
| `PINS_SF_ROLE` | Snowflake role |
| `PINS_SF_STAGE` | Stage name (default: `@~`) |
| `PINS_SF_DRIVER` | Override driver name |

3. Click OK and **restart R/RStudio**.

---

### Linux (Ubuntu/Debian)

#### Step 1: Install Snowflake ODBC Driver

```bash
# Download and install (check Snowflake docs for latest version)
wget https://sfc-repo.snowflakecomputing.com/odbc/linux/latest/snowflake-odbc-3.13.0.x86_64.deb
sudo dpkg -i snowflake-odbc-3.13.0.x86_64.deb

# Fix library symlink if needed (Ubuntu 24.04)
sudo ln -sf /usr/lib/x86_64-linux-gnu/libodbcinst.so.2 /usr/lib/x86_64-linux-gnu/libodbcinst.so.1
```

#### Step 2: Register the Driver

Create `odbcinst.ini` in your project or `/etc/`:

```ini
[Snowflake]
Driver = /usr/lib/snowflake/odbc/lib/libSnowflake.so
```

If using a project-local file, set `ODBCSYSINI` to its directory:

```bash
export ODBCSYSINI=/path/to/project
```

#### Step 3: Set Up JWT Authentication

```bash
mkdir -p ~/.snowflake

# Generate keys
openssl genrsa 2048 | openssl pkcs8 -topk8 -nocrypt -out ~/.snowflake/rsa_key.p8
openssl rsa -in ~/.snowflake/rsa_key.p8 -pubout -out ~/.snowflake/rsa_key.pub

# Display public key for Snowflake registration
cat ~/.snowflake/rsa_key.pub
```

Register in Snowflake:

```sql
ALTER USER your_username SET RSA_PUBLIC_KEY='MIIBIjANBgkq...';
```

#### Step 4: Set Environment Variables

Add to `~/.bashrc` or `~/.profile`:

```bash
export PINS_SF_SERVER="acct-id.snowflakecomputing.com"
export PINS_SF_USER="MYUSERNAME"
export PINS_SF_AUTHENTICATOR="SNOWFLAKE_JWT"
export PINS_SF_PRIVATE_KEY_FILE="$HOME/.snowflake/rsa_key.p8"
export PINS_SF_WAREHOUSE="COMPUTE_WH"
# Optional: export ODBCSYSINI="/path/to/project"
```

---

## Environment Variables Reference

| Variable | Required | Description |
|----------|----------|-------------|
| `PINS_SF_SERVER` | Yes | Snowflake account URL (e.g., `acct.snowflakecomputing.com`) |
| `PINS_SF_USER` | Yes | Snowflake username |
| `PINS_SF_AUTHENTICATOR` | Yes | Auth method (`SNOWFLAKE_JWT` recommended) |
| `PINS_SF_PRIVATE_KEY_FILE` | Yes | Path to private key file |
| `PINS_SF_WAREHOUSE` | Yes | Compute warehouse name |
| `PINS_SF_DATABASE` | No | Default database |
| `PINS_SF_SCHEMA` | No | Default schema |
| `PINS_SF_ROLE` | No | Snowflake role to use |
| `PINS_SF_STAGE` | No | Stage name (default: `@~` user stage) |
| `PINS_SF_DRIVER` | No | Override ODBC driver name |
| `ODBCSYSINI` | No | Linux only: path to `odbcinst.ini` directory |

---

## Troubleshooting

### "Driver not found"

- **Windows**: Run `odbcad32` and verify `SnowflakeDSIIDriver` appears in Drivers tab
- **Linux**: Check `ODBCSYSINI` points to directory containing `odbcinst.ini`
- **Override**: Set `PINS_SF_DRIVER` environment variable explicitly

### "Authentication failed"

- Verify public key is registered: `DESC USER your_username;`
- Check private key path is correct and file is readable
- Ensure key was generated without a passphrase

### "Warehouse does not exist"

- Verify warehouse name matches exactly (case-sensitive)
- Check you have USAGE privilege on the warehouse

---

## Tests

Integration tests in `tests/testthat/test-board_sf_stage.R` are skipped unless `PINS_SF_*` environment variables are set. They perform full read/write/version/upload/manifest/delete flows and clean up after themselves.

Run tests:

```r
devtools::test()
```

---

## Roadmap

- Keep as standalone package; do not wire into pins core
- Add support for external stages (S3/Azure-backed)
- Connection pooling for high-throughput use cases
