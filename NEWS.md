# pinsExtras 0.1.0

First release. `board_sf_stage()` stores pins in a Snowflake internal stage. This release is mostly about two things: asking Snowflake for far less, and never leaving a pin in a state where it cannot be read.

## Far fewer requests

Reading a pin used to list the entire stage several times over. Every read path is now scoped to the one pin it is about, and the listings it does issue return only that pin's rows.

Measured live against a Snowflake user stage, five repetitions, medians:

| operation | explicit `LIST`s | rows returned by those listings | median |
|------------------|------------------|------------------|------------------|
| `pin_meta()` | 4 -\> **1** | 28 -\> **2** | 0.88s -\> **0.54s** |
| `pin_read()` | 4 -\> **1** | 28 -\> **2** | 1.32s -\> **0.99s** |
| `pin_versions()` | 2 -\> **1** | 18 -\> **4** | 0.23s -\> **0.13s** |
| `pin_write()`, existing pin | 7 -\> **2** | 49 -\> **4** | 2.11s -\> **1.54s** |
| `pin_write(versioned = FALSE)` | 7 -\> **4** | 49 -\> **7** | 2.16s -\> 2.19s |
| all eight benchmarked operations | 28 -\> **13** | 200 -\> **27** | 8.05s -\> **6.86s** |

Those row counts are the *best* case for the old behaviour: the benchmark stage held 5 files. Because the old code listed the whole stage, its cost grew with everything stored there, while the new code's does not. On a stage holding 1164 files the old row counts would be roughly a hundred times larger and the new ones unchanged.

`pin_exists()` and `pin_list()` were already one listing each and still are; they now return fewer rows.

## A pin version exists only when its metadata does

Publication writes payload files first and the `data.txt` metadata marker **last**. A version counts as published only once that marker exists, and every discovery path — `pin_list()`, `pin_exists()`, `pin_versions()`, `pin_meta()`, `pin_fetch()` — applies that same definition.

The practical consequence: an interrupted write leaves files on the stage but no version that anything can see. Readers never observe a half-written version, and a failed write cannot shadow the good version that preceded it.

## Writes that collide, and writes that replace

Two writers producing the same version id for the same pin is now an error rather than a silent merge of two payloads into one directory. The contract is per pin and serialized: concurrent writes to *different* pins are fine; concurrent writes to the *same* pin are not supported, and the failure is visible instead of quiet.

Versioned and unversioned behaviour follows upstream pins. Unversioned replacement uploads the new version **before** removing the old one, so there is never a moment when the pin does not exist. If cleanup of the old version cannot be confirmed afterwards, publication still succeeds and you get a warning naming the exact version to read (class `pinsExtras_cleanup_incomplete`) — a completed write is never turned into an error by a cleanup problem.

## Deletion happens exactly where you asked

Every delete is scoped so that a pin named `cars` cannot reach a pin named `cars_extra`, and removing one file cannot remove a similarly named sibling. `pin_delete()` and `pin_version_delete()` reject empty names and versions rather than acting on them.

`pin_delete()` processes names in order and deletes as it goes, matching upstream pins. **There is no all-or-nothing guarantee across a vector of names**: `pin_delete(board, c("cars", ""))` deletes `cars` and then errors on the empty string.

## Removing what discovery cannot see

Because a version without `data.txt` is not published, `pin_delete()` reports such a pin as absent — "Can't find pin called ...". That is deliberate, and `pin_version_delete()` is the way out: it does no listing and no existence check, and deletes the version directory directly.

``` r
# A write was interrupted and left files behind with no metadata marker.
# pin_delete() cannot see them:
pin_delete(board, "cars")
#> Error: Can't find pin called "cars"

# Remove the orphaned version directory directly:
pin_version_delete(board, "cars", "20250101T120000Z-abcde")
```

Version directories whose names are malformed are ignored by discovery rather than reported as versions; `pin_version_delete()` removes those too.

## Downloads that are checked

Every transfer is validated before its result is used. Uploads never overwrite (`OVERWRITE=FALSE`), so a retry into a directory holding a failed attempt's files fails loudly instead of mixing two attempts together. A download that does not arrive is an error, not a stale cached file quietly returned in its place.

## Three things Snowflake does that the adapter got wrong

All three were found by running against a real Snowflake stage, and none of them was visible to offline tests. They are recorded here because they changed user-visible behaviour.

- **Listing matched by prefix.** Listing the pin `cars` also returned rows belonging to a pin named `cars_extra`. Listings are now scoped with a trailing slash.

- **File deletion matched nothing.** The pattern used to delete a single file was anchored against the full staged path, but Snowflake applies it relative to the location named in the command, so it matched nothing and deleted nothing. The visible symptom was unversioned replacement leaving both versions in place and warning that cleanup was incomplete.

- **Downloads matched by prefix.** Fetching a file named `report` also matched `report.pdf` in the same version, returned two results, and aborted the read. A pin uploaded with both files could not be read back. Downloads now name the directory and select the one file by pattern.

## Testing

Integration tests talk to a real Snowflake account, create objects, and delete them. They are opt-in and skip unless you ask for them:

``` sh
PINS_SF_RUN_INTEGRATION=true Rscript -e 'devtools::test()'
```

Credentials being present in your environment is not enough, and that is on purpose: R reads `.Renviron` on startup, so credentials are in scope for any R process started in the project directory.

The offline suite needs no credentials and no network:

``` sh
Rscript --vanilla -e '.libPaths(c("rv/library/4.5/x86_64/noble", .libPaths())); devtools::test()'
```

This release also adds `tests/testthat.R`, without which `R CMD check` ran no tests at all.