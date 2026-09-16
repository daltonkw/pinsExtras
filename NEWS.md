# pinsExtras 0.1.3

Test suite consolidation. No user-facing behaviour changes.

* The test suite was re-read as a whole and cut from 385 `test_that()`
  blocks to 66, keeping only tests that guard a contract with Snowflake,
  pins or the user: one table-driven test per validator matrix instead of
  one block per cell, the per-verb request-count file folded into the
  tests that already assert each operation's exact command sequence, and
  helper-level tests dropped where a public-method test already reaches
  the helper.
* Four gaps the volume had hidden are now tested: the last `PUT` of an
  unversioned replace precedes its first `REMOVE`; nothing is removed when
  a metadata upload is uncertain; the identical-hash skip in `pin_write()`
  is exercised with real metadata; and `board_deparse()` round-trips
  offline.
* Two helpers nothing called, `sf_stage_exists()` and `sf_end_with_slash()`,
  are removed along with their tests.
* The opt-in live suite now checks only what a real stage can prove
  (accepted SQL shapes, prefix scoping of siblings, a dotted name in a
  `PATTERN`, an unversioned replace, a named stage, a closed connection)
  and no longer pays a Snowflake round trip to re-test `saveRDS()` and
  `yaml`.
* Known conformance gaps, found by running pins' own board conformance
  helpers against this board and not fixed in this release: missing-pin,
  missing-version and invalid-name errors are raised without pins' classed
  conditions (`pins_pin_missing`, `pins_pin_version_missing`,
  `pins_check_name`), and downloaded cache files are not made read-only.

# pinsExtras 0.1.1

* A `PUT` or `GET` response whose `target`, `file`, or `status` value is
  missing (`NA`) is now reported with the same classed condition as any other
  uninterpretable response: `pinsExtras_upload_failed` for a payload upload,
  `pinsExtras_publication_uncertain` for the metadata upload, and
  `pinsExtras_download_failed` for a download. Previously such a row raised
  R's unclassed "missing value where TRUE/FALSE needed" error, which lost the
  recovery guidance for an ambiguous publication. Reported by the Codex PR
  reviewer on #1 and by SEC-08 of the 0.1.0 security review.

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

## Security hardening

An independent security review of the release candidate found twelve issues.
The ones that changed behaviour are fixed here; the rest are documented in
`?board_sf_stage` and the README so you can judge them for yourself.

* **A version argument could delete a whole board.** `pin_version_delete()`
  accepted any string, and `"/"` collapsed to a delete of every version, or
  of the entire board prefix. Pin names and version ids supplied to
  `pin_delete()` and `pin_version_delete()` must now be a single path
  segment: no separators, no `.` or `..`. Malformed-but-safe version names
  such as `bogus-def12` still delete, because removing an orphaned version
  directory depends on it.

* **Names discovered from the stage could escape the local cache.** A pin or
  version whose name contained `..` or a separator produced a cache path
  outside the board's cache directory. Such entries are now dropped from
  discovery: they do not appear in `pin_list()` or `pin_versions()` and
  cannot be read.

* **Downloaded metadata could redirect a read.** The `file` field in a pin's
  metadata was used as given. Metadata naming `../../private.csv` would
  fetch one file and return the name of another, which the reader then
  resolved against your cache. The field must now be a list of plain file
  names, each used exactly once, and anything else is an error rather than
  being quietly trimmed.

* **A write that could not be read could delete the version it replaced.**
  Metadata whose timestamp does not parse produces a version discovery
  cannot resolve. Such a write was accepted, uploaded, and allowed to remove
  the previous version, leaving a pin with nothing readable in it. This is
  now rejected before anything is uploaded.

* **"Delete one file" could match files in subdirectories.** The patterns
  used to fetch or remove a single file matched that name anywhere below the
  location, so a nested file with the same name could be fetched instead of
  the one asked for, or deleted alongside it. Both patterns are now anchored
  to the exact directory.

* **Recovery suggestions could carry extra code.** The reconnection hint and
  the incomplete-cleanup warning build R code for you to copy. They pasted
  the board path and pin name into that code as text, so a value containing
  a quote could add a second statement to what you copied. Both are now
  built as R expressions, so a value is always a string literal.

* **Credential files could be packaged.** `R CMD build` does not read
  `.gitignore`, so files such as `.env`, `*.pem` and `odbc.ini` in a
  developer's checkout were eligible for a source tarball. They are now
  excluded from the build as well.

* **CI actions are pinned** to commit SHAs rather than moving tags, and the
  workflow's permissions are narrowed to reading the repository.

Documented rather than changed, because they are properties of the design
rather than defects in it: `connect_args` is reproduced by `board_deparse()`
and by serialization, so secrets in it travel; the cache directory is keyed
by stage text and board path only, so it must not be shared across accounts;
cache directories use ambient permissions; and the one-mutator-at-a-time
contract covers deleters as well as writers, with no snapshot isolation for
readers.

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