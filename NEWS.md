# SCDB (development version)

## Minor improvements and fixes

* Package logo slightly altered to have a readable clock (@RasmusSkytte, #49)
* Added a vignette describing the concept of a slowly changing dimension using examples (@marcusmunch, #53)

# SCDB 0.2

## Breaking changes

* `update_snapshot` now take a `Logger` object through the `logger` argument instead of `log_path` and `log_table_id` arguments (@marcusmunch, #24)

* `Logger\$log_filename` has been changed to `Logger\$log_basename` to reduce ambiguity

## New features

* Package functions are now also tested with `RPostgres::Postgres()`, which is
  therefore now *officially* supported (@marcusmunch, #31)

* `get_connection` shows a warning if an unsupported backend is used (@marcusmunch, #26)

* Increased flexibility for the `Logger` object (@marcusmunch, #21 #24)
  - A `Logger` instance may now be created with no arguments
  - Suppress console output with `output_to_console` (`TRUE` by default)
  - If no `log_path` is set, `Logger` does not fail before trying to write to a file
  - `Logger\$log_realpath` gives the full path to log file being written

## Minor improvements and fixes

* `schema_exists` correctly detects a schema with no tables (@marcusmunch, #30)

* `db_timestamps` now newer calls `translate_sql` with `con = NULL` (@marcusmunch, #37)

* Package description has been updated to not use a footnote on CRAN

## Known issues

* As `schema_exists` on an empty schema tests by creating a new table, this may
  cause issues if the user does not have sufficient privileges.

# SCDB 0.1

## Features

* Functions to handle DB connections
  - `get_connection`, `close_connection`, `id`

* Functions to interface with DB
  - `get_tables`, `table_exists`, `get_schema`, `schema_exists`

* Functions to create "historical" tables and logs
  - `create_table`, `create_logs_if_missing`

* Function to maintain "historical" tables
  - `update_snapshot`

* Functions to interface with "historical" tables
  - `get_table`, `slice_time`, `is.historical`

* Functions to facilitate faster joins with NAs on SQL backends
  - `full_join`, `inner_join`, `left_join`, `right_join`

* Functions to manipulate tables on SQL backends
  - `filter_keys`, `unite.tbl_dbi`, `interlace_sql`

* A logging object to facilitate logging
  - `Logger`

* Function to generate checksums
  - `digest_to_checksum`

* Function to write timestamps to tables on SQL backends
  - `db_timestamp`

* Helper functions
  - `nrow` - DB compliant `nrow`
  - `%notin%` - negated `%in%`

## Testing

* Most package functions are tested here

## Documentation

* The functions are fully documented
