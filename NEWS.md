# SCDB (development version)

## Minor Improvements and Fixes

* Improved checks on `get_connection()` (#83):
  - If given, `host` does not need to look like an IP address (e.g. "localhost" is not unrealistic).
  - A `character` input for `port` is allowed if it is a string of digits.
  - Now checks if `timezone` and `timezone_out` is an IANA time zone.

* `get_connection()` now checks the value of any `timezone` and `timezone_out` arguments.

* `table_exists()` now correctly gives ambiguity warning on Microsoft SQL Server and PostgreSQL backends (#80).

* Fixed dplyr joins failing if `testthat` is not installed (#90).

# SCDB 0.3

* Added support for Microsoft SQL Server using ODBC (#77).

## Minor Improvements and Fixes

* Implementation of `*_join`s improved, now extending `dplyr::*_join`s rather than masking them (#77).

* Added S3 method for `id.tbl_dbi`, returning a `DBI::Id()` instance matching the table (#72).
  - Calling `id` on a `tbl_dbi` thus allows to retrieve a `schema` even when not
    initially given.

* Fixed `update_snapshot()` not working with a `DBI::Id()` instance as `db_table` argument (#72).

* Suppressed recurring messages from dbplyr >= 2.4.0 about table names containing `.`.

* Added `show_temp` option to `get_tables()` to allow retrieving temporary tables.

## Other news

* Maintainer changed to Rasmus Skytte Randløv (@RasmusSkytte).

# SCDB 0.2.1

## Minor Improvements and Fixes

* SQLite connections now support schemata similar to other backends (@marcusmunch, #67).

* The package logo has been slightly altered to have a readable clock (@RasmusSkytte, #49).

* Added a vignette describing the concept of a slowly changing dimension using examples (@marcusmunch, #53).

* Added a `Logger$finalize` method, which removes the `log_file` in the DB when not writing to a file (@marcusmunch, #66).

## Other news

* Maintainer changed to Marcus Munch Grünewald @marcusmunch (@RasmusSkytte, #59)

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
