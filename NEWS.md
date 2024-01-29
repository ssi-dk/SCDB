# SCDB (development version)

## BREAKING CHANGES:

* Table identification is now more specific (#??):

  Most SCDB functions allow for tables to be specified by a character representation of "schema.table".

  Before, if no schema was implied in this context, SCDB would attempt to match the table among both
  permanent and temporary tables.

  Now, it will always assume that a lack of schema means the default schema should be used.
  This is also the case if `DBI::Id()` is used without a schema specification.

* The `show_temporary` argument of `get_tables()` is now a simple logical (#??).

  In addition, schema is always returned in the list of table (no longer NA for default schema).

* Tables created with `create_table()` will now be temporary or permanent dependent on the default value of
  `DBI::dbWriteTable()` (#??).

  If you wish to overwrite this, use `...` arguments which are passed to `DBI::dbWriteTable()`.

* If a `SQLiteConnection` is passed to `get_schema()`, the returned schema will always be "main" (#??).

## Features
* The S3 method `as.character.Id()` is added which converts `DBI::Id()` to `character` (#??).

## Improvements and Fixes

* Improvements for `create_table()` (#??):
  - now writes the table if a remote connection is given. Before, it would only create the
  table with corresponding columns.
  - can now create temporary tables for Microsoft SQL Server.

* Improved checks on `get_connection()` (#83):
  - If given, `host` does not need to look like an IP address (e.g. "localhost" is not unrealistic).
  - A `character` input for `port` is allowed if it is a string of digits.
  - Now checks if `timezone` and `timezone_out` is an IANA time zone.

* `get_connection()` now checks the value of any `timezone` and `timezone_out` arguments (#83).

* `table_exists()` now correctly gives ambiguity warning on Microsoft SQL Server and PostgreSQL backends (#80).

* `get_tables()` now supports temporary tables for Microsoft SQL Server (#??).

* Fixed dplyr joins failing if `testthat` is not installed (#90).

## Testing

Added missing tests for `create_logs_if_missing()` (#??).

Improved tests for `get_tables()`, `table_exists()`, and `create_table()` (#??).

# SCDB 0.3

* Added support for Microsoft SQL Server using ODBC (#77).

## Minor Improvements and Fixes

* Implementation of `*_join`s improved, now extending `dplyr::*_join`s rather than masking them (#77).

* Added S3 method for `id.tbl_dbi`, returning a `DBI::Id()` instance matching the table (#72).
  - Calling `id` on a `tbl_dbi` thus allows to retrieve a `schema` even when not
    initially given.

* Fixed `update_snapshot()` not working with a `DBI::Id()` instance as `db_table` argument (#72).

* Suppressed recurring messages from dbplyr >= 2.4.0 about table names containing `.` (#72).

* Added `show_temp` option to `get_tables()` to allow retrieving temporary tables (#72).

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
