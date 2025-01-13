# SCDB (development version)

## New features

* Added function `create_index()` to allow easy creating of an index on a table (#137).

## Improvements and Fixes

* `update_snapshot()` has been optimized and now runs faster on all the supported backends (#137).

* `*_joins()` can now take `dplyr::join_by()` as `by` argument when no `na_by` argument is given (#156).

* `SCDB` has been made backwards compatibility to R >= 3.6 (#164).

* `nrow()` now always returns integers (#163).

## Documentation

* A vignette including benchmarks of `update_snapshot()` across various backends is added (#138).


# SCDB 0.4.1

## Improvements and Fixes

* `Logger` now correctly writes to the "catalog" field on backends that support it (#149).

* `get_schema()` now correctly returns the temporary schema on PostgreSQL backends (#139).

* `get_tables()` now returns catalog on DuckDB backends (#145).

* `unique_table_names()` now uses random alphanumerics to form the unique name instead of tracking via options (#158).

## Documentation
* Deprecated `check_from` argument no longer used in `dbplyr` calls (#136).

## Testing

* Improved tests for `get_tables()` (#145).


# SCDB 0.4.0

## BREAKING CHANGES:

* Table identification is now more specific (#93).

  Most SCDB functions allow for tables to be specified by a character representation of "[catalog].schema.table".

  Before, if no schema was implied in this context, SCDB would attempt to match the table among both permanent and temporary tables.

  Now, it will always assume that a lack of schema means the default schema should be used.
  This is also the case if `DBI::Id()` is used without a schema specification.

* The `show_temporary` argument of `get_tables()` is now a simple logical (#93).

  In addition, schema is always returned in the list of tables (no longer NA for default schema).

* Tables created with `create_table()` will now be temporary or permanent dependent on the default value of
  `DBI::dbCreateTable()` (#93).

  If you wish to overwrite this, use `...` arguments which are passed to `DBI::dbCreateTable()`.

* The `%notin%` operator has been removed from the package (#96).

* The `db_table_id` argument in `create_table()`, `get_table()`, `table_exists()` and `id()` is renamed to `db_table`
  (#115).
  Any object coercible by `id()` can now be passed to these functions.

* The order of arguments in `create_logs_if_missing()` has been swapped to match the rest of the package (#96).
  The `conn` argument is now before the `log_table` argument.

* The arguments of `Logger` has been updated (#98):
  * `db_tablestring` is replaced with the `db_table` argument.

    This argument takes any input coercible by `id()` instead of only allowing a character string.

  * `ts` is replaced with the `timestamp` argument to align with `update_snapshot()`.

* The order of input arguments to `Logger` is changed (#98).

* The `interlace_sql()` function is deprecated in favor of the S3 generic `interlace()` (#113).

## New features

* Added support for DuckDB (#121).

* The S3 method `as.character.Id()` is added which converts `DBI::Id()` to `character` (#93).

* A new `id.data.frame()` which converts `data.frame` to `DBI::Id()` (#108).
  Useful in combination with `get_tables(conn, pattern)`.

* A new `get_catalog()` function is added to give more specific table identification (#99).

* A new clean up function, `defer_db_cleanup()`, is added (#89).

  By passing a `tbl_sql` object to this function, the corresponding table will be deleted once
  the parent function exits.

* A new function, `unique_table_name()`, to generate unique table names is added (#89).
  This function is heavily inspired by the unexported `dbplyr:::unique_table_name()`.

* A logger is introduced `LoggerNull` (#98):

  * `Logger` facilitates logging to file/console and logging to database.

  * `LoggerNull` is "no-logging" logger that can be used to suppress all logging.

* Added a set of helper functions to prevent race conditions when writing to data bases (#104).

  See `lock_table()` and `unlock_table()`.

## Improvements and Fixes

* Improvements for `create_table()` (#93):

  - now writes the table if a remote connection is given. Before, it would only create the
  table with corresponding columns.

  - can now create temporary tables for Microsoft SQL Server.

* Improved checks on `get_connection()` (#83):

  - If given, `host` does not need to look like an IP address (e.g. "localhost" is not unrealistic).

  - A `character` input for `port` is allowed if it is a string of digits.

  - Now checks if `timezone` and `timezone_out` is an IANA time zone.

* `digest_to_checksum()` has improved performance on Microsoft SQL Server by use of the built-in `HashBytes` function (#97).

* `table_exists()` now correctly gives ambiguity warning on Microsoft SQL Server and PostgreSQL backends (#80).

* `get_tables()` now supports temporary tables for Microsoft SQL Server (#93).

* `get_schema()` has been updated (#107):

  * It will now always return a schema (either directly from the object or inferred by `id()`).

  * A `temporary` argument is added to get the temporary schemas from `DBIConnections`.

* `id()` now includes information of catalog in more cases (#99, #107).

* Fixed dplyr joins failing if `testthat` is not installed (#90).

* The footprint of `update_snapshot()` is reduced by cleaning up intermediate tables with `defer_db_cleanup()` (#89)

* `update_snapshot()` now attempts to get a lock on the table being updated before updating (#104).

* `Logger$log_info()` now uses `message()` instead of `cat()` to write to console (#98).
  The message written is now also returned invisibly.

## Testing

* Added missing tests for `create_logs_if_missing()` (#93).

* Added missing tests for `get_schema()` (#99).

* Added missing tests for `get_catalog()` (#107).

* Improved tests for `get_tables()`, `table_exists()`, and `create_table()` (#93).

* Improved tests for `Logger` (#98).


# SCDB 0.3

* Added support for Microsoft SQL Server using ODBC (#77).

## Minor Improvements and Fixes

* Implementation of `*_join`s improved, now extending `dplyr::*_join`s rather than masking them (#77).

* Added S3 method for `id.tbl_dbi()`, returning a `DBI::Id()` instance matching the table (#72).

  - Calling `id()` on a `tbl_dbi` thus allows to retrieve a `schema` even when not initially given.

* Fixed `update_snapshot()` not working with a `DBI::Id()` instance as `db_table` argument (#72).

* Suppressed recurring messages from dbplyr >= 2.4.0 about table names containing `.` (#72).

* Added `show_temp` option to `get_tables()` to allow retrieving temporary tables (#72).

## Other news

* Maintainer changed to Rasmus Skytte Randløv (@RasmusSkytte).

# SCDB 0.2.1

## Minor Improvements and Fixes

* SQLite connections now support schemata similar to other backends (#67).

* The package logo has been slightly altered to have a readable clock (#49).

* Added a vignette describing the concept of a slowly changing dimension using examples (#53).

* Added a `Logger$finalize` method, which removes the `log_file` in the database when not writing to a file (#66).

## Other news

* Maintainer changed to Marcus Munch Grünewald (#59).


# SCDB 0.2

## Breaking changes

* `update_snapshot()` now take a `Logger` object through the `logger` argument instead of `log_path` and `log_table_id` arguments (#24).

* `Logger\$log_filename` has been changed to `Logger\$log_basename` to reduce ambiguity.

## New features

* Package functions are now also tested with `RPostgres::Postgres()`, which is
  therefore now *officially* supported (#31).

* `get_connection()` shows a warning if an unsupported backend is used (#26).

* Increased flexibility for the `Logger` object (#21 #24):

  - A `Logger` instance may now be created with no arguments.

  - Suppress console output with `output_to_console` (`TRUE` by default).

  - If no `log_path` is set, `Logger` does not fail before trying to write to a file.

  - `Logger\$log_realpath` gives the full path to log file being written.

## Minor improvements and fixes

* `schema_exists` correctly detects a schema with no tables (#30).

* `db_timestamps` now newer calls `translate_sql` with `con = NULL` (#37).

* Package description has been updated to not use a footnote on CRAN.

## Known issues

* As `schema_exists` on an empty schema tests by creating a new table, this may
  cause issues if the user does not have sufficient privileges.


# SCDB 0.1

## Features

* Functions to handle database connections:

  - `get_connection()`, `close_connection()`, `id()`

* Functions to interface with database:

  - `get_tables()`, `table_exists()`, `get_schema()`, `schema_exists()`

* Functions to create "historical" tables and logs:

  - `create_table()`, `create_logs_if_missing()`

* Function to maintain "historical" tables:

  - `update_snapshot()`

* Functions to interface with "historical" tables:

  - `get_table()`, `slice_time()`, `is.historical()`

* Functions to facilitate faster joins with NAs on SQL backends:

  - `full_join()`, `inner_join()`, `left_join()`, `right_join()`

* Functions to manipulate tables on SQL backends:

  - `filter_keys()`, `unite.tbl_dbi()`, `interlace_sql()`

* A logging object to facilitate logging:

  - `Logger()`

* Function to generate checksums:

  - `digest_to_checksum()`

* Function to write timestamps to tables on SQL backends:

  - `db_timestamp()`

* Helper functions:

  - `nrow()` - database compliant `nrow()`

  - `%notin%` - negated `%in%`

## Testing

* Most package functions are tested here.

## Documentation

* The functions are fully documented.
