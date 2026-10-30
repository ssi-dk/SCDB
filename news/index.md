# Changelog

## SCDB (development version)

## SCDB 0.6.2

CRAN release: 2026-06-22

### BREAKING CHANGE

- `SCDB` has reduced backwards compatibility to R \>= 4.2
  ([\#248](https://github.com/ssi-dk/SCDB/issues/248)).

### Improvements and Fixes

- Compatibility with `dbplyr` 2.6.0
  ([\#241](https://github.com/ssi-dk/SCDB/issues/241)).

## SCDB 0.6.1

CRAN release: 2026-05-18

### Improvements and Fixes

- `delta_export` and `delta_load` now accepts `db_table_id` of class
  `Id` without error.

- `delta_load` now always writes integer `n_insertions` and
  `n_deactivations` to the logs table
  ([\#221](https://github.com/ssi-dk/SCDB/issues/221)).

- Compatibility with `dbplyr` 2.6.0
  ([\#223](https://github.com/ssi-dk/SCDB/issues/223),
  [@hadley](https://github.com/hadley) and
  [\#225](https://github.com/ssi-dk/SCDB/issues/225)).

## SCDB 0.6.0

CRAN release: 2026-01-23

### Improvements and Fixes

- [`delta_export()`](https://ssi-dk.github.io/SCDB/reference/delta_loading.md)
  and `delta_import()` functions are added
  ([\#213](https://github.com/ssi-dk/SCDB/issues/213)) These functions
  facilitate incremental backups and batch migration of a historical
  table created with
  [`update_snapshot()`](https://ssi-dk.github.io/SCDB/reference/update_snapshot.md).
  See
  [`vignette("delta-loading")`](https://ssi-dk.github.io/SCDB/articles/delta-loading.md)
  for usage details.

- Bug fixes in
  [`update_snapshot()`](https://ssi-dk.github.io/SCDB/reference/update_snapshot.md):

  - Fixed a bug records were being incorrectly deactivated
    ([\#214](https://github.com/ssi-dk/SCDB/issues/214)).

## SCDB 0.5.2

CRAN release: 2026-01-08

### Improvements and Fixes

- Bug fixes in
  [`update_snapshot()`](https://ssi-dk.github.io/SCDB/reference/update_snapshot.md):

  - Fixed a bug where the process would unexpectedly hang
    ([\#192](https://github.com/ssi-dk/SCDB/issues/192)).

  - `filter` is copied to *both* source and target connections if on
    different connections
    ([\#201](https://github.com/ssi-dk/SCDB/issues/201)).

- [`get_tables()`](https://ssi-dk.github.io/SCDB/reference/get_tables.md)
  no longer fails for some PostgreSQL configurations
  ([\#198](https://github.com/ssi-dk/SCDB/issues/198)).

- [`filter_keys()`](https://ssi-dk.github.io/SCDB/reference/filter_keys.md)
  no longer fails if filter list has zero rows
  ([\#200](https://github.com/ssi-dk/SCDB/issues/200)).

- [`db_timestamp()`](https://ssi-dk.github.io/SCDB/reference/db_timestamp.md)
  now always casts to timestamp
  ([\#202](https://github.com/ssi-dk/SCDB/issues/202)).

## SCDB 0.5.1

CRAN release: 2025-02-27

### BREAKING CHANGES

- [`update_snapshot()`](https://ssi-dk.github.io/SCDB/reference/update_snapshot.md)
  no longer collapses continuous records by default
  ([\#183](https://github.com/ssi-dk/SCDB/issues/183),
  [@kjakobse](https://github.com/kjakobse)). To preserve this behavior,
  use `collapse_continuous_records = TRUE`. In return,
  [`update_snapshot()`](https://ssi-dk.github.io/SCDB/reference/update_snapshot.md)
  is now faster by default.

### Improvements and Fixes

- Intermediary tables are now being cleaned up
  ([\#174](https://github.com/ssi-dk/SCDB/issues/174)).

- [`update_snapshot()`](https://ssi-dk.github.io/SCDB/reference/update_snapshot.md)
  now handles the given `timestamp` more consistently
  ([\#187](https://github.com/ssi-dk/SCDB/issues/187)). Thanks to
  [@kjakobse](https://github.com/kjakobse) for discovering the issue.

- `?Logger$finalize()` is now a private method
  ([\#180](https://github.com/ssi-dk/SCDB/issues/180)). Required as of
  `R6` v2.4.0.

## SCDB 0.5.0

CRAN release: 2025-02-03

### New features

- Added function
  [`create_index()`](https://ssi-dk.github.io/SCDB/reference/create_index.md)
  to allow easy creating of an index on a table
  ([\#137](https://github.com/ssi-dk/SCDB/issues/137)).

### Improvements and Fixes

- [`update_snapshot()`](https://ssi-dk.github.io/SCDB/reference/update_snapshot.md)
  has been optimized and now runs faster on all the supported backends
  ([\#137](https://github.com/ssi-dk/SCDB/issues/137)).

- `*_joins()` can now take
  [`dplyr::join_by()`](https://dplyr.tidyverse.org/reference/join_by.html)
  as `by` argument when no `na_by` argument is given
  ([\#156](https://github.com/ssi-dk/SCDB/issues/156)).

- `SCDB` has been made backwards compatibility to R \>= 3.6
  ([\#164](https://github.com/ssi-dk/SCDB/issues/164)).

- [`nrow()`](https://ssi-dk.github.io/SCDB/reference/nrow.md) now always
  returns integers ([\#163](https://github.com/ssi-dk/SCDB/issues/163)).

- `interlace_sql` has been fully deprecated and removed
  ([\#169](https://github.com/ssi-dk/SCDB/issues/169)).

### Documentation

- A vignette including benchmarks of
  [`update_snapshot()`](https://ssi-dk.github.io/SCDB/reference/update_snapshot.md)
  across various backends is added
  ([\#138](https://github.com/ssi-dk/SCDB/issues/138)).

## SCDB 0.4.1

CRAN release: 2024-10-03

### Improvements and Fixes

- `Logger` now correctly writes to the “catalog” field on backends that
  support it ([\#149](https://github.com/ssi-dk/SCDB/issues/149)).

- [`get_schema()`](https://ssi-dk.github.io/SCDB/reference/get_schema.md)
  now correctly returns the temporary schema on PostgreSQL backends
  ([\#139](https://github.com/ssi-dk/SCDB/issues/139)).

- [`get_tables()`](https://ssi-dk.github.io/SCDB/reference/get_tables.md)
  now returns catalog on DuckDB backends
  ([\#145](https://github.com/ssi-dk/SCDB/issues/145)).

- `unique_table_names()` now uses random alphanumerics to form the
  unique name instead of tracking via options
  ([\#158](https://github.com/ssi-dk/SCDB/issues/158)).

### Documentation

- Deprecated `check_from` argument no longer used in `dbplyr` calls
  ([\#136](https://github.com/ssi-dk/SCDB/issues/136)).

### Testing

- Improved tests for
  [`get_tables()`](https://ssi-dk.github.io/SCDB/reference/get_tables.md)
  ([\#145](https://github.com/ssi-dk/SCDB/issues/145)).

## SCDB 0.4.0

CRAN release: 2024-03-21

### BREAKING CHANGES

- Table identification is now more specific
  ([\#93](https://github.com/ssi-dk/SCDB/issues/93)).

  Most SCDB functions allow for tables to be specified by a character
  representation of “\[catalog\].schema.table”.

  Before, if no schema was implied in this context, SCDB would attempt
  to match the table among both permanent and temporary tables.

  Now, it will always assume that a lack of schema means the default
  schema should be used. This is also the case if
  [`DBI::Id()`](https://dbi.r-dbi.org/reference/Id.html) is used without
  a schema specification.

- The `show_temporary` argument of
  [`get_tables()`](https://ssi-dk.github.io/SCDB/reference/get_tables.md)
  is now a simple logical
  ([\#93](https://github.com/ssi-dk/SCDB/issues/93)).

  In addition, schema is always returned in the list of tables (no
  longer NA for default schema).

- Tables created with
  [`create_table()`](https://ssi-dk.github.io/SCDB/reference/create_table.md)
  will now be temporary or permanent dependent on the default value of
  [`DBI::dbCreateTable()`](https://dbi.r-dbi.org/reference/dbCreateTable.html)
  ([\#93](https://github.com/ssi-dk/SCDB/issues/93)).

  If you wish to overwrite this, use `...` arguments which are passed to
  [`DBI::dbCreateTable()`](https://dbi.r-dbi.org/reference/dbCreateTable.html).

- The `%notin%` operator has been removed from the package
  ([\#96](https://github.com/ssi-dk/SCDB/issues/96)).

- The `db_table_id` argument in
  [`create_table()`](https://ssi-dk.github.io/SCDB/reference/create_table.md),
  [`get_table()`](https://ssi-dk.github.io/SCDB/reference/get_table.md),
  [`table_exists()`](https://ssi-dk.github.io/SCDB/reference/table_exists.md)
  and [`id()`](https://ssi-dk.github.io/SCDB/reference/id.md) is renamed
  to `db_table` ([\#115](https://github.com/ssi-dk/SCDB/issues/115)).
  Any object coercible by
  [`id()`](https://ssi-dk.github.io/SCDB/reference/id.md) can now be
  passed to these functions.

- The order of arguments in
  [`create_logs_if_missing()`](https://ssi-dk.github.io/SCDB/reference/create_logs_if_missing.md)
  has been swapped to match the rest of the package
  ([\#96](https://github.com/ssi-dk/SCDB/issues/96)). The `conn`
  argument is now before the `log_table` argument.

- The arguments of `Logger` has been updated
  ([\#98](https://github.com/ssi-dk/SCDB/issues/98)):

  - `db_tablestring` is replaced with the `db_table` argument.

    This argument takes any input coercible by
    [`id()`](https://ssi-dk.github.io/SCDB/reference/id.md) instead of
    only allowing a character string.

  - `ts` is replaced with the `timestamp` argument to align with
    [`update_snapshot()`](https://ssi-dk.github.io/SCDB/reference/update_snapshot.md).

- The order of input arguments to `Logger` is changed
  ([\#98](https://github.com/ssi-dk/SCDB/issues/98)).

- The `interlace_sql()` function is deprecated in favor of the S3
  generic
  [`interlace()`](https://ssi-dk.github.io/SCDB/reference/interlace.md)
  ([\#113](https://github.com/ssi-dk/SCDB/issues/113)).

### New features

- Added support for DuckDB
  ([\#121](https://github.com/ssi-dk/SCDB/issues/121)).

- The S3 method `as.character.Id()` is added which converts
  [`DBI::Id()`](https://dbi.r-dbi.org/reference/Id.html) to `character`
  ([\#93](https://github.com/ssi-dk/SCDB/issues/93)).

- A new
  [`id.data.frame()`](https://ssi-dk.github.io/SCDB/reference/id.md)
  which converts `data.frame` to
  [`DBI::Id()`](https://dbi.r-dbi.org/reference/Id.html)
  ([\#108](https://github.com/ssi-dk/SCDB/issues/108)). Useful in
  combination with `get_tables(conn, pattern)`.

- A new
  [`get_catalog()`](https://ssi-dk.github.io/SCDB/reference/get_schema.md)
  function is added to give more specific table identification
  ([\#99](https://github.com/ssi-dk/SCDB/issues/99)).

- A new clean up function,
  [`defer_db_cleanup()`](https://ssi-dk.github.io/SCDB/reference/defer_db_cleanup.md),
  is added ([\#89](https://github.com/ssi-dk/SCDB/issues/89)).

  By passing a `tbl_sql` object to this function, the corresponding
  table will be deleted once the parent function exits.

- A new function,
  [`unique_table_name()`](https://ssi-dk.github.io/SCDB/reference/unique_table_name.md),
  to generate unique table names is added
  ([\#89](https://github.com/ssi-dk/SCDB/issues/89)). This function is
  heavily inspired by the unexported `dbplyr:::unique_table_name()`.

- A logger is introduced `LoggerNull`
  ([\#98](https://github.com/ssi-dk/SCDB/issues/98)):

  - `Logger` facilitates logging to file/console and logging to
    database.

  - `LoggerNull` is “no-logging” logger that can be used to suppress all
    logging.

- Added a set of helper functions to prevent race conditions when
  writing to data bases
  ([\#104](https://github.com/ssi-dk/SCDB/issues/104)).

  See
  [`lock_table()`](https://ssi-dk.github.io/SCDB/reference/db_locks.md)
  and
  [`unlock_table()`](https://ssi-dk.github.io/SCDB/reference/db_locks.md).

### Improvements and Fixes

- Improvements for
  [`create_table()`](https://ssi-dk.github.io/SCDB/reference/create_table.md)
  ([\#93](https://github.com/ssi-dk/SCDB/issues/93)):

  - now writes the table if a remote connection is given. Before, it
    would only create the table with corresponding columns.

  - can now create temporary tables for Microsoft SQL Server.

- Improved checks on
  [`get_connection()`](https://ssi-dk.github.io/SCDB/reference/get_connection.md)
  ([\#83](https://github.com/ssi-dk/SCDB/issues/83)):

  - If given, `host` does not need to look like an IP address
    (e.g. “localhost” is not unrealistic).

  - A `character` input for `port` is allowed if it is a string of
    digits.

  - Now checks if `timezone` and `timezone_out` is an IANA time zone.

- [`digest_to_checksum()`](https://ssi-dk.github.io/SCDB/reference/digest_to_checksum.md)
  has improved performance on Microsoft SQL Server by use of the
  built-in `HashBytes` function
  ([\#97](https://github.com/ssi-dk/SCDB/issues/97)).

- [`table_exists()`](https://ssi-dk.github.io/SCDB/reference/table_exists.md)
  now correctly gives ambiguity warning on Microsoft SQL Server and
  PostgreSQL backends
  ([\#80](https://github.com/ssi-dk/SCDB/issues/80)).

- [`get_tables()`](https://ssi-dk.github.io/SCDB/reference/get_tables.md)
  now supports temporary tables for Microsoft SQL Server
  ([\#93](https://github.com/ssi-dk/SCDB/issues/93)).

- [`get_schema()`](https://ssi-dk.github.io/SCDB/reference/get_schema.md)
  has been updated ([\#107](https://github.com/ssi-dk/SCDB/issues/107)):

  - It will now always return a schema (either directly from the object
    or inferred by
    [`id()`](https://ssi-dk.github.io/SCDB/reference/id.md)).

  - A `temporary` argument is added to get the temporary schemas from
    `DBIConnections`.

- [`id()`](https://ssi-dk.github.io/SCDB/reference/id.md) now includes
  information of catalog in more cases
  ([\#99](https://github.com/ssi-dk/SCDB/issues/99),
  [\#107](https://github.com/ssi-dk/SCDB/issues/107)).

- Fixed dplyr joins failing if `testthat` is not installed
  ([\#90](https://github.com/ssi-dk/SCDB/issues/90)).

- The footprint of
  [`update_snapshot()`](https://ssi-dk.github.io/SCDB/reference/update_snapshot.md)
  is reduced by cleaning up intermediate tables with
  [`defer_db_cleanup()`](https://ssi-dk.github.io/SCDB/reference/defer_db_cleanup.md)
  ([\#89](https://github.com/ssi-dk/SCDB/issues/89))

- [`update_snapshot()`](https://ssi-dk.github.io/SCDB/reference/update_snapshot.md)
  now attempts to get a lock on the table being updated before updating
  ([\#104](https://github.com/ssi-dk/SCDB/issues/104)).

- `Logger$log_info()` now uses
  [`message()`](https://rdrr.io/r/base/message.html) instead of
  [`cat()`](https://rdrr.io/r/base/cat.html) to write to console
  ([\#98](https://github.com/ssi-dk/SCDB/issues/98)). The message
  written is now also returned invisibly.

### Testing

- Added missing tests for
  [`create_logs_if_missing()`](https://ssi-dk.github.io/SCDB/reference/create_logs_if_missing.md)
  ([\#93](https://github.com/ssi-dk/SCDB/issues/93)).

- Added missing tests for
  [`get_schema()`](https://ssi-dk.github.io/SCDB/reference/get_schema.md)
  ([\#99](https://github.com/ssi-dk/SCDB/issues/99)).

- Added missing tests for
  [`get_catalog()`](https://ssi-dk.github.io/SCDB/reference/get_schema.md)
  ([\#107](https://github.com/ssi-dk/SCDB/issues/107)).

- Improved tests for
  [`get_tables()`](https://ssi-dk.github.io/SCDB/reference/get_tables.md),
  [`table_exists()`](https://ssi-dk.github.io/SCDB/reference/table_exists.md),
  and
  [`create_table()`](https://ssi-dk.github.io/SCDB/reference/create_table.md)
  ([\#93](https://github.com/ssi-dk/SCDB/issues/93)).

- Improved tests for `Logger`
  ([\#98](https://github.com/ssi-dk/SCDB/issues/98)).

## SCDB 0.3

CRAN release: 2024-01-13

- Added support for Microsoft SQL Server using ODBC
  ([\#77](https://github.com/ssi-dk/SCDB/issues/77)).

### Minor Improvements and Fixes

- Implementation of `*_join`s improved, now extending `dplyr::*_join`s
  rather than masking them
  ([\#77](https://github.com/ssi-dk/SCDB/issues/77)).

- Added S3 method for `id.tbl_dbi()`, returning a
  [`DBI::Id()`](https://dbi.r-dbi.org/reference/Id.html) instance
  matching the table ([\#72](https://github.com/ssi-dk/SCDB/issues/72)).

  - Calling [`id()`](https://ssi-dk.github.io/SCDB/reference/id.md) on a
    `tbl_dbi` thus allows to retrieve a `schema` even when not initially
    given.

- Fixed
  [`update_snapshot()`](https://ssi-dk.github.io/SCDB/reference/update_snapshot.md)
  not working with a
  [`DBI::Id()`](https://dbi.r-dbi.org/reference/Id.html) instance as
  `db_table` argument
  ([\#72](https://github.com/ssi-dk/SCDB/issues/72)).

- Suppressed recurring messages from dbplyr \>= 2.4.0 about table names
  containing `.` ([\#72](https://github.com/ssi-dk/SCDB/issues/72)).

- Added `show_temp` option to
  [`get_tables()`](https://ssi-dk.github.io/SCDB/reference/get_tables.md)
  to allow retrieving temporary tables
  ([\#72](https://github.com/ssi-dk/SCDB/issues/72)).

### Other news

- Maintainer changed to Rasmus Skytte Randløv
  ([@RasmusSkytte](https://github.com/RasmusSkytte)).

## SCDB 0.2.1

CRAN release: 2023-11-30

### Minor Improvements and Fixes

- SQLite connections now support schemata similar to other backends
  ([\#67](https://github.com/ssi-dk/SCDB/issues/67)).

- The package logo has been slightly altered to have a readable clock
  ([\#49](https://github.com/ssi-dk/SCDB/issues/49)).

- Added a vignette describing the concept of a slowly changing dimension
  using examples ([\#53](https://github.com/ssi-dk/SCDB/issues/53)).

- Added a `Logger$finalize` method, which removes the `log_file` in the
  database when not writing to a file
  ([\#66](https://github.com/ssi-dk/SCDB/issues/66)).

### Other news

- Maintainer changed to Marcus Munch Grünewald
  ([\#59](https://github.com/ssi-dk/SCDB/issues/59)).

## SCDB 0.2

CRAN release: 2023-09-26

### BREAKING CHANGES

- [`update_snapshot()`](https://ssi-dk.github.io/SCDB/reference/update_snapshot.md)
  now take a `Logger` object through the `logger` argument instead of
  `log_path` and `log_table_id` arguments
  ([\#24](https://github.com/ssi-dk/SCDB/issues/24)).

- `Logger\$log_filename` has been changed to `Logger\$log_basename` to
  reduce ambiguity.

### New features

- Package functions are now also tested with
  [`RPostgres::Postgres()`](https://rpostgres.r-dbi.org/reference/Postgres.html),
  which is therefore now *officially* supported
  ([\#31](https://github.com/ssi-dk/SCDB/issues/31)).

- [`get_connection()`](https://ssi-dk.github.io/SCDB/reference/get_connection.md)
  shows a warning if an unsupported backend is used
  ([\#26](https://github.com/ssi-dk/SCDB/issues/26)).

- Increased flexibility for the `Logger` object
  ([\#21](https://github.com/ssi-dk/SCDB/issues/21)
  [\#24](https://github.com/ssi-dk/SCDB/issues/24)):

  - A `Logger` instance may now be created with no arguments.

  - Suppress console output with `output_to_console` (`TRUE` by
    default).

  - If no `log_path` is set, `Logger` does not fail before trying to
    write to a file.

  - `Logger\$log_realpath` gives the full path to log file being
    written.

### Minor improvements and fixes

- `schema_exists` correctly detects a schema with no tables
  ([\#30](https://github.com/ssi-dk/SCDB/issues/30)).

- `db_timestamps` now newer calls `translate_sql` with `con = NULL`
  ([\#37](https://github.com/ssi-dk/SCDB/issues/37)).

- Package description has been updated to not use a footnote on CRAN.

### Known issues

- As `schema_exists` on an empty schema tests by creating a new table,
  this may cause issues if the user does not have sufficient privileges.

## SCDB 0.1

CRAN release: 2023-08-28

### Features

- Functions to handle database connections:

  - [`get_connection()`](https://ssi-dk.github.io/SCDB/reference/get_connection.md),
    [`close_connection()`](https://ssi-dk.github.io/SCDB/reference/close_connection.md),
    [`id()`](https://ssi-dk.github.io/SCDB/reference/id.md)

- Functions to interface with database:

  - [`get_tables()`](https://ssi-dk.github.io/SCDB/reference/get_tables.md),
    [`table_exists()`](https://ssi-dk.github.io/SCDB/reference/table_exists.md),
    [`get_schema()`](https://ssi-dk.github.io/SCDB/reference/get_schema.md),
    [`schema_exists()`](https://ssi-dk.github.io/SCDB/reference/schema_exists.md)

- Functions to create “historical” tables and logs:

  - [`create_table()`](https://ssi-dk.github.io/SCDB/reference/create_table.md),
    [`create_logs_if_missing()`](https://ssi-dk.github.io/SCDB/reference/create_logs_if_missing.md)

- Function to maintain “historical” tables:

  - [`update_snapshot()`](https://ssi-dk.github.io/SCDB/reference/update_snapshot.md)

- Functions to interface with “historical” tables:

  - [`get_table()`](https://ssi-dk.github.io/SCDB/reference/get_table.md),
    [`slice_time()`](https://ssi-dk.github.io/SCDB/reference/slice_time.md),
    [`is.historical()`](https://ssi-dk.github.io/SCDB/reference/is.historical.md)

- Functions to facilitate faster joins with NAs on SQL backends:

  - [`full_join()`](https://dplyr.tidyverse.org/reference/mutate-joins.html),
    [`inner_join()`](https://dplyr.tidyverse.org/reference/mutate-joins.html),
    [`left_join()`](https://dplyr.tidyverse.org/reference/mutate-joins.html),
    [`right_join()`](https://dplyr.tidyverse.org/reference/mutate-joins.html)

- Functions to manipulate tables on SQL backends:

  - [`filter_keys()`](https://ssi-dk.github.io/SCDB/reference/filter_keys.md),
    [`unite.tbl_dbi()`](https://ssi-dk.github.io/SCDB/reference/unite.tbl_dbi.md),
    `interlace_sql()`

- A logging object to facilitate logging:

  - [`Logger()`](https://ssi-dk.github.io/SCDB/reference/Logger.md)

- Function to generate checksums:

  - [`digest_to_checksum()`](https://ssi-dk.github.io/SCDB/reference/digest_to_checksum.md)

- Function to write timestamps to tables on SQL backends:

  - [`db_timestamp()`](https://ssi-dk.github.io/SCDB/reference/db_timestamp.md)

- Helper functions:

  - [`nrow()`](https://ssi-dk.github.io/SCDB/reference/nrow.md) -
    database compliant
    [`nrow()`](https://ssi-dk.github.io/SCDB/reference/nrow.md)

  - `%notin%` - negated `%in%`

### Testing

- Most package functions are tested here.

### Documentation

- The functions are fully documented.
