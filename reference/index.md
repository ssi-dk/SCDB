# Package index

## Connect and retrieve data

- [`get_connection()`](https://ssi-dk.github.io/SCDB/reference/get_connection.md)
  : Opens connection to the database
- [`close_connection()`](https://ssi-dk.github.io/SCDB/reference/close_connection.md)
  : Close connection to the database
- [`id()`](https://ssi-dk.github.io/SCDB/reference/id.md) : Convenience
  function for DBI::Id
- [`get_tables()`](https://ssi-dk.github.io/SCDB/reference/get_tables.md)
  : List the available tables on the connection
- [`get_table()`](https://ssi-dk.github.io/SCDB/reference/get_table.md)
  : Retrieves a named table from a given schema on the connection
- [`get_catalog()`](https://ssi-dk.github.io/SCDB/reference/get_schema.md)
  [`get_schema()`](https://ssi-dk.github.io/SCDB/reference/get_schema.md)
  : Get the current schema/catalog of a database-related objects
- [`table_exists()`](https://ssi-dk.github.io/SCDB/reference/table_exists.md)
  : Test if a table exists in database
- [`schema_exists()`](https://ssi-dk.github.io/SCDB/reference/schema_exists.md)
  : Test if a schema exists in given connection

## Create or manage tables

- [`create_table()`](https://ssi-dk.github.io/SCDB/reference/create_table.md)
  : Create a historical table from input data
- [`create_index()`](https://ssi-dk.github.io/SCDB/reference/create_index.md)
  : Create the indexes on table
- [`create_logs_if_missing()`](https://ssi-dk.github.io/SCDB/reference/create_logs_if_missing.md)
  : Create a table with the SCDB log structure if it does not exists
- [`update_snapshot()`](https://ssi-dk.github.io/SCDB/reference/update_snapshot.md)
  : Update a historical table
- [`filter_keys()`](https://ssi-dk.github.io/SCDB/reference/filter_keys.md)
  : Filters .data according to all records in the filter
- [`delta_export()`](https://ssi-dk.github.io/SCDB/reference/delta_loading.md)
  [`delta_load()`](https://ssi-dk.github.io/SCDB/reference/delta_loading.md)
  : Import and export a data-chunk with history from historical data
- [`Logger`](https://ssi-dk.github.io/SCDB/reference/Logger.md) :
  Logger: Complete logging to console, file and database
- [`LoggerNull`](https://ssi-dk.github.io/SCDB/reference/LoggerNull.md)
  : LoggerNull: The no-logging Logger

## Manipulate tables

- [`slice_time()`](https://ssi-dk.github.io/SCDB/reference/slice_time.md)
  : Slices a data object based on time / date
- [`inner_join(`*`<tbl_sql>`*`)`](https://ssi-dk.github.io/SCDB/reference/joins.md)
  [`left_join(`*`<tbl_sql>`*`)`](https://ssi-dk.github.io/SCDB/reference/joins.md)
  [`right_join(`*`<tbl_sql>`*`)`](https://ssi-dk.github.io/SCDB/reference/joins.md)
  [`full_join(`*`<tbl_sql>`*`)`](https://ssi-dk.github.io/SCDB/reference/joins.md)
  [`semi_join(`*`<tbl_sql>`*`)`](https://ssi-dk.github.io/SCDB/reference/joins.md)
  [`anti_join(`*`<tbl_sql>`*`)`](https://ssi-dk.github.io/SCDB/reference/joins.md)
  : SQL Joins
- [`unite.tbl_dbi()`](https://ssi-dk.github.io/SCDB/reference/unite.tbl_dbi.md)
  : tidyr::unite for tbl_dbi
- [`interlace()`](https://ssi-dk.github.io/SCDB/reference/interlace.md)
  : Combine any number of tables, where each has their own time axis of
  validity

## Query table information

- [`is.historical()`](https://ssi-dk.github.io/SCDB/reference/is.historical.md)
  : Checks if table contains historical data
- [`nrow()`](https://ssi-dk.github.io/SCDB/reference/nrow.md) : nrow()
  but also works on remote tables

## Developer functions

- [`lock_table()`](https://ssi-dk.github.io/SCDB/reference/db_locks.md)
  [`unlock_table()`](https://ssi-dk.github.io/SCDB/reference/db_locks.md)
  : Sets, queries and removes locks for database tables
- [`db_timestamp()`](https://ssi-dk.github.io/SCDB/reference/db_timestamp.md)
  : Determine the type of timestamps the database supports
- [`defer_db_cleanup()`](https://ssi-dk.github.io/SCDB/reference/defer_db_cleanup.md)
  : Delete table at function exit
- [`digest_to_checksum()`](https://ssi-dk.github.io/SCDB/reference/digest_to_checksum.md)
  : Computes an checksum from columns
- [`unique_table_name()`](https://ssi-dk.github.io/SCDB/reference/unique_table_name.md)
  : Create a name for a temporary table
