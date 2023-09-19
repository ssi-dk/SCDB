# SCDB (development version)

Features:
* Increased flexibility for the Logger object
  - A Logger instance may now be created with no arguments
  - Suppress console output with output_to_console (TRUE by default)
  - If no log_path is set, Logger does not fail before trying to write to a file
* Warn user is get_connection is called with untested driver
* update_snapshot may take an already initialized Logger


Testing:
* Package functions are now also tested on a Postgres database

Documentation:
* Package description has been updated to not use a footnote

# SCDB 0.1

Features:
* Functions to handle DB connections
  * get_connection, close_connection, id
* Functions to interface with DB
  * get_tables, table_exists, get_schema, schema_exists
* Functions to create "historical" tables and logs
  * create_table, create_logs_if_missing
* Function to maintain "historical" tables
  * update_snapshot
* Functions to interface with "historical" tables
  * get_table, slice_time, is.historical
* Functions to facilitate faster joins with NAs on SQL backends
  * full_join, inner_join, left_join, right_join
* Functions to manipulate tables on SQL backends
  * filter_keys, unite.tbl_dbi, interlace_sql
* A logging object to facilitate logging
  * Logger
* Function to generate checksums
  * digest_to_checksum
* Function to write timestamps to tables on SQL backends
  * db_timestamp
* Helper functions
  * nrow - DB compliant nrow
  * %notin% - negated %in%

Testing:
* Most package functions are tested here

Documentation
* The functions are fully documented
