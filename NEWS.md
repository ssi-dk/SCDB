# SCDB (development version)

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
