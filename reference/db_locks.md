# Sets, queries and removes locks for database tables

This set of function adds a simple locking system to database tables.

- `lock_table()` adds a record in the schema.locks table with the
  current time and R-session process id.

- `unlock_table()` removes records in the schema.locks table with the
  target table and the R-session process id.

When locking a table, the function will check for existing locks on the
table and produce an error a lock is held by a process which no longer
exists. In this case, the lock needs to be removed manually by removing
the record from the lock table. In addition, the error implies that a
table may have partial updates that needs to be manually rolled back.

## Usage

``` r
lock_table(conn, db_table, schema = NULL)

unlock_table(conn, db_table, schema = NULL, pid = Sys.getpid())
```

## Arguments

- conn:

  (`DBIConnection(1)`)\
  Connection object.

- db_table:

  (`character(1)`)\
  A specification of "schema.table" to modify lock for.

- schema:

  (`character(1)`)\
  The schema where the "locks" table should be created.

- pid:

  (`numeric(1)`)\
  The process id to remove the lock for.

## Value

- `lock_table()` returns the `TRUE` (`FALSE`) if the lock was
  (un)successfully added. If a lock exists for a non-active process, an
  error is thrown.

- `unlock_table()` returns `NULL` (called for side effects).

## Examples

``` r
  conn <- DBI::dbConnect(RSQLite::SQLite())

  lock_table(conn, "test_table") # TRUE
#> [1] TRUE

  unlock_table(conn, "test_table")
#> NULL

  DBI::dbDisconnect(conn)
```
