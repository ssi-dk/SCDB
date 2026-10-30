# Delete table at function exit

This function marks a table for deletion once the current function
exits.

## Usage

``` r
defer_db_cleanup(db_table)
```

## Arguments

- db_table:

  (`tbl_sql`)\
  A unmanipulated reference to a sql table.

## Value

NULL (called for side effects)

## Examples

``` r
  conn <- get_connection()

  mt <- dplyr::copy_to(conn, mtcars)
  id_mt <- id(mt)

  defer_db_cleanup(mt)

  DBI::dbExistsTable(conn, id_mt) # TRUE
#> [1] FALSE

  withr::deferred_run()
#> No deferred expressions to run

  DBI::dbExistsTable(conn, id_mt) # FALSE
#> [1] FALSE

  close_connection(conn)
```
