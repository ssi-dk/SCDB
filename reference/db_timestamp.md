# Determine the type of timestamps the database supports

Determine the type of timestamps the database supports

## Usage

``` r
db_timestamp(timestamp, conn = NULL)
```

## Arguments

- timestamp:

  (`POSIXct(1)` or `character(1)`)\
  The timestamp to be transformed to the database type.

- conn:

  (`DBIConnection(1)`)\
  Connection object.

## Value

The given timestamp converted to a SQL-backend dependent timestamp.

## Examples

``` r
  conn <- get_connection()

  db_timestamp(Sys.time(), conn)
#> <SQL> '2026-10-31 00:32:08.214493'

  close_connection(conn)
```
