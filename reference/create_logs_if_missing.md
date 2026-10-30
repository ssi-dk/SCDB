# Create a table with the SCDB log structure if it does not exists

Create a table with the SCDB log structure if it does not exists

## Usage

``` r
create_logs_if_missing(conn, log_table)
```

## Arguments

- conn:

  (`DBIConnection(1)`)\
  Connection object.

- log_table:

  (`id-like object`)\
  A table specification where the logs should exist (coercible by
  [`id()`](https://ssi-dk.github.io/SCDB/reference/id.md)).

## Value

Invisibly returns the generated (or existing) log table.

## Examples

``` r
  conn <- get_connection()
  log_table <- id("test.logs", conn = conn, allow_table_only = TRUE)

  create_logs_if_missing(conn, log_table)
#> # A query:  ?? x 11
#> # Database: sqlite 3.53.3 [:memory:]
#> # ℹ 11 variables: date <dbl>, schema <chr>, table <chr>, n_insertions <int>,
#> #   n_deactivations <int>, start_time <dbl>, end_time <dbl>, duration <chr>,
#> #   success <int>, message <chr>, log_file <chr>

  close_connection(conn)
```
