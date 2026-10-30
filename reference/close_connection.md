# Close connection to the database

Close connection to the database

## Usage

``` r
close_connection(conn)
```

## Arguments

- conn:

  (`DBIConnection(1)`)\
  Connection object.

## Value

`dbDisconnect()` returns `TRUE`, invisibly.

## Examples

``` r
  conn <- get_connection()

  close_connection(conn)
```
