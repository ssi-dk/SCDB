# Test if a schema exists in given connection

Test if a schema exists in given connection

## Usage

``` r
schema_exists(conn, schema)
```

## Arguments

- conn:

  (`DBIConnection(1)`)\
  Connection object.

- schema:

  (`character(1)`)\
  The schema name to test existence for.

## Value

TRUE if the given schema is found on `conn`.

## Examples

``` r
  conn <- get_connection()

  schema_exists(conn, "test")
#> [1] FALSE

  close_connection(conn)
```
