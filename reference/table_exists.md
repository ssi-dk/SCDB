# Test if a table exists in database

This functions attempts to determine the existence of a given table. If
a character input is given, matching is done heuristically assuming a
"schema.table" notation. If no schema is implied in this case, the
default schema is assumed.

## Usage

``` r
table_exists(conn, db_table)

# S3 method for class 'DBIConnection'
table_exists(conn, db_table)
```

## Arguments

- conn:

  (`DBIConnection(1)`)\
  Connection object.

- db_table:

  (`id-like object(1)`)\
  A table specification (coercible by
  [`id()`](https://ssi-dk.github.io/SCDB/reference/id.md)).

## Value

`TRUE` if db_table can be parsed to a table found in `conn`.

## Examples

``` r
  conn <- get_connection()

  dplyr::copy_to(conn, mtcars, name = "mtcars", temporary = FALSE)
  dplyr::copy_to(conn, iris, name = "iris")

  table_exists(conn, "mtcars")    # TRUE
#> [1] TRUE
  table_exists(conn, "iris")      # FALSE
#> [1] FALSE
  table_exists(conn, "temp.iris") # TRUE
#> [1] TRUE

  close_connection(conn)
```
