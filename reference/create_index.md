# Create the indexes on table

Create the indexes on table

## Usage

``` r
create_index(conn, db_table, columns)
```

## Arguments

- conn:

  (`DBIConnection`)\
  A connection to a database.

- db_table:

  (`id-like object(1)`)\
  A table specification (coercible by
  [`id()`](https://ssi-dk.github.io/SCDB/reference/id.md)).

- columns:

  ([`character()`](https://rdrr.io/r/base/character.html))\
  The columns that should be unique.

## Value

NULL (called for side effects)

## Examples

``` r
  conn <- get_connection()

  mt <- dplyr::copy_to(conn, dplyr::distinct(mtcars, .data$mpg, .data$cyl), name = "mtcars")
  create_index(conn, mt, c("mpg", "cyl"))
#> [1] 0

  close_connection(conn)
```
