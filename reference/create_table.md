# Create a historical table from input data

Create a historical table from input data

## Usage

``` r
create_table(.data, conn = NULL, db_table, ...)
```

## Arguments

- .data:

  (`data.frame(1)`, `tibble(1)`, `data.table(1)`, or `tbl_dbi(1)`)\
  Data object.

- conn:

  (`DBIConnection(1)`)\
  Connection object.

- db_table:

  (`id-like object(1)`)\
  A table specification (coercible by
  [`id()`](https://ssi-dk.github.io/SCDB/reference/id.md)).

- ...:

  Other arguments passed to
  [`DBI::dbCreateTable()`](https://dbi.r-dbi.org/reference/dbCreateTable.html).

## Value

Invisibly returns the table as it looks on the destination (or locally
if `conn` is `NULL`).

## Examples

``` r
  conn <- get_connection()

  create_table(mtcars, conn = conn, db_table = "mtcars")

  close_connection(conn)
```
