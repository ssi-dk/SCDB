# Retrieves a named table from a given schema on the connection

Retrieves a named table from a given schema on the connection

## Usage

``` r
get_table(conn, db_table = NULL, slice_ts = NA, include_slice_info = FALSE)
```

## Arguments

- conn:

  (`DBIConnection(1)`)\
  Connection object.

- db_table:

  (`id-like object(1)`)\
  A table specification (coercible by
  [`id()`](https://ssi-dk.github.io/SCDB/reference/id.md)). If missing,
  a list of available tables is printed.

- slice_ts:

  (`POSIXct(1)`, `Date(1)`, or `character(1)`)\
  If set different from `NA` (default), the returned data looks as on
  the given date. If set as `NULL`, all data is returned.

- include_slice_info:

  (`logical(1)`)\
  Should the history columns "checksum", "from_ts", "until_ts" are also
  be returned?

## Value

A "lazy" data.frame (tbl_lazy) generated using dbplyr.

Note that a temporary table will be preferred over ordinary tables in
the default schema (see
[`get_schema()`](https://ssi-dk.github.io/SCDB/reference/get_schema.md))
with an identical name.

## Examples

``` r
  conn <- get_connection()

  dplyr::copy_to(conn, mtcars, name = "mtcars", temporary = FALSE)

  get_table(conn)
#> Select one of the following tables:
#>   schema  table
#> 1   main mtcars
  if (table_exists(conn, "mtcars")) {
    get_table(conn, "mtcars")
  }
#> # A query:  ?? x 11
#> # Database: sqlite 3.53.3 [:memory:]
#>      mpg   cyl  disp    hp  drat    wt  qsec    vs    am  gear  carb
#>    <dbl> <dbl> <dbl> <dbl> <dbl> <dbl> <dbl> <dbl> <dbl> <dbl> <dbl>
#>  1  21       6  160    110  3.9   2.62  16.5     0     1     4     4
#>  2  21       6  160    110  3.9   2.88  17.0     0     1     4     4
#>  3  22.8     4  108     93  3.85  2.32  18.6     1     1     4     1
#>  4  21.4     6  258    110  3.08  3.22  19.4     1     0     3     1
#>  5  18.7     8  360    175  3.15  3.44  17.0     0     0     3     2
#>  6  18.1     6  225    105  2.76  3.46  20.2     1     0     3     1
#>  7  14.3     8  360    245  3.21  3.57  15.8     0     0     3     4
#>  8  24.4     4  147.    62  3.69  3.19  20       1     0     4     2
#>  9  22.8     4  141.    95  3.92  3.15  22.9     1     0     4     2
#> 10  19.2     6  168.   123  3.92  3.44  18.3     1     0     4     4
#> # ℹ more rows

  close_connection(conn)
```
