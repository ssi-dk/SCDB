# Checks if table contains historical data

Checks if table contains historical data

## Usage

``` r
is.historical(.data)
```

## Arguments

- .data:

  (`data.frame(1)`, `tibble(1)`, `data.table(1)`, or `tbl_dbi(1)`)\
  Data object.

## Value

`TRUE` if `.data` contains the columns: "checksum", "from_ts", and
"until_ts". `FALSE` otherwise.

## Examples

``` r
  conn <- get_connection()

  dplyr::copy_to(conn, mtcars, name = "mtcars", temporary = FALSE)
  create_table(mtcars, conn, db_table = id("mtcars_historical", conn))

  is.historical(get_table(conn, "mtcars")) # FALSE
#> [1] FALSE
  is.historical(get_table(conn, "mtcars_historical")) # TRUE
#> [1] FALSE

  close_connection(conn)
```
