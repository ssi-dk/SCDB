# nrow() but also works on remote tables

nrow() but also works on remote tables

## Usage

``` r
nrow(.data)
```

## Arguments

- .data:

  (`data.frame(1)`, `tibble(1)`, `data.table(1)`, or `tbl_dbi(1)`)\
  Data object.

## Value

The number of records in the object.

## Examples

``` r
  conn <- get_connection()

  m <- dplyr::copy_to(conn, mtcars)
  nrow(m) == nrow(mtcars) # TRUE
#> [1] TRUE

  close_connection(conn)
```
