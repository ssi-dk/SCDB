# Combine any number of tables, where each has their own time axis of validity

The function "interlaces" the queries and combines their validity time
axes (valid_from and valid_until) onto a single time axis.

## Usage

``` r
interlace(tables, by = NULL, colnames = NULL)
```

## Arguments

- tables:

  (`list`(`tbl_dbi(1)`))\
  The historical tables to combine.

- by:

  ([`character()`](https://rdrr.io/r/base/character.html))\
  The variable to merge by.

- colnames:

  (`named list()`)\
  If the time axes of validity is not called "valid_to" and
  "valid_until" inside each `tbl_dbi`, you can specify their names by
  supplying the arguments as a list: e.g. c(t1.from = "\\colname\\",
  t2.until = "\\colname\\"). colnames must be named in same order as as
  given in tables (i.e. t1, t2, t3, ...).

## Value

The combination of input queries with a single, interlaced valid_from /
valid_until time axis.

         The combination of input queries with a single, interlaced
                 valid_from / valid_until time axis

## Examples

``` r
  conn <- get_connection()

  t1 <- data.frame(key = c("A", "A", "B"),
                   obs_1   = c(1, 2, 2),
                   valid_from  = as.Date(c("2021-01-01", "2021-02-01", "2021-01-01")),
                   valid_until = as.Date(c("2021-02-01", "2021-03-01", NA))) |>
    dplyr::copy_to(conn, df = _, name = "t1")

  t2 <- data.frame(key = c("A", "B"),
                   obs_2 = c("a", "b"),
                   valid_from  = as.Date(c("2021-01-01", "2021-01-01")),
                   valid_until = as.Date(c("2021-04-01", NA))) |>
    dplyr::copy_to(conn, df = _, name = "t2")

  interlace(list(t1, t2), by = "key")
#> # A query:    ?? x 5
#> # Database:   sqlite 3.53.3 [:memory:]
#> # Ordered by: valid_from
#>   key   obs_1 obs_2 valid_from valid_until
#>   <chr> <dbl> <chr>      <dbl>       <dbl>
#> 1 A         1 a          18628       18659
#> 2 A         2 a          18659       18687
#> 3 A        NA a          18687       18718
#> 4 B         2 b          18628          NA

  close_connection(conn)
```
