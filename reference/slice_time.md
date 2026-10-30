# Slices a data object based on time / date

Slices a data object based on time / date

## Usage

``` r
slice_time(.data, slice_ts, from_ts = "from_ts", until_ts = "until_ts")
```

## Arguments

- .data:

  (`data.frame(1)`, `tibble(1)`, `data.table(1)`, or `tbl_dbi(1)`)\
  Data object.

- slice_ts:

  (`POSIXct(1)`, `Date(1)`, or `character(1)`)\
  The time / date to slice by.

- from_ts, until_ts:

  (`character(1)`)\
  The name of the columns in .data specifying valid from and valid until
  time.

## Value

An object of same class as .data

## Examples

``` r
  conn <- get_connection()

  m <- mtcars |>
    dplyr::mutate(
      "from_ts" = dplyr::if_else(dplyr::row_number() > 10,
                                 as.Date("2020-01-01"),
                                 as.Date("2021-01-01")),
      "until_ts" = as.Date(NA))

  dplyr::copy_to(conn, m, name = "mtcars", temporary = FALSE)

  q <- dplyr::tbl(conn, id("mtcars", conn))

  nrow(slice_time(q, "2020-01-01")) # 10
#> [1] 32
  nrow(slice_time(q, "2021-01-01")) # nrow(mtcars)
#> [1] 32

  close_connection(conn)
```
