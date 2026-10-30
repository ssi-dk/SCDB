# Update a historical table

`update_snapshot()` makes it easy to create and update a historical data
table on a remote (SQL) server. The function takes the data (`.data`) as
it looks on a given point in time (`timestamp`) and then updates (or
creates) an remote table identified by `db_table`. This update only
stores the changes between the new data (`.data`) and the data currently
stored on the remote. This way, the data can be reconstructed as it
looked at any point in time while taking as little space as possible.

See `vignette("basic-principles")` for further introduction to the
function.

## Usage

``` r
update_snapshot(
  .data,
  conn,
  db_table,
  timestamp,
  filters = NULL,
  message = NULL,
  tic = Sys.time(),
  logger = NULL,
  enforce_chronological_order = TRUE,
  collapse_continuous_records = FALSE
)
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

- timestamp:

  (`POSIXct(1)`, `Date(1)`, or `character(1)`)\
  The timestamp describing the data being processed (not the current
  time).

- filters:

  (`data.frame(1)`, `tibble(1)`, `data.table(1)`, or `tbl_dbi(1)`)\
  A object subset data by. If filters is `NULL`, no filtering occurs.
  Otherwise, an
  [`inner_join()`](https://dplyr.tidyverse.org/reference/mutate-joins.html)
  is performed using all columns of the filter object.

- message:

  (`character(1)`)\
  A message to add to the log-file (useful for supplying metadata to the
  log).

- tic:

  (`POSIXct(1)`)\
  A timestamp when computation began. If not supplied, it will be
  created at call-time (used to more accurately convey the runtime of
  the update process).

- logger:

  (`Logger(1)`)\
  A configured logging object. If none is given, one is initialized with
  default arguments.

- enforce_chronological_order:

  (`logical(1)`)\
  Are updates allowed if they are chronologically earlier than latest
  update?

- collapse_continuous_records:

  (`logical(1)`)\
  Check for records where from/until time stamps are equal and delete?
  Forced `TRUE` when `enforce_chronological_order` is `FALSE`.

## Value

No return value, called for side effects.

## Details

The most common use case is having consecutive snapshots of a dataset
and wanting to store the changes between them. If you have a special
case where you want to insert data that is not consecutive, you can set
the `enforce_chronological_order` to `FALSE`. This will allow you to
insert data that is earlier than the latest time stamp.

If you have more updates in a single day and use
[`Date()`](https://lubridate.tidyverse.org/reference/date_utils.html)
rather than
[`POSIXct()`](https://lubridate.tidyverse.org/reference/posix_utils.html),
as your time stamp, you may end up with records where `from_ts` and
`until_ts` are equal. These records not normally accessible with
[`get_table()`](https://ssi-dk.github.io/SCDB/reference/get_table.md)
and you may want to prevent these records using
`collapse_continuous_records = TRUE`.

## See also

filter_keys

## Examples

``` r
  conn <- get_connection()

  data <- dplyr::copy_to(conn, mtcars)

  # Copy the first 3 records
  update_snapshot(
    head(data, 3),
    conn = conn,
    db_table = "test.mtcars",
    timestamp = Sys.time()
  )
#> Warning: `log_path` and `log_tbl` are both `NULL` and therefore NO file or database logging will be done.
#> Consider adding options SCDB.log_table_id and/or SCDB.log_path to your .Rprofile
#> 2026-10-31 00:32:11.831 - rstudio - INFO - Started
#> 2026-10-31 00:32:11.831 - rstudio - INFO - Parsing data for table main.test.mtcars started
#> 2026-10-31 00:32:11.831 - rstudio - INFO - Given timestamp for table is 2026-10-31 00:32:11.831397
#> 2026-10-31 00:32:11.928 - rstudio - INFO - Calculated checksums
#> 2026-10-31 00:32:11.991 - rstudio - INFO - Deactivating records
#> 2026-10-31 00:32:12.024 - rstudio - INFO - Deactivate records count: 0
#> 2026-10-31 00:32:12.024 - rstudio - INFO - Adding new records
#> 2026-10-31 00:32:12.036 - rstudio - INFO - Insert records count: 3
#> 2026-10-31 00:32:12.037 - rstudio - INFO - Finished processing for table main.test.mtcars
#> NULL

  # Update with the first 5 records
  update_snapshot(
    head(data, 5),
    conn = conn,
    db_table = "test.mtcars",
    timestamp = Sys.time()
  )
#> Warning: `log_path` and `log_tbl` are both `NULL` and therefore NO file or database logging will be done.
#> Consider adding options SCDB.log_table_id and/or SCDB.log_path to your .Rprofile
#> 2026-10-31 00:32:12.046 - rstudio - INFO - Started
#> 2026-10-31 00:32:12.046 - rstudio - INFO - Parsing data for table main.test.mtcars started
#> 2026-10-31 00:32:12.046 - rstudio - INFO - Given timestamp for table is 2026-10-31 00:32:12.04655
#> 2026-10-31 00:32:12.192 - rstudio - INFO - Calculated checksums
#> 2026-10-31 00:32:12.252 - rstudio - INFO - Deactivating records
#> 2026-10-31 00:32:12.286 - rstudio - INFO - Deactivate records count: 0
#> 2026-10-31 00:32:12.286 - rstudio - INFO - Adding new records
#> 2026-10-31 00:32:12.299 - rstudio - INFO - Insert records count: 2
#> 2026-10-31 00:32:12.299 - rstudio - INFO - Finished processing for table main.test.mtcars
#> NULL

  dplyr::tbl(conn, "test.mtcars")
#> # A query:  ?? x 14
#> # Database: sqlite 3.53.3 [:memory:]
#>     mpg   cyl  disp    hp  drat    wt  qsec    vs    am  gear  carb checksum    
#>   <dbl> <dbl> <dbl> <dbl> <dbl> <dbl> <dbl> <dbl> <dbl> <dbl> <dbl> <chr>       
#> 1  21       6   160   110  3.9   2.62  16.5     0     1     4     4 1da1bd6281e…
#> 2  21       6   160   110  3.9   2.88  17.0     0     1     4     4 7a302dae353…
#> 3  22.8     4   108    93  3.85  2.32  18.6     1     1     4     1 0532ea093ca…
#> 4  21.4     6   258   110  3.08  3.22  19.4     1     0     3     1 7ff73a7388d…
#> 5  18.7     8   360   175  3.15  3.44  17.0     0     0     3     2 613d84d8ade…
#> # ℹ 2 more variables: from_ts <chr>, until_ts <dbl>

  close_connection(conn)
```
