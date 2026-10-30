# Import and export a data-chunk with history from historical data

`delta_export()` exports data from tables created with
[`update_snapshot()`](https://ssi-dk.github.io/SCDB/reference/update_snapshot.md)
in chunks to allow for faster migration of data between sources.

`delta_load()` import deltas created by `delta_export()` to rebuild a
historical table.

## Usage

``` r
delta_export(conn, db_table, timestamp_from, timestamp_until = NULL)

delta_load(conn, db_table, delta, logger = NULL)
```

## Arguments

- conn:

  (`DBIConnection(1)`)\
  Connection object.

- db_table:

  (`id-like object(1)`)\
  A table specification (coercible by
  [`id()`](https://ssi-dk.github.io/SCDB/reference/id.md)).

- timestamp_from:

  (`POSIXct(1)`, `Date(1)`, or `character(1)`)\
  The timestamp describing the start of the export (including).

- timestamp_until:

  (`POSIXct(1)`, `Date(1)`, or `character(1)`)\
  The timestamp describing the end of the export (not-including).

  If `NULL` (default), all history after `timestamp_from` is exported.

- delta:

  .data (`data.frame(1)`, `tibble(1)`, `data.table(1)`, or
  `tbl_dbi(1)`)\
  A "delta" exported from `delta_export()` to load.

  A list of "deltas" can also be supplied.

- logger:

  (`Logger(1)`)\
  A configured logging object. If none is given, one is initialized with
  default arguments.

## Value

`delta_export()` returns a lazy-query containing the data (and history)
in the source to be used in conjunction with `delta_load()`.

This table is a temporary table that may need cleaning up.

`delta_load()` returns NULL (called for side effects).

## Details

This pair of functions is designed to facilitate easy migration or
incremental backups of a historical table (created by
[`update_snapshot()`](https://ssi-dk.github.io/SCDB/reference/update_snapshot.md)).

To construct the basis of incremental backups, `delta_export()` can be
called with only `timestamp_from` at the desired frequency (weekly etc.)

To migrate a historical table in chunks, `delta_export()` can be called
with `timestamp_until` to constrain the size of the delta.

In either case, the table can then be re-constructed by "replaying" the
deltas with `delta_load()`. The order the deltas are replayed does not
matter, but all have to be replayed to achieve the same state as the
source table.

## See also

update_snapshot

## Examples

``` r
  conn <- get_connection()

  data <- dplyr::copy_to(conn, mtcars)

  # Copy the first 3 records
  update_snapshot(
    head(data, 3),
    conn = conn,
    db_table = "test.mtcars",
    timestamp = "2020-01-01"
  )
#> Warning: `log_path` and `log_tbl` are both `NULL` and therefore NO file or database logging will be done.
#> Consider adding options SCDB.log_table_id and/or SCDB.log_path to your .Rprofile
#> 2026-10-30 23:28:21.850 - rstudio - INFO - Started
#> 2026-10-30 23:28:21.850 - rstudio - INFO - Parsing data for table main.test.mtcars started
#> 2026-10-30 23:28:21.850 - rstudio - INFO - Given timestamp for table is 2020-01-01
#> 2026-10-30 23:28:21.958 - rstudio - INFO - Calculated checksums
#> 2026-10-30 23:28:22.011 - rstudio - INFO - Deactivating records
#> 2026-10-30 23:28:22.047 - rstudio - INFO - Deactivate records count: 0
#> 2026-10-30 23:28:22.048 - rstudio - INFO - Adding new records
#> 2026-10-30 23:28:22.059 - rstudio - INFO - Insert records count: 3
#> 2026-10-30 23:28:22.059 - rstudio - INFO - Finished processing for table main.test.mtcars
#> NULL

  # Create a delta with the current state
  delta <- delta_export(
    conn,
    db_table = "test.mtcars",
    timestamp_from = "2020-01-01"
  )

  # Update with the first 5 records
  update_snapshot(
    head(data, 5),
    conn = conn,
    db_table = "test.mtcars",
    timestamp = "2021-01-01"
  )
#> Warning: `log_path` and `log_tbl` are both `NULL` and therefore NO file or database logging will be done.
#> Consider adding options SCDB.log_table_id and/or SCDB.log_path to your .Rprofile
#> 2026-10-30 23:28:22.090 - rstudio - INFO - Started
#> 2026-10-30 23:28:22.090 - rstudio - INFO - Parsing data for table main.test.mtcars started
#> 2026-10-30 23:28:22.090 - rstudio - INFO - Given timestamp for table is 2021-01-01
#> 2026-10-30 23:28:22.216 - rstudio - INFO - Calculated checksums
#> 2026-10-30 23:28:22.268 - rstudio - INFO - Deactivating records
#> 2026-10-30 23:28:22.299 - rstudio - INFO - Deactivate records count: 0
#> 2026-10-30 23:28:22.300 - rstudio - INFO - Adding new records
#> 2026-10-30 23:28:22.310 - rstudio - INFO - Insert records count: 2
#> 2026-10-30 23:28:22.310 - rstudio - INFO - Finished processing for table main.test.mtcars
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

  # Create a backup using the delta
  delta_load(
    conn = conn,
    db_table = "test.mtcars_backup",
    delta = delta
  )
#> NULL

  dplyr::tbl(conn, "test.mtcars_backup")
#> # A query:  ?? x 14
#> # Database: sqlite 3.53.3 [:memory:]
#>     mpg   cyl  disp    hp  drat    wt  qsec    vs    am  gear  carb checksum    
#>   <dbl> <dbl> <dbl> <dbl> <dbl> <dbl> <dbl> <dbl> <dbl> <dbl> <dbl> <chr>       
#> 1  21       6   160   110  3.9   2.62  16.5     0     1     4     4 1da1bd6281e…
#> 2  21       6   160   110  3.9   2.88  17.0     0     1     4     4 7a302dae353…
#> 3  22.8     4   108    93  3.85  2.32  18.6     1     1     4     1 0532ea093ca…
#> # ℹ 2 more variables: from_ts <chr>, until_ts <dbl>

  close_connection(conn)
```
