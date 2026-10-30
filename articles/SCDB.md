# SCDB: Basic principles

[`library`](https://rdrr.io/r/base/library.html)`(`[`SCDB`](https://github.com/ssi-dk/SCDB)`)`` ``#> `` ``#> Attaching package: 'SCDB'`` ``#> The following object is masked from 'package:base':`` ``#> `` ``#> nrow`

The basic principle of the SCDB package is to enable the user to easily
implement and maintain a database of time-versioned data.

In practice, this is done by labeling each record in the data with three
additional fields:

- checksum: An md5 hash of information stored the record
- from_ts: the timestamp at which the record was introduced to the
  dataset
- until_ts: the timestamp at which the record was removed from the
  dataset

This strategy of time versioning is often called “type 2” history
(Kimball and Ross 2013).

The SCDB package provides the function `update_snapshot` to handle the
insertion and deactivation of records using this strategy. It further
includes several functions to improve the Quality of life for working
with database data.

A simple example of usage is shown below.\
For this example, we use a temporary, on-disk SQLite database. Note that
[`get_connection()`](https://ssi-dk.github.io/SCDB/reference/get_connection.md)
tries to establish connection using
[`DBI::dbConnect()`](https://dbi.r-dbi.org/reference/dbConnect.html)
with as few additional arguments as possible. Different drivers may
require authentication which can be read from a configuration file.[^1]

Our example data is
[`datasets::mtcars`](https://rdrr.io/r/datasets/mtcars.html) reduced to
only two columns: row names converted to a column `car`, and `hp`

`conn`` ``<-`` `[`get_connection`](https://ssi-dk.github.io/SCDB/reference/get_connection.md)`(``)`

`example_data`` ``<-`` ``dplyr``::`[`tbl`](https://dplyr.tidyverse.org/reference/tbl.html)`(``conn``, ``DBI``::`[`Id`](https://dbi.r-dbi.org/reference/Id.html)`(``table ``=`` ``"example_data"``)``)`` ``example_data`` ``#> ``# A query: ?? x 2`` ``#> ``# Database: sqlite 3.53.3 [:memory:]`` ``#> car hp`` ``#> ``<chr>`` ``<dbl>`` ``#> `` 1`` Mazda RX4 110`` ``#> `` 2`` Mazda RX4 Wag 110`` ``#> `` 3`` Datsun 710 93`` ``#> `` 4`` Hornet 4 Drive 110`` ``#> `` 5`` Hornet Sportabout 175`` ``#> `` 6`` Valiant 105`` ``#> `` 7`` Duster 360 245`` ``#> `` 8`` Merc 240D 62`` ``#> `` 9`` Merc 230 95`` ``#> ``10`` Merc 280 123`` ``#> ``# ℹ more rows`

Imagine on Day 1, in this case January 1st, 2020, our currently
available data is the first three records of the `example_data`. We then
store this data in a table `mtcars`:

`data`` ``<-`` `[`head`](https://rdrr.io/r/utils/head.html)`(``example_data``, ``3``)`` `` `[`update_snapshot`](https://ssi-dk.github.io/SCDB/reference/update_snapshot.md)`(`` `` .data ``=`` ``data``,`` `` conn ``=`` ``conn``,`` `` db_table ``=`` ``"mtcars"``, ``# the name of the DB table to store the data in`` `` timestamp ``=`` `[`as.POSIXct`](https://rdrr.io/r/base/as.POSIXlt.html)`(``"2020-01-01 11:00:00"``)`` ``)`` ``#> NULL`

We can then access out data using the
[`get_table()`](https://ssi-dk.github.io/SCDB/reference/get_table.md)
function, and include information on data validity period using
`include_slice_info = TRUE`:

[`get_table`](https://ssi-dk.github.io/SCDB/reference/get_table.md)`(``conn``, ``"mtcars"``)`` ``#> ``# A query: ?? x 2`` ``#> ``# Database: sqlite 3.53.3 [:memory:]`` ``#> car hp`` ``#> ``<chr>`` ``<dbl>`` ``#> ``1`` Mazda RX4 110`` ``#> ``2`` Mazda RX4 Wag 110`` ``#> ``3`` Datsun 710 93`` `` `[`get_table`](https://ssi-dk.github.io/SCDB/reference/get_table.md)`(``conn``, ``"mtcars"``, include_slice_info ``=`` ``TRUE``)`` ``#> ``# A query: ?? x 5`` ``#> ``# Database: sqlite 3.53.3 [:memory:]`` ``#> car hp checksum from_ts until_ts`` ``#> ``<chr>`` ``<dbl>`` ``<chr>`` ``<chr>`` ``<dbl>`` ``#> ``1`` Mazda RX4 110 7cbe488757cc85aab6583dbc4226bf68 2020-01-01 11:0… ``NA`` ``#> ``2`` Mazda RX4 Wag 110 b82618e7f5dd30d5df68540cecc696c8 2020-01-01 11:0… ``NA`` ``#> ``3`` Datsun 710 93 08c864e3854eb5a1460d87b3360d636f 2020-01-01 11:0… ``NA`

Note that where
e.g. [`dplyr::tbl()`](https://dplyr.tidyverse.org/reference/tbl.html)
requires a more exact specification of the table identity
(`tbl(conn, DBI::Id(table = "mtcars"))`),
[`get_table()`](https://ssi-dk.github.io/SCDB/reference/get_table.md)
will parse any character to a
[`DBI::Id()`](https://dbi.r-dbi.org/reference/Id.html) object input
using [`id()`](https://ssi-dk.github.io/SCDB/reference/id.md).

The following day, the current data is now the first five rows of our
example data. We then store this data in the database using
[`update_snapshot()`](https://ssi-dk.github.io/SCDB/reference/update_snapshot.md):

`# Let's say that the next day, our data set is now the first 5 of our example data`` ``data`` ``<-`` `[`head`](https://rdrr.io/r/utils/head.html)`(``example_data``, ``5``)`` `` `[`update_snapshot`](https://ssi-dk.github.io/SCDB/reference/update_snapshot.md)`(`` `` .data ``=`` ``data``,`` `` conn ``=`` ``conn``,`` `` db_table ``=`` ``"mtcars"``, ``# the name of the DB table to store the data in`` `` timestamp ``=`` `[`as.POSIXct`](https://rdrr.io/r/base/as.POSIXlt.html)`(``"2020-01-02 12:00:00"``)`` ``)`` ``#> NULL`

We can again use the
[`get_table()`](https://ssi-dk.github.io/SCDB/reference/get_table.md)
function to see the latest available data, including time-keeping with
`include_slice_info = TRUE`:

[`get_table`](https://ssi-dk.github.io/SCDB/reference/get_table.md)`(``conn``, ``"mtcars"``)`` ``#> ``# A query: ?? x 2`` ``#> ``# Database: sqlite 3.53.3 [:memory:]`` ``#> car hp`` ``#> ``<chr>`` ``<dbl>`` ``#> ``1`` Mazda RX4 110`` ``#> ``2`` Mazda RX4 Wag 110`` ``#> ``3`` Datsun 710 93`` ``#> ``4`` Hornet 4 Drive 110`` ``#> ``5`` Hornet Sportabout 175`` `` `[`get_table`](https://ssi-dk.github.io/SCDB/reference/get_table.md)`(``conn``, ``"mtcars"``, include_slice_info ``=`` ``TRUE``)`` ``#> ``# A query: ?? x 5`` ``#> ``# Database: sqlite 3.53.3 [:memory:]`` ``#> car hp checksum from_ts until_ts`` ``#> ``<chr>`` ``<dbl>`` ``<chr>`` ``<chr>`` ``<dbl>`` ``#> ``1`` Mazda RX4 110 7cbe488757cc85aab6583dbc4226bf68 2020-01-01 … ``NA`` ``#> ``2`` Mazda RX4 Wag 110 b82618e7f5dd30d5df68540cecc696c8 2020-01-01 … ``NA`` ``#> ``3`` Datsun 710 93 08c864e3854eb5a1460d87b3360d636f 2020-01-01 … ``NA`` ``#> ``4`` Hornet 4 Drive 110 3c1b6c43b206dd93ee4f6c3d06e1b416 2020-01-02 … ``NA`` ``#> ``5`` Hornet Sportabout 175 9355ed7a70e3ff73a4b6ee7f7129aa35 2020-01-02 … ``NA`

Since our data is time-versioned, we can recover the data from the day
before

[`get_table`](https://ssi-dk.github.io/SCDB/reference/get_table.md)`(``conn``, ``"mtcars"``, slice_ts ``=`` ``"2020-01-01 11:00:00"``)`` ``#> ``# A query: ?? x 2`` ``#> ``# Database: sqlite 3.53.3 [:memory:]`` ``#> car hp`` ``#> ``<chr>`` ``<dbl>`` ``#> ``1`` Mazda RX4 110`` ``#> ``2`` Mazda RX4 Wag 110`` ``#> ``3`` Datsun 710 93`

On day 3, we imagine that we have the same 5 records, but one of them is
altered

`data`` ``<-`` `[`head`](https://rdrr.io/r/utils/head.html)`(``example_data``, ``5``)`` ``|>`` `` ``dplyr``::`[`mutate`](https://dplyr.tidyverse.org/reference/mutate.html)`(``hp ``=`` `[`ifelse`](https://rdrr.io/r/base/ifelse.html)`(``car`` ``==`` ``"Mazda RX4"``, ``hp`` ``/`` ``2``, ``hp``)``)`` `` `[`update_snapshot`](https://ssi-dk.github.io/SCDB/reference/update_snapshot.md)`(`` `` .data ``=`` ``data``,`` `` conn ``=`` ``conn``,`` `` db_table ``=`` ``"mtcars"``, ``# the name of the DB table to store the data in`` `` timestamp ``=`` `[`as.POSIXct`](https://rdrr.io/r/base/as.POSIXlt.html)`(``"2020-01-03 10:00:00"``)`` ``)`` ``#> NULL`

We can again access our data using the
[`get_table()`](https://ssi-dk.github.io/SCDB/reference/get_table.md)
function and see that the currently available data (with the changed hp
value for Mazda RX4)

[`get_table`](https://ssi-dk.github.io/SCDB/reference/get_table.md)`(``conn``, ``"mtcars"``)`` ``#> ``# A query: ?? x 2`` ``#> ``# Database: sqlite 3.53.3 [:memory:]`` ``#> car hp`` ``#> ``<chr>`` ``<dbl>`` ``#> ``1`` Mazda RX4 Wag 110`` ``#> ``2`` Datsun 710 93`` ``#> ``3`` Hornet 4 Drive 110`` ``#> ``4`` Hornet Sportabout 175`` ``#> ``5`` Mazda RX4 55`

Finally, using `slice_ts = NULL`, the full history (and time-keeping
information) is returned:

[`get_table`](https://ssi-dk.github.io/SCDB/reference/get_table.md)`(``conn``, ``"mtcars"``, slice_ts ``=`` ``NULL``)`` ``#> ``# A query: ?? x 5`` ``#> ``# Database: sqlite 3.53.3 [:memory:]`` ``#> car hp checksum from_ts until_ts`` ``#> ``<chr>`` ``<dbl>`` ``<chr>`` ``<chr>`` ``<chr>`` `` ``#> ``1`` Mazda RX4 110 7cbe488757cc85aab6583dbc4226bf68 2020-01-01 … 2020-01…`` ``#> ``2`` Mazda RX4 Wag 110 b82618e7f5dd30d5df68540cecc696c8 2020-01-01 … ``NA`` `` ``#> ``3`` Datsun 710 93 08c864e3854eb5a1460d87b3360d636f 2020-01-01 … ``NA`` `` ``#> ``4`` Hornet 4 Drive 110 3c1b6c43b206dd93ee4f6c3d06e1b416 2020-01-02 … ``NA`` `` ``#> ``5`` Hornet Sportabout 175 9355ed7a70e3ff73a4b6ee7f7129aa35 2020-01-02 … ``NA`` `` ``#> ``6`` Mazda RX4 55 1232f78f7befb3a765b91176eaacdbb0 2020-01-03 … ``NA`

### References

Kimball, R., and M. Ross. 2013. *The Data Warehouse Toolkit: The
Definitive Guide to Dimensional Modeling*. Wiley.

[^1]: In the context of the SCDB package, this is most notably
    [`RPostgres::Postgres()`](https://rpostgres.r-dbi.org/reference/Postgres.html),
    which may read from a `.pgpass` file. See also the [PostgreSQL
    documentation](https://www.postgresql.org/docs/current/libpq-pgpass.html).
