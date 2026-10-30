# Slowly Changing Dimension methodology

[`library`](https://rdrr.io/r/base/library.html)`(`[`tidyverse`](https://tidyverse.tidyverse.org)`)`` ``#> ── ``Attaching core tidyverse packages`` ──────────────────────── tidyverse 2.0.0 ──`` ``#> ``✔`` ``dplyr `` 1.2.1 ``✔`` ``readr `` 2.2.0`` ``#> ``✔`` ``forcats `` 1.0.1 ``✔`` ``stringr `` 1.6.0`` ``#> ``✔`` ``ggplot2 `` 4.0.3 ``✔`` ``tibble `` 3.3.1`` ``#> ``✔`` ``lubridate`` 1.9.5 ``✔`` ``tidyr `` 1.3.2`` ``#> ``✔`` ``purrr `` 1.2.2 `` ``#> ── ``Conflicts`` ────────────────────────────────────────── tidyverse_conflicts() ──`` ``#> ``✖`` ``dplyr``::``filter()`` masks ``stats``::filter()`` ``#> ``✖`` ``dplyr``::``lag()`` masks ``stats``::lag()`` ``#> ``ℹ`` Use the conflicted package (``<http://conflicted.r-lib.org/>``) to force all conflicts to become errors`

A slowly changing dimension is a concept in data warehousing which
refers to data which may change over time, but at an irregular schedule.

## Type 1 and Type 2 history

For example, consider the following table of forecasts for a number of
cities:

`# Current date: 2023-09-28`` ``forecasts`` ``#> ``# A tibble: 4 × 2`` ``#> City Forecast`` ``#> ``<chr>`` ``<dbl>`` ``#> ``1`` New York 20`` ``#> ``2`` Los Angeles 23`` ``#> ``3`` Seattle 16`` ``#> ``4`` Houston 34`

The following day, the forecasts will have changed, and — barring the
occasional data hoarder — the existing data is no longer relevant.

In this example, most (if not all) of the values of the `Forecast`
column will change with each regular update. Putting it into other
words, the table is a *snapshot*[^1] of forecasts at the last time of
update.

The following day, the forecasts naturally change:

`# Current date: 2023-09-29`` ``forecasts2`` ``#> ``# A tibble: 4 × 2`` ``#> City Forecast`` ``#> ``<chr>`` ``<dbl>`` ``#> ``1`` New York 18`` ``#> ``2`` Los Angeles 25`` ``#> ``3`` Seattle 17`` ``#> ``4`` Houston 34`

We could choose to update the forecasts table so that it would always
contain the current data. This is what is referred to as Type 1
methodology (Kimball and Ross 2013).

Databases are thankfully a rather efficient way of storing and accessing
data, so instead of discarding the values from the previous day, we
append the new data to those of the previous day. Also, in order to keep
our data organized, we add a column with the date of the forecast, aptly
named `ForecastDate`.

The full table of forecasts for the two days now looks like below, and
we are slowly building a full history of forecasts:

`forecasts_full`` ``#> ``# A tibble: 8 × 3`` ``#> City Forecast ForecastDate`` ``#> ``<chr>`` ``<dbl>`` ``<date>`` `` ``#> ``1`` New York 20 2023-09-28 `` ``#> ``2`` Los Angeles 23 2023-09-28 `` ``#> ``3`` Seattle 16 2023-09-28 `` ``#> ``4`` Houston 34 2023-09-28 `` ``#> ``5`` New York 18 2023-09-29 `` ``#> ``6`` Los Angeles 25 2023-09-29 `` ``#> ``7`` Seattle 17 2023-09-29 `` ``#> ``8`` Houston 34 2023-09-29`

Managing historical data by inserting new data in this manner is often
referred to as Type 2 methodology or Type 2 history.

Our table now provides much more information for the user through
filtering:

` ``# Current forecasts`` ``forecasts_full`` `[`%>%`](https://magrittr.tidyverse.org/reference/pipe.html)` `` `[`slice_max`](https://dplyr.tidyverse.org/reference/slice.html)`(``ForecastDate``, n ``=`` ``1``)`` `[`%>%`](https://magrittr.tidyverse.org/reference/pipe.html)` `` `[`select`](https://dplyr.tidyverse.org/reference/select.html)`(``!``"ForecastDate"``)`` ``#> ``# A tibble: 4 × 2`` ``#> City Forecast`` ``#> ``<chr>`` ``<dbl>`` ``#> ``1`` New York 18`` ``#> ``2`` Los Angeles 25`` ``#> ``3`` Seattle 17`` ``#> ``4`` Houston 34`` `` ``# Forecasts for a given date`` ``forecasts_full`` `[`%>%`](https://magrittr.tidyverse.org/reference/pipe.html)` `` `[`filter`](https://dplyr.tidyverse.org/reference/filter.html)`(``ForecastDate`` ``==`` ``"2023-09-28"``)`` ``#> ``# A tibble: 4 × 3`` ``#> City Forecast ForecastDate`` ``#> ``<chr>`` ``<dbl>`` ``<date>`` `` ``#> ``1`` New York 20 2023-09-28 `` ``#> ``2`` Los Angeles 23 2023-09-28 `` ``#> ``3`` Seattle 16 2023-09-28 `` ``#> ``4`` Houston 34 2023-09-28`` `` ``# Full history for a given city`` ``forecasts_full`` `[`%>%`](https://magrittr.tidyverse.org/reference/pipe.html)` `` `[`filter`](https://dplyr.tidyverse.org/reference/filter.html)`(``City`` ``==`` ``"New York"``)`` ``#> ``# A tibble: 2 × 3`` ``#> City Forecast ForecastDate`` ``#> ``<chr>`` ``<dbl>`` ``<date>`` `` ``#> ``1`` New York 20 2023-09-28 `` ``#> ``2`` New York 18 2023-09-29`

Now, we note that the forecast for Houston has not changed between the
two days.

In order to keep our data as minimized as possible, we modify the table
again, now expanding `ForecastDate` into `ForecastFrom` and
`ForecastUntil`.

Our table of forecasts now looks like this:

`forecasts_scd`` ``#> ``# A tibble: 7 × 4`` ``#> City Forecast ForecastFrom ForecastUntil`` ``#> ``<chr>`` ``<dbl>`` ``<date>`` ``<date>`` `` ``#> ``1`` New York 20 2023-09-28 2023-09-29 `` ``#> ``2`` Los Angeles 23 2023-09-28 2023-09-29 `` ``#> ``3`` Seattle 16 2023-09-28 2023-09-29 `` ``#> ``4`` Houston 34 2023-09-28 ``NA`` `` ``#> ``5`` New York 18 2023-09-29 ``NA`` `` ``#> ``6`` Los Angeles 25 2023-09-29 ``NA`` `` ``#> ``7`` Seattle 17 2023-09-29 ``NA`

For now, the `ForecastUntil` value is set to `NA`, as it is not known
when these rows will “expire” (if ever). This also makes it easy to
identify currently valid data.

Adding a new column to save a single row of data naturally seems a bit
overkill, but as the number of rows in the data set increases
indefinitely, this solutions scales much better.

## A “timeline of timelines”

Let’s now introduce additional information and see how managing slowly
changing dimensions enables us to easily navigate large amounts of data
over large periods of time.

Imagine a town of several thousand citizens, with a town hall
maintaining a civil registry of names and addresses of every citizen,
updated daily with any changes submitted by the citizens, each of whom
having an individual identification number.[^2]

The data is largely static, as a very small fraction of citizens move on
any given day, but it is of interest to keep data relatively up-to-date.
This is where managing a slowly changing dimension becomes very
powerful, compared to full incremental backups.

One day, Alice Doe meets [Robert “Bobby” Tables](https://xkcd.com/327/),
and they move in together:

`addresses`` ``#> ``# A tibble: 4 × 8`` ``#> ID GivenName Surname Address MovedIn MovedOut ValidFrom ValidUntil`` ``#> ``<dbl>`` ``<chr>`` ``<chr>`` ``<chr>`` ``<date>`` ``<date>`` ``<date>`` ``<date>`` `` ``#> ``1`` 1 Alice Doe Donut Pla… 1989-06-26 ``NA`` 1989-06-26 2021-03-08`` ``#> ``2`` 2 Robert Tables Rainbow R… 1989-12-13 ``NA`` 1989-12-13 ``NA`` `` ``#> ``3`` 1 Alice Doe Donut Pla… 1989-06-26 2021-03-01 2021-03-08 ``NA`` `` ``#> ``4`` 1 Alice Doe Rainbow R… 2021-03-01 ``NA`` 2021-03-08 ``NA`

First thing to notice is that the registry is not updated in real-time,
as citizens may have been late in registering a change of address. This
can be seen when comparing the values of `MovedIn` and `ValidFrom` for
row 4.

When using Type 2 history, this feature is correctly replicated when
reconstructing historical data:

`slice_timestamp`` ``<-`` ``"2021-03-02"`` `` ``addresses`` `[`%>%`](https://magrittr.tidyverse.org/reference/pipe.html)` `` `[`filter`](https://dplyr.tidyverse.org/reference/filter.html)`(``ID`` ``==`` ``1``,`` `` ``ValidFrom`` ``<`` ``!``!``slice_timestamp``,`` `` ``ValidUntil`` ``>=`` ``!``!``slice_timestamp`` ``|`` `[`is.na`](https://rdrr.io/r/base/NA.html)`(``ValidUntil``)``)`` `[`%>%`](https://magrittr.tidyverse.org/reference/pipe.html)` `` `[`select`](https://dplyr.tidyverse.org/reference/select.html)`(``!`[`c`](https://rdrr.io/r/base/c.html)`(``"ValidFrom"``, ``"ValidUntil"``)``)`` ``#> ``# A tibble: 1 × 6`` ``#> ID GivenName Surname Address MovedIn MovedOut`` ``#> ``<dbl>`` ``<chr>`` ``<chr>`` ``<chr>`` ``<date>`` ``<date>`` `` ``#> ``1`` 1 Alice Doe Donut Plains 1 1989-06-26 ``NA`

In other words, even though Alice’s address was subsequently updated in
the registry, we can still see that she *was* registered as living in
Donut Plains at this time. This modeling of “timelines of timelines” is
also called bitemporal modeling.

By now, things are going well between Alice and Robert; they get
married, with Alice taking Robert’s surname. It is the same person that
has lived with Robert, but as of the day of marriage, she has a
different name:

[`filter`](https://dplyr.tidyverse.org/reference/filter.html)`(``addresses2``,`` `` ``ID`` ``==`` ``1``,`` `` ``Address`` ``==`` ``"Rainbow Road 8"``)`` `[`%>%`](https://magrittr.tidyverse.org/reference/pipe.html)` `` `[`select`](https://dplyr.tidyverse.org/reference/select.html)`(``ID``, ``GivenName``, ``Surname``, ``MovedIn``, ``MovedOut``, ``ValidFrom``, ``ValidUntil``)`` ``#> ``# A tibble: 2 × 7`` ``#> ID GivenName Surname MovedIn MovedOut ValidFrom ValidUntil`` ``#> ``<dbl>`` ``<chr>`` ``<chr>`` ``<date>`` ``<date>`` ``<date>`` ``<date>`` `` ``#> ``1`` 1 Alice Doe 2021-03-01 ``NA`` 2021-03-08 2023-08-28`` ``#> ``2`` 1 Alice Tables 2021-03-01 ``NA`` 2023-08-28 ``NA`

This is now also reflected in the data; the `MovedIn` date is persistent
across the date of the name change, only the `Surname` changes:

`slice_timestamp`` ``<-`` ``"2022-03-04"`` `` ``addresses2`` `[`%>%`](https://magrittr.tidyverse.org/reference/pipe.html)` `` `[`filter`](https://dplyr.tidyverse.org/reference/filter.html)`(``Address`` ``==`` ``"Rainbow Road 8"``,`` `` `[`is.na`](https://rdrr.io/r/base/NA.html)`(``MovedOut``)``,`` `` ``ValidFrom`` ``<`` ``!``!``slice_timestamp``,`` `` ``ValidUntil`` ``>=`` ``!``!``slice_timestamp`` ``|`` `[`is.na`](https://rdrr.io/r/base/NA.html)`(``ValidUntil``)``)`` `[`%>%`](https://magrittr.tidyverse.org/reference/pipe.html)` `` `[`select`](https://dplyr.tidyverse.org/reference/select.html)`(``ID``, ``GivenName``, ``Surname``, ``MovedIn``, ``MovedOut``)`` ``#> ``# A tibble: 2 × 5`` ``#> ID GivenName Surname MovedIn MovedOut`` ``#> ``<dbl>`` ``<chr>`` ``<chr>`` ``<date>`` ``<date>`` `` ``#> ``1`` 2 Robert Tables 1989-12-13 ``NA`` `` ``#> ``2`` 1 Alice Doe 2021-03-01 ``NA`` `` ``slice_timestamp`` ``<-`` ``"2023-09-29"`` `` ``addresses2`` `[`%>%`](https://magrittr.tidyverse.org/reference/pipe.html)` `` `[`filter`](https://dplyr.tidyverse.org/reference/filter.html)`(``Address`` ``==`` ``"Rainbow Road 8"``,`` `` `[`is.na`](https://rdrr.io/r/base/NA.html)`(``MovedOut``)``,`` `` ``ValidFrom`` ``<`` ``!``!``slice_timestamp``,`` `` ``ValidUntil`` ``>=`` ``!``!``slice_timestamp`` ``|`` `[`is.na`](https://rdrr.io/r/base/NA.html)`(``ValidUntil``)``)`` `[`%>%`](https://magrittr.tidyverse.org/reference/pipe.html)` `` `[`select`](https://dplyr.tidyverse.org/reference/select.html)`(``ID``, ``GivenName``, ``Surname``, ``MovedIn``, ``MovedOut``)`` ``#> ``# A tibble: 2 × 5`` ``#> ID GivenName Surname MovedIn MovedOut`` ``#> ``<dbl>`` ``<chr>`` ``<chr>`` ``<date>`` ``<date>`` `` ``#> ``1`` 2 Robert Tables 1989-12-13 ``NA`` `` ``#> ``2`` 1 Alice Tables 2021-03-01 ``NA`

## Summary

By now, it is hopefully clear how managing a slowly changing dimension
allows you to access data at any point in (tracked) time while
potentially avoiding a lot of data redundancy.

You are now ready to get started with the `SCDB` package!

### References

Kimball, R., and M. Ross. 2013. *The Data Warehouse Toolkit: The
Definitive Guide to Dimensional Modeling*. Wiley.

[^1]: A *snapshot* is a static view of (part of) a database at a
    specific point in time

[^2]: If this concept seems very familiar, you may have heard of [the
    Danish central civil
    registry](https://en.wikipedia.org/wiki/Personal_identification_number_(Denmark))
