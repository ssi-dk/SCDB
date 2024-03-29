% Generated by roxygen2: do not edit by hand
% Please edit documentation in R/get_table.R
\name{get_table}
\alias{get_table}
\title{Retrieves a named table from a given schema on the connection}
\usage{
get_table(conn, db_table = NULL, slice_ts = NA, include_slice_info = FALSE)
}
\arguments{
\item{conn}{(\code{DBIConnection(1)})\cr
Connection object.}

\item{db_table}{(\verb{id-like object(1)})\cr
A table specification (coercible by \code{id()}).
If missing, a list of available tables is printed.}

\item{slice_ts}{(\code{POSIXct(1)}, \code{Date(1)}, or \code{character(1)})\cr
If set different from \code{NA} (default), the returned data looks as on the given date.
If set as \code{NULL}, all data is returned.}

\item{include_slice_info}{(\code{logical(1)})\cr
Should the history columns "checksum", "from_ts", "until_ts" are also be returned?}
}
\value{
A "lazy" data.frame (tbl_lazy) generated using dbplyr.

Note that a temporary table will be preferred over ordinary tables in the default schema (see \code{\link[=get_schema]{get_schema()}}) with
an identical name.
}
\description{
Retrieves a named table from a given schema on the connection
}
\examples{
\dontshow{if (requireNamespace("RSQLite", quietly = TRUE)) (if (getRversion() >= "3.4") withAutoprint else force)(\{ # examplesIf}
  conn <- get_connection()

  dplyr::copy_to(conn, mtcars, name = "mtcars", temporary = FALSE)

  get_table(conn)
  if (table_exists(conn, "mtcars")) {
    get_table(conn, "mtcars")
  }

  close_connection(conn)
\dontshow{\}) # examplesIf}
}
