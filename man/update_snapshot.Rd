% Generated by roxygen2: do not edit by hand
% Please edit documentation in R/update_snapshot.R
\name{update_snapshot}
\alias{update_snapshot}
\title{Update a historical table}
\usage{
update_snapshot(
  .data,
  conn,
  db_table,
  timestamp,
  filters = NULL,
  message = NULL,
  tic = Sys.time(),
  logger = NULL,
  enforce_chronological_order = TRUE
)
}
\arguments{
\item{.data}{(\code{data.frame(1)}, \code{tibble(1)}, \code{data.table(1)}, or \code{tbl_dbi(1)})\cr
Data object.}

\item{conn}{(\code{DBIConnection(1)})\cr
Connection object.}

\item{db_table}{(\verb{id-like object(1)})\cr
A table specification (coercible by \code{id()}).}

\item{timestamp}{(\code{POSIXct(1)}, \code{Date(1)}, or \code{character(1)})\cr
The timestamp describing the data being processed (not the current time).}

\item{filters}{(\code{data.frame(1)}, \code{tibble(1)}, \code{data.table(1)}, or \code{tbl_dbi(1)})\cr
A object subset data by.
If filters is \code{NULL}, no filtering occurs.
Otherwise, an \code{inner_join()} is performed using all columns of the filter object.}

\item{message}{(\code{character(1)})\cr
A message to add to the log-file (useful for supplying metadata to the log).}

\item{tic}{(\code{POSIXct(1)})\cr
A timestamp when computation began. If not supplied, it will be created at call-time
(used to more accurately convey the runtime of the update process).}

\item{logger}{(\code{Logger(1)})\cr
A configured logging object. If none is given, one is initialized with default arguments.}

\item{enforce_chronological_order}{(\code{logical(1)})\cr
Are updates allowed if they are chronologically earlier than latest update?}
}
\value{
No return value, called for side effects.
}
\description{
\code{update_snapshots} makes it easy to create and update a historical data table on a remote (SQL) server.
The function takes the data (\code{.data}) as it looks on a given point in time (\code{timestamp}) and then updates
(or creates) an remote table identified by \code{db_table}.
This update only stores the changes between the new data (\code{.data}) and the data currently stored on the remote.
This way, the data can be reconstructed as it looked at any point in time while taking as little space as possible.

See \code{vignette("basic-principles")} for further introduction to the function.
}
\examples{
\dontshow{if (requireNamespace("RSQLite", quietly = TRUE)) (if (getRversion() >= "3.4") withAutoprint else force)(\{ # examplesIf}
  conn <- get_connection()

  data <- dplyr::copy_to(conn, mtcars)

  # Copy the first 3 records
  update_snapshot(
    head(data, 3),
    conn = conn,
    db_table = "test.mtcars",
    timestamp = Sys.time()
  )

  # Update with the first 5 records
  update_snapshot(
    head(data, 5),
    conn = conn,
    db_table = "test.mtcars",
    timestamp = Sys.time()
  )

  dplyr::tbl(conn, "test.mtcars")

  close_connection(conn)
\dontshow{\}) # examplesIf}
}
\seealso{
filter_keys
}
