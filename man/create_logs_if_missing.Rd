% Generated by roxygen2: do not edit by hand
% Please edit documentation in R/create_table.R
\name{create_logs_if_missing}
\alias{create_logs_if_missing}
\title{Create a table with the SCDB log structure if it does not exists}
\usage{
create_logs_if_missing(log_table, conn)
}
\arguments{
\item{log_table}{A specification of where the logs should exist ("schema.table")}

\item{conn}{An object that inherits from DBIConnection (as generated by get_connection())'}
}
\value{
A tbl_dbi with the generated (or existing) log table
}
\description{
Create a table with the SCDB log structure if it does not exists
}
\examples{
conn <- get_connection(drv = RSQLite::SQLite())
log_table_id <- id("test.logs", conn = conn, allow_table_only = TRUE)

create_logs_if_missing(log_table_id, conn)

close_connection(conn)
}
