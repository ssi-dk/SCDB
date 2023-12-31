% Generated by roxygen2: do not edit by hand
% Please edit documentation in R/digest_to_checksum.R
\name{digest_to_checksum}
\alias{digest_to_checksum}
\title{Computes an MD5 checksum from columns}
\usage{
digest_to_checksum(.data, col = "checksum", exclude = NULL, warn = TRUE)
}
\arguments{
\item{.data}{A data frame, a tibble, a data.table or a tbl.}

\item{col}{Name of the column to put the checksums in}

\item{exclude}{Columns to exclude from the checksum generation}

\item{warn}{Flag to warn if target column already exists in data}
}
\value{
.data with an checksum column added
}
\description{
Computes an MD5 checksum from columns
}
\examples{
digest_to_checksum(mtcars)

}
