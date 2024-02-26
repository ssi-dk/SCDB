#' Determine the type of timestamps the DB supports
#'
#' @name db_timestamp
#' @param timestamp (`POSIXct(1)` or `character(1)`)\cr
#'   The timestamp to be transformed to the DB type.
#' @template conn
#' @return
#'   The given timestamp converted to a SQL-backend dependent timestamp.
#' @examplesIf requireNamespace("RSQLite", quietly = TRUE)
#'   conn <- get_connection(drv = RSQLite::SQLite())
#'
#'   db_timestamp(Sys.time(), conn)
#'
#'   close_connection(conn)
#' @export
db_timestamp <- function(timestamp, conn = NULL) {
  if (is.null(conn)) db_timestamp.NULL(timestamp, conn)
  else UseMethod("db_timestamp", conn)
}

#' @export
db_timestamp.default <- function(timestamp, conn) {
  if (inherits(timestamp, "POSIXt")) timestamp <- format(timestamp)
  return(dbplyr::translate_sql(as.POSIXct(!!timestamp), con = conn))
}

#' @export
db_timestamp.NULL <- function(timestamp, conn) {
  if (inherits(timestamp, "POSIXt")) timestamp <- format(timestamp)
  return(timestamp)
}

#' @export
db_timestamp.SQLiteConnection <- function(timestamp, conn) {
  if (is.na(timestamp)) {
    return(dbplyr::translate_sql(NA_character_, con = conn))
  } else {
    if (inherits(timestamp, "POSIXt")) timestamp <- format(timestamp)
    return(dbplyr::translate_sql(!!strftime(timestamp), con = conn))
  }
}

#' @export
db_timestamp.duckdb_connection <- function(timestamp, conn) {
  if (inherits(timestamp, "character")) timestamp <- as.POSIXct(timestamp, tz = Sys.timezone()) # Add local tz
  return(dbplyr::translate_sql(!!as.POSIXct(timestamp, tz = "UTC"), con = conn)) # duckdb only stores as UTC
}
