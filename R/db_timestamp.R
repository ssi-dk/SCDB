#' Determine the type of timestamps the database supports
#'
#' @name db_timestamp
#' @param timestamp (`POSIXct(1)` or `character(1)`)\cr
#'   The timestamp to be transformed to the database type.
#' @template conn
#' @return
#'   The given timestamp converted to a SQL-backend dependent timestamp.
#' @examplesIf requireNamespace("RSQLite", quietly = TRUE)
#'   conn <- get_connection()
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

  # Parse timestamp to posix and then format to character string in the local tz
  # This way, we should get a consistent format which dbplyr can translate
  # If we kept as POSIXct, dbplyr would not correctly translate.
  ts <- format(to_posix(timestamp), tz = Sys.timezone())

  # Wrap in `as.POSIXct()` call to trigger dbplyr translations
  return(dbplyr::translate_sql(as.POSIXct(!!ts), con = conn))

}

#' @export
db_timestamp.NULL <- function(timestamp, conn) {
  return(to_posix(timestamp))
}

#' @export
db_timestamp.SQLiteConnection <- function(timestamp, conn) {
  if (is.na(timestamp)) {
    return(dbplyr::translate_sql(NA_character_, con = conn))
  } else {
    return(dbplyr::translate_sql(!!as.character(to_posix(timestamp)), con = conn))
  }
}

#' @export
db_timestamp.duckdb_connection <- function(timestamp, conn) {
  # Do not format before letting duckdb cast to DB

  # Wrap in `as.POSIXct()` call to trigger dbplyr translations
  return(dbplyr::translate_sql(as.POSIXct(!!to_posix(timestamp)), con = conn))
}

#' @noRd
to_posix <- function(timestamp) {

  # Convert all timezones to local timezone
  if (inherits(timestamp, "POSIXct")) {
    timestamp <- lubridate::with_tz(timestamp, tz = Sys.timezone())
  }

  # Cast to POSIXct with local timezone
  lubridate::parse_date_time(
    timestamp,
    orders = c(
      "ymdHMSOS",
      "ymdHMS",
      "ymdHM",
      "ymd"
    ),
    tz = Sys.timezone()
  )
}
