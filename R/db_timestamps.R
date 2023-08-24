#' Determine the type of timestamps the DB supports
#' @name db_timestamp
#' @param timestamp The timestamp to be transformed to the DB type. Can be character.
#' @param conn A `DBIConnection` to the DB where the timestamp should be stored
#' @examples
#' conn <- get_connection(drv = RSQLite::SQLite())
#'
#' db_timestamp(Sys.time(), conn)
#'
#' close_connection(conn)
#' @return The given timestamp converted to a SQL-backend dependent timestamp
#' @export
db_timestamp <- function(timestamp, conn) {
  UseMethod("db_timestamp", conn)
}

#' @rdname db_timestamp
#' @export
db_timestamp.default <- function(timestamp, conn) {
  if (inherits(timestamp, "POSIXt")) timestamp <- format(timestamp)
  return(dbplyr::translate_sql(as.POSIXct(!!timestamp), con = conn))
}

#' @rdname db_timestamp
#' @export
db_timestamp.SQLiteConnection <- function(timestamp, conn) {
  if (is.na(timestamp)) {
    dbplyr::translate_sql(NA_character_, con = conn)
  } else {
    if (inherits(timestamp, "POSIXt")) timestamp <- format(timestamp)
    dbplyr::translate_sql(!!strftime(timestamp), con = conn)
  }
}
