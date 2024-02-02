#' Create a table with the SCDB log structure if it does not exists
#' @template conn
#' @param log_table A specification of where the logs should exist ("schema.table")
#' @return A tbl_dbi with the generated (or existing) log table
#' @examples
#' conn <- get_connection(drv = RSQLite::SQLite())
#' log_table_id <- id("test.logs", conn = conn, allow_table_only = TRUE)
#'
#' create_logs_if_missing(conn, log_table_id)
#'
#' close_connection(conn)
#' @export
create_logs_if_missing <- function(conn, log_table) {

  checkmate::assert_class(conn, "DBIConnection")

  if (!table_exists(conn, log_table)) {
    log_signature <- data.frame(date = as.POSIXct(NA),
                                schema = NA_character_,
                                table = NA_character_,
                                n_insertions = NA_integer_,
                                n_deactivations = NA_integer_,
                                start_time = as.POSIXct(NA),
                                end_time = as.POSIXct(NA),
                                duration = NA_character_,
                                success = NA,
                                message = NA_character_,
                                log_file = NA_character_)

    DBI::dbCreateTable(conn, id(log_table, conn), log_signature)
  }

  return(dplyr::tbl(conn, id(log_table, conn), check_from = FALSE))
}
