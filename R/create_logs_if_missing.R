#' Create a table with the SCDB log structure if it does not exists
#'
#' @template conn
#' @param log_table (`id-like object`)\cr
#'   A table specification where the logs should exist (coercible by `id()`).
#' @return
#'   Invisibly returns the generated (or existing) log table.
#' @examplesIf requireNamespace("RSQLite", quietly = TRUE)
#'   conn <- get_connection()
#'   log_table <- id("test.logs", conn = conn, allow_table_only = TRUE)
#'
#'   create_logs_if_missing(conn, log_table)
#'
#'   close_connection(conn)
#' @export
create_logs_if_missing <- function(conn, log_table) {

  checkmate::assert_class(conn, "DBIConnection")

  if (!table_exists(conn, log_table)) {
    log_signature <- data.frame(
      date = as.POSIXct(character(0)),
      catalog = character(0),
      schema = character(0),
      table = character(0),
      n_insertions = integer(0),
      n_deactivations = integer(0),
      start_time = as.POSIXct(character(0)),
      end_time = as.POSIXct(character(0)),
      duration = character(0),
      success = logical(),
      message = character(0),
      log_file = character(0)
    )

    if (!checkmate::test_multi_class(conn, c("Microsoft SQL Server", "duckdb_connection"))) {
      log_signature <- dplyr::select(log_signature, !"catalog")
    }

    DBI::dbCreateTable(conn, id(log_table, conn), log_signature)
  }

  return(dplyr::tbl(conn, id(log_table, conn)))
}
