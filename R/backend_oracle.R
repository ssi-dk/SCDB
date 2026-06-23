#' @exportS3Method dbplyr::sql_dialect
sql_dialect.JDBCConnection <- function(con) {
  dialect <- dbplyr::sql_dialect(dbplyr::simulate_oracle())
  class(dialect) <- c("sql_dialect_scdb_oracle_jdbc", class(dialect))
  return(dialect)
}

#' @exportS3Method dbplyr::sql_table_analyze
sql_table_analyze.sql_dialect_scdb_oracle_jdbc <- function(con, table, ...) {
  NULL
}
