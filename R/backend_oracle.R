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

oracle_jdbc_table_exists <- function(conn, name) {
  db_table_id <- id(name, conn)

  schema_name <- purrr::pluck(db_table_id, "name", "schema")
  table_name <- purrr::pluck(db_table_id, "name", "table")

  if (is.null(schema_name) || is.na(schema_name)) {
    schema_name <- get_schema(conn)
  }

  schema_candidates <- unique(c(schema_name, toupper(schema_name)))
  table_candidates <- unique(c(table_name, toupper(table_name)))

  quote_chr <- function(x) {
    paste(as.character(DBI::dbQuoteString(conn, x)), collapse = ", ")
  }

  query <- paste(
    "SELECT COUNT(*) AS n",
    "FROM all_objects",
    "WHERE object_type IN ('TABLE', 'VIEW')",
    "AND owner IN (", quote_chr(schema_candidates), ")",
    "AND object_name IN (", quote_chr(table_candidates), ")"
  )

  result <- DBI::dbGetQuery(conn, query)

  return(as.integer(result[[1]][[1]]) > 0L)
}


#' @importFrom DBI dbExistsTable
#' @export
methods::setMethod(
  "dbExistsTable",
  signature(conn = "JDBCConnection", name = "Id"),
  function(conn, name, ...) {
    oracle_jdbc_table_exists(conn, name)
  }
)
