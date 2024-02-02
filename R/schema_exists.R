#' Test if a schema exists in given connection
#' @param schema A character string giving the schema name
#' @template conn
#' @return TRUE if the given schema is found on conn
#' @examples
#'
#' conn <- get_connection(drv = RSQLite::SQLite())
#'
#' schema_exists(conn, "test")
#'
#' close_connection(conn)
#' @export
schema_exists <- function(conn, schema) {
  UseMethod("schema_exists")
}

#' @export
schema_exists.SQLiteConnection <- function(conn, schema) {
  query <- paste0(
    "SELECT schema, name FROM pragma_table_list WHERE schema == '",
    schema,
    "' AND name IN ('sqlite_schema', 'sqlite_temp_schema')"
  )
  result <- DBI::dbGetQuery(conn, query)

  return(nrow(result) == 1)
}

#' @export
schema_exists.DBIConnection <- function(conn, schema) {
  query <- paste0("SELECT schema_name FROM INFORMATION_SCHEMA.SCHEMATA WHERE schema_name = '", schema, "'")
  result <- DBI::dbGetQuery(conn, query)

  return(nrow(result) == 1)
}

#' @export
schema_exists.default <- function(conn, schema) {

  checkmate::assert_character(schema)

  objs <- DBI::dbListObjects(conn)
  matches <- sapply(objs$table, \(.x) methods::slot(.x, "name")) |>
    (\(.x) names(.x) == "schema" & .x == schema)()

  if (any(matches)) return(TRUE)

  tryCatch(
    {
      DBI::dbCreateTable(
        conn,
        name = DBI::Id(schema = schema, table = "SCDB_schema_test"),
        fields = data.frame(name = character()),
        temporary = FALSE
      )

      DBI::dbRemoveTable(conn, DBI::Id(schema = schema, table = "SCDB_schema_test"))
      return(TRUE)
    },
    error = function(e) {
      return(FALSE)
    }
  )
}
