#' Get the current schema of a DB connection
#'
#' @param .x A DBIConnection or lazy_query object
#' @return The current schema name, but defaults to "prod" instead of "public"
#' @examples
#' conn <- get_connection(drv = RSQLite::SQLite())
#'
#' dplyr::copy_to(conn, mtcars, name = "mtcars")
#'
#' get_schema(conn)
#' get_schema(get_table(conn, "mtcars"))
#'
#' close_connection(conn)
#' @export
get_schema <- function(.x) { # nocov start

  if (inherits(.x, "PqConnection")) {
    # Get schema from connection object
    schema <- DBI::dbGetQuery(.x, "SELECT CURRENT_SCHEMA()")$current_schema

  } else if (inherits(.x, "SQLiteConnection") || inherits(.x, "tbl_SQLiteConnection")) {
    return()
  } else if (inherits(.x, "tbl_dbi")) {
    # Get schema from a DBI object (e.g. lazy query)
    schema <- stringr::str_extract_all(dbplyr::remote_query(.x), '(?<=FROM \")[^"]*')[[1]]
    if (length(unique(schema)) > 1) {
      # Not sure if this is even possible due to dbplyr limitations
      warning("Multiple different schemas detected. You might need to handle these (more) manually:\n",
              paste(unique(schema), collapse = ", "))
    } else {
      schema <- unique(schema)
    }
  } else {
    stop("Could not detect object type")
  }

  if (schema == "public") schema <- "prod"

  return(schema)
} # nocov end


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

  checkmate::assert_class(conn, "DBIConnection")
  checkmate::assert_character(schema)

  if (inherits(conn, "SQLiteConnection")) return(FALSE)

  objs <- DBI::dbListObjects(conn)
  matches <- sapply(objs$table, \(.x) methods::slot(.x, "name")) |>
    (\(.x) names(.x) == "schema" & .x == schema)()

  if (any(matches)) return(TRUE)

  tryCatch({
    DBI::dbCreateTable(
      conn,
      name = DBI::Id(schema = schema, table = "SCDB_schema_test"),
      fields = data.frame(name = character()),
      temporary = FALSE
    )

    DBI::dbRemoveTable(conn, DBI::Id(schema = schema, table = "SCDB_schema_test"))
    TRUE
  },
  error = function(e) FALSE)
}
