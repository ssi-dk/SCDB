#' Get the current schema of a database-related objects
#'
#' @param .x The object from which to retrieve a schema
#' @return
#' For `DBIConnection` objects, the current schema of the connection. See "default schema" for more.
#'
#' For `tbl_dbi` objects, the schema as retrieved from the lazy_query.
#' If the lazy_query does not specify a schema, `NA` is returned.
#' Note that lazy queries are sensitive to server-side changes and may therefore return entirely different tables
#' if changes are made server-side.
#'
#' @section Default schema:
#'
#' In some backends, it is possible to modify settings so that when a schema is not explicitly stated in a query,
#' the backend searches for the table in this schema by default.
#' For Postgres databases, this can be shown with `SELECT CURRENT_SCHEMA()` (defaults to `public`) and modified with
#' `SET search_path TO { schema }`.
#'
#' For SQLite databases, a `temp` schema for temporary tables always exists as well as a `main` schema for permanent
#' tables.
#' Additional databases may be attached to the connection with a named schema, but as the attachment must be made after
#' the connection is established, `get_schema` will never return any of these, as the default schema will always be
#' one of `main` or `temp` (if `main` contains no tables).
#' A return value of `NA` means that no tables exist within the connection and no default schema therefore exists.
#'
#' @examples
#' conn <- get_connection(drv = RSQLite::SQLite())
#'
#' dplyr::copy_to(conn, mtcars, name = "mtcars", temporary = FALSE)
#'
#' get_schema(conn)
#' get_schema(get_table(conn, id("mtcars", conn = conn)))
#'
#' close_connection(conn)
#' @export
get_schema <- function(.x) {
  UseMethod("get_schema")
}

#' @export
get_schema.tbl_dbi <- function(.x) {
  return(unclass(dbplyr::remote_table(.x))$schema)
}

#' @export
get_schema.Id <- function(.x) {
  return(unname(.x@name["schema"]))
}

#' @export
get_schema.PqConnection <- function(.x) {
  return(DBI::dbGetQuery(.x, "SELECT CURRENT_SCHEMA()")$current_schema)
}

#' @importFrom rlang .data
#' @export
get_schema.SQLiteConnection <- function(.x) {
  schemata <-
    DBI::dbGetQuery(.x, "PRAGMA table_list") |>
    dplyr::filter(!.data$name %in% c("sqlite_schema", "sqlite_temp_schema"),
                  !grepl("^sqlite_stat", .data$name)) |>
    dplyr::pull(.data$schema) |>
    unique()

  if ("main" %in% schemata) {
    return("main")
  } else if ("temp" %in% schemata) {
    return("temp")
  } else {
    return(NA_character_)
  }
}

#' @export
`get_schema.Microsoft SQL Server` <- function(.x) {
  query <- paste("SELECT ISNULL((SELECT",
                 "COALESCE(default_schema_name, 'dbo') AS default_schema",
                 "FROM sys.database_principals",
                 "WHERE [name] = CURRENT_USER), 'dbo') default_schema")

  return(DBI::dbGetQuery(.x, query)$default_schema)
}

#' @export
get_schema.NULL <- function(.x) {
  return(NULL)
}

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
