#' Get the current schema / catalog of a database-related objects
#'
#' @name get_schema
#' @param obj The object from which to retrieve a schema
#' @param temporary (`logical(1)`) \cr
#'   Should the reference be to the temporary schema/database?
#' @param ... Further arguments passed to methods.
#' @return
#' For `get_schema.DBIConnection`, the current schema of the connection if `temporary = FALSE``.
#' See "Default schema" for more.
#' If `temporary = TRUE`, the temporary schema of the connection is returned.
#'
#' For `get_schema.tbl_dbi` the schema is determined via `id()`.
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
#' `main`.
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
get_schema <- function(obj, ...) {
  UseMethod("get_schema")
}

#' @export
get_schema.tbl_dbi <- function(obj,  ...) {
  return(purrr::pluck(id(obj), "name", "schema"))
}

#' @export
get_schema.Id <- function(obj,  ...) {
  return(purrr::pluck(obj@name, "schema"))
}

#' @export
#' @rdname get_schema
get_schema.PqConnection <- function(obj, temporary = FALSE,  ...) {
  if (isTRUE(temporary))  {
    return("pg_temp")
  } else {
    return(DBI::dbGetQuery(obj, "SELECT CURRENT_SCHEMA()")$current_schema)
  }
}

#' @export
#' @rdname get_schema
get_schema.SQLiteConnection <- function(obj, temporary = FALSE,  ...) {
  if (isTRUE(temporary))  {
    return("temp")
  } else {
    return("main")
  }
}

#' @export
`get_schema.Microsoft SQL Server` <- function(obj, ...) {
  query <- paste("SELECT ISNULL((SELECT",
                 "COALESCE(default_schema_name, 'dbo') AS default_schema",
                 "FROM sys.database_principals",
                 "WHERE [name] = CURRENT_USER), 'dbo') default_schema")

  return(DBI::dbGetQuery(obj, query)$default_schema)
}

#' @export
get_schema.NULL <- function(obj, ...) {
  return(NULL)
}
