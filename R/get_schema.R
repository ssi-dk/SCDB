#' Get the current schema of a database-related objects
#'
#' @param .x The object from which to retrieve a schema
#' @return The current schema name, but defaults to "prod" instead of "public"
#' @examples
#' conn <- get_connection(drv = RSQLite::SQLite())
#'
#' dplyr::copy_to(conn, mtcars, name = "mtcars")
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
    "' AND name == 'sqlite_schema'"
  )
  result <- DBI::dbGetQuery(conn, query)

  return(nrow(result) == 1)
}

#' @export
schema_exists.PqConnection <- function(conn, schema) {
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
