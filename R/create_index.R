#' Create the indexes on table
#' @param conn (`DBIConnection`)\cr
#'   A connection to a database.
#' @template db_table
#' @param columns (`character()`)\cr
#'   The columns that should be unique.
#' @return
#'   NULL (called for side effects)
#' @examplesIf requireNamespace("RSQLite", quietly = TRUE)
#'   conn <- get_connection()
#'
#'   mt <- dplyr::copy_to(conn, dplyr::distinct(mtcars, .data$mpg, .data$cyl), name = "mtcars")
#'   create_index(conn, mt, c("mpg", "cyl"))
#'
#'   close_connection(conn)
#' @export
create_index <- function(conn, db_table, columns) {
  checkmate::assert_class(conn, "DBIConnection")
  assert_id_like(db_table)
  checkmate::assert_character(columns)
  checkmate::assert_true(table_exists(conn, db_table))

  UseMethod("create_index")
}

#' @export
create_index.PqConnection <- function(conn, db_table, columns) {
  db_table_id <- id(db_table, conn)                                                                                     # nolint: object_usage_linter

  query <- dbplyr::sql_glue2(
    conn,
    "CREATE UNIQUE INDEX ON {.tbl db_table_id} ({.id columns})"
  )

  DBI::dbExecute(conn, query)
}

#' @export
create_index.SQLiteConnection <- function(conn, db_table, columns) {
  db_table_id <- id(db_table, conn)

  schema <- purrr::pluck(db_table_id, "name", "schema")
  table <- purrr::pluck(db_table_id, "name", "table")

  has_schema <- !(schema %in% c("main", "temp"))

  # Generate index name
  index_name <- paste(c(table, "scdb_index", columns), collapse = "_")

  index_id <- if (has_schema) {                                                                                         # nolint: object_usage_linter
    DBI::Id(schema = schema, table = index_name)
  } else {
    index_name
  }

  query <- dbplyr::sql_glue2(
    conn,
    "CREATE UNIQUE INDEX {.tbl index_id} ON {.id table} ({.id columns})"
  )

  DBI::dbExecute(conn, query)
}

#' @export
create_index.DBIConnection <- function(conn, db_table, columns) {
  db_table_id <- id(db_table, conn)                                                                                     # nolint: object_usage_linter

  index <- glue::glue("{db_table}_scdb_index_{paste(columns, collapse = '_')}") |>                                      # nolint: object_usage_linter
    stringr::str_replace_all(stringr::fixed("."), "_")

  query <- dbplyr::sql_glue2(
    conn,
    "CREATE UNIQUE INDEX {.id index} ON {.tbl db_table_id} ({.id columns})"
  )

  DBI::dbExecute(conn, query)
}
