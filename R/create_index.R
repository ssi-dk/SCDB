#' Create the indexes on table
#' @param conn (`DBIConnection`)\cr
#'   A connection to a database.
#' @param db_table_id (`Id`)\cr
#'   The table to create the index for.
#' @param columns (`character()`)\cr
#'   The columns that should be unique.
#' @return
#'   NULL (called for side effects)
#' @examplesIf requireNamespace("RSQLite", quietly = TRUE)
#'   conn <- get_connection()
#'
#'   dplyr::copy_to(conn, dplyr::distinct(mtcars, .data$mpg, .data$cyl), name = "mtcars")
#'   create_index(conn, "mtcars", c("mpg", "cyl"))
#'
#'   close_connection(conn)
#' @export
create_index <- function(conn, db_table_id, columns) {
  checkmate::assert_class(conn, "DBIConnection")
  assert_id_like(db_table_id)
  checkmate::assert_character(columns)
  checkmate::assert_true(table_exists(conn, db_table_id))

  UseMethod("create_index")
}

#' @export
create_index.PqConnection <- function(conn, db_table_id, columns) {
  db_table_id <- id(db_table_id, conn)

  DBI::dbExecute(
    conn,
    glue::glue(
      "CREATE UNIQUE INDEX ON {as.character(db_table_id, explicit = TRUE)} ({toString(columns)})"
    )
  )
}

#' @export
create_index.SQLiteConnection <- function(conn, db_table_id, columns) {
  db_table_id <- id(db_table_id, conn)

  schema <- purrr::pluck(db_table_id, "name", "schema")
  table  <- purrr::pluck(db_table_id, "name", "table")

  if (schema %in% c("main", "temp")) schema <- NULL

  # Generate index name
  index <- paste(
    c(
      shQuote(schema),
      shQuote(paste0(c(table, "scdb_index", columns), collapse = "_"))
    ),
    collapse = "."
  )

  DBI::dbExecute(
    conn,
    glue::glue(
      "CREATE UNIQUE INDEX {index} ON {shQuote(table)} ({toString(columns)})"
    )
  )
}

#' @export
create_index.DBIConnection <- function(conn, db_table_id, columns) {
  db_table_id <- id(db_table_id, conn)

  index <- glue::glue("{db_table_id}_scdb_index_{paste(columns, collapse = '_')}") |>
    stringr::str_replace_all(stringr::fixed("."), "_")

  query <- glue::glue(
    "CREATE UNIQUE INDEX {index} ON {as.character(db_table_id, explicit = TRUE)} ({toString(columns)})"
  )

  DBI::dbExecute(conn, query)
}
