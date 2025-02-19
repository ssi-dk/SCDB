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
  db_table <- id(db_table, conn)

  DBI::dbExecute(
    conn,
    glue::glue(
      "CREATE UNIQUE INDEX ON {as.character(db_table, explicit = TRUE)} ({toString(columns)})"
    )
  )
}

#' @export
create_index.SQLiteConnection <- function(conn, db_table, columns) {
  db_table <- id(db_table, conn)

  schema <- purrr::pluck(db_table, "name", "schema")
  table  <- purrr::pluck(db_table, "name", "table")

  if (schema %in% c("main", "temp")) schema <- NULL

  # Generate index name
  index <- paste(
    purrr::map(
      c(
        schema,
        paste(c(table, "scdb_index", columns), collapse = "_")
      ),
      shQuote
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
create_index.DBIConnection <- function(conn, db_table, columns) {
  db_table <- id(db_table, conn)

  index <- glue::glue("{db_table}_scdb_index_{paste(columns, collapse = '_')}") %>%
    stringr::str_replace_all(stringr::fixed("."), "_")

  query <- glue::glue(
    "CREATE UNIQUE INDEX {index} ON {as.character(db_table, explicit = TRUE)} ({toString(columns)})"
  )

  DBI::dbExecute(conn, query)
}
