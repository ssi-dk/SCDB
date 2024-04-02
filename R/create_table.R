#' Create a historical table from input data
#'
#' @template .data
#' @template conn
#' @template db_table
#' @param ...
#'   Other arguments passed to [DBI::dbCreateTable()].
#' @return
#'   Invisibly returns the table as it looks on the destination (or locally if `conn` is `NULL`).
#' @examplesIf requireNamespace("RSQLite", quietly = TRUE)
#'   conn <- get_connection()
#'
#'   create_table(mtcars, conn = conn, db_table = "mtcars")
#'
#'   close_connection(conn)
#' @export
create_table <- function(.data, conn = NULL, db_table, ...) {                                                           # nolint: function_argument_linter

  checkmate::assert_class(.data, "data.frame")
  checkmate::assert_class(conn, "DBIConnection", null.ok = TRUE)
  assert_id_like(db_table)

  # Assert unique column names (may cause unexpected getTableSignature results)
  checkmate::assert_character(names(.data), unique = TRUE)

  if (is.historical(.data)) {
    stop("checksum/from_ts/until_ts column(s) already exist(s) in .data!")
  }

  # Add "metadata" columns to .data
  .data <- .data |>
    dplyr::mutate(checksum = NA_character_,
                  from_ts  = as.POSIXct(NA_real_),
                  until_ts = as.POSIXct(NA_real_),
                  .after = tidyselect::everything())

  # Early return if there is no connection to push to
  if (is.null(conn)) return(invisible(utils::head(.data, 0)))

  # Convert to id
  # But supply no "conn" argument to prevent inference of (default) schema
  db_table_id <- id(db_table)

  # Check db_table_id conforms to requirements:
  # 1) Temporary tables on some backends must to begin with "#".
  # 2) DBI::dbCreateTable requires that table Ids are unqualified if the table should be temporary
  if (purrr::pluck(list(...), "temporary", .default = formals(DBI::dbCreateTable)$temporary)) {

    # If catalog/schema is given, it must match the temporary locations
    checkmate::assert_choice(
      purrr::pluck(db_table_id, "name", "schema"), get_schema(conn, temporary = TRUE),
      null.ok = TRUE
    )
    checkmate::assert_choice(
      purrr::pluck(db_table_id, "name", "catalog"), get_catalog(conn, temporary = TRUE),
      null.ok = TRUE
    )

    table <- purrr::pluck(db_table_id, "name", "table")
    schema <- purrr::pluck(db_table_id, "name", "schema", .default = get_schema(conn, temporary = TRUE))
    catalog <- purrr::pluck(db_table_id, "name", "catalog", .default = get_catalog(conn, temporary = TRUE))

    if (inherits(conn, "Microsoft SQL Server") && !startsWith(table, "#")) {
      table <- paste0("#", table)
    }

    # Create full and partial Ids of the table to create
    dbi_create_table_id <- DBI::Id(table = table)
    db_table_id <- DBI::Id("catalog" = catalog, "schema" = schema, "table" = table)

  } else {

    dbi_create_table_id <- db_table_id <- id(db_table_id, conn) # If permanent, use all available info
  }

  # Check if the table already exists
  if (table_exists(conn, id(db_table_id, conn))) {
    stop("Table ", db_table_id, " already exists!")
  }

  # Create the table on the remote and return the table
  DBI::dbCreateTable(
    conn = conn,
    name = dbi_create_table_id,
    fields = getTableSignature(.data = .data, conn = conn),
    ...
  )

  create_index(conn, db_table_id, columns = c("checksum", "from_ts"))

  return(invisible(dplyr::tbl(conn, db_table_id)))
}


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

  UseMethod("create_index")
}

#' @export
create_index.PqConnection <- function(conn, db_table_id, columns) {
  DBI::dbExecute(
    conn,
    glue::glue(
      "CREATE UNIQUE INDEX ON {as.character(db_table_id, explicit = TRUE)} ({toString(columns)})"
    )
  )
}

#' @export
create_index.SQLiteConnection <- function(conn, db_table_id, columns) {
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

  index <- glue::glue("{db_table_id}_scdb_index_{paste(columns, collapse = '_')}") |>
    stringr::str_replace_all(stringr::fixed("."), "_")

  query <- glue::glue(
    "CREATE UNIQUE INDEX {index} ON {as.character(db_table_id, explicit = TRUE)} ({toString(columns)})"
  )

  DBI::dbExecute(conn, query)
}
