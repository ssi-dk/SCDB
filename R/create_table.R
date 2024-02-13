#' Create a historical table from input data
#'
#' @name create_table
#'
#' @template .data
#' @template conn
#' @template db_table_id
#' @param ... Other arguments passed to [DBI::dbCreateTable()]
#' @return Invisibly returns the table as it looks on the destination (or locally if conn is NULL)
#' @examples
#' conn <- get_connection(drv = RSQLite::SQLite())
#'
#' create_table(mtcars, conn = conn, db_table_id = "mtcars")
#'
#' close_connection(conn)
#' @export
create_table <- function(.data, conn = NULL, db_table_id, ...) {

  checkmate::assert_class(.data, "data.frame")
  checkmate::assert_class(conn, "DBIConnection", null.ok = TRUE)
  assert_id_like(db_table_id)

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
  db_table_id <- id(db_table_id)

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

    dbi_create_table_id <- db_table_id <- id(db_table_id, conn) # If permanent, use all availalbe info
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

  return(invisible(dplyr::tbl(conn, db_table_id, check_from = FALSE)))
}
