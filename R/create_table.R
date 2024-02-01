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
  if (is.null(conn)) return(invisible(.data))

  # Check db_table_id conforms to requirements
  # Temporary tables on some back ends must to begin with "#"
  if (purrr::pluck(list(...), "temporary", .default = formals(DBI::dbCreateTable)$temporary)) {

    db_table_schema <- purrr::pluck(db_table_id, "name", "schema")
    db_table_name <- purrr::pluck(db_table_id, "name", "table", .default = db_table_id)

    db_table_id <- DBI::Id(
      schema = db_table_schema,
      table = paste0(
        ifelse(inherits(conn, "Microsoft SQL Server") && !startsWith(db_table_name, "#"), "#", ""),
        db_table_name
      )
    )

  } else {

    # Non-temporary tables must be fully qualified
    db_table_id <- id(db_table_id, conn)

  }

  # Create the table on the remote and return the table
  DBI::dbWriteTable(
    conn = conn,
    name = db_table_id,
    value = .data,
    fields = getTableSignature(.data = .data, conn = conn),
    ...
  )

  return(invisible(dplyr::tbl(conn, db_table_id, check_from = FALSE)))
}
