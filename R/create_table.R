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



#' @importFrom methods setGeneric
methods::setGeneric("getTableSignature",
                    function(.data, conn = NULL) standardGeneric("getTableSignature"),
                    signature = "conn")

methods::setMethod("getTableSignature", "DBIConnection", function(.data, conn) {
  # Define the column types to be updated based on backend class
  col_types <- DBI::dbDataType(conn, .data)

  backend_coltypes <- list(
    "PqConnection" = c(
      checksum = "TEXT",
      from_ts  = "TIMESTAMP",
      until_ts = "TIMESTAMP"
    ),
    "SQLiteConnection" = c(
      checksum = "TEXT",
      from_ts  = "TEXT",
      until_ts = "TEXT"
    ),
    "Microsoft SQL Server" = c(
      checksum = "varchar(32)",
      from_ts  = "DATETIME2",
      until_ts = "DATETIME2"
    )
  )

  checkmate::assert_choice(class(conn), names(backend_coltypes))

  # Update columns with indices instead of names to avoid conflicts
  special_cols <- backend_coltypes[[class(conn)]]
  special_indices <- (1 + length(.data) - length(special_cols)):length(.data)

  return(replace(col_types, special_indices, special_cols))
})

methods::setMethod("getTableSignature", "NULL", function(.data, conn) {
  # Emulate product of DBI::dbDataType
  signature <- dplyr::summarise(.data, dplyr::across(tidyselect::everything(), ~ class(.)[1]))

  stats::setNames(as.character(signature), names(signature))

  return(signature)
})


#' Create a table with the SCDB log structure if it does not exists
#' @template conn
#' @param log_table A specification of where the logs should exist ("schema.table")
#' @return A tbl_dbi with the generated (or existing) log table
#' @examples
#' conn <- get_connection(drv = RSQLite::SQLite())
#' log_table_id <- id("test.logs", conn = conn, allow_table_only = TRUE)
#'
#' create_logs_if_missing(log_table_id, conn)
#'
#' close_connection(conn)
#' @export
create_logs_if_missing <- function(log_table, conn) {

  checkmate::assert_class(conn, "DBIConnection")

  if (!table_exists(conn, log_table)) {
    log_signature <- data.frame(date = as.POSIXct(NA),
                                schema = NA_character_,
                                table = NA_character_,
                                n_insertions = NA_integer_,
                                n_deactivations = NA_integer_,
                                start_time = as.POSIXct(NA),
                                end_time = as.POSIXct(NA),
                                duration = NA_character_,
                                success = NA,
                                message = NA_character_,
                                log_file = NA_character_)

    DBI::dbCreateTable(conn, id(log_table, conn), log_signature)
  }

  return(dplyr::tbl(conn, id(log_table, conn), check_from = FALSE))
}
