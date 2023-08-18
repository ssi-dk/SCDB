#' Create a historical table from input data
#'
#' @name create_table
#'
#' @template .data
#' @template conn
#' @template db_table_id
#' @param temporary Should the table be created as a temporary table?
#' @param ... Other arguments passed to [DBI::dbCreateTable()]
#' @returns Invisibly returns the table as it looks on the destination (or locally if conn is NULL)
#' @examples
#' conn <- get_connection()
#'
#' create_table(mtcars, conn = conn, db_table_id = "mtcars_tmp")
#'
#' close_connection(conn)
#' @export
create_table <- function(.data, conn = NULL, db_table_id = NULL, temporary = TRUE, ...) {

  checkmate::assert_class(.data, "data.frame")
  checkmate::assert_class(conn, "DBIConnection", null.ok = TRUE)
  assert_id_like(db_table_id)

  # Assert unique column names (may cause unexpected getTableSignature results)
  checkmate::assert_character(names(.data), unique = TRUE)

  # Convert db_table_id to id (id() returns early if this is the case)
  if (!is.null(db_table_id)) { # TODO: db_table_name vs db_table_id
    db_table_id <- id(db_table_id, conn = conn)
  } else {
    db_table_id <- deparse(substitute(.data))
  }

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

  # Create the table on the remote and return the table
  stopifnot("Table already exists!" = !table_exists(conn, db_table_id))
  DBI::dbCreateTable(conn = conn,
                     name = db_table_id,
                     fields = getTableSignature(.data = .data, conn = conn),
                     temporary = temporary,
                     ...)

  invisible(dplyr::tbl(conn, db_table_id))
}



# TODO: some development comments
#' @importFrom methods setGeneric
methods::setGeneric("getTableSignature",
                    function(.data, conn = NULL) standardGeneric("getTableSignature"),
                    signature = "conn")

#'
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
    )
  )

  # Update columns with indices instead of names to avoid conflicts
  special_cols <- backend_coltypes[[class(conn)]]
  special_indices <- (1 + length(.data) - length(special_cols)):length(.data)

  return(replace(col_types, special_indices, special_cols))
})

#'
methods::setMethod("getTableSignature", "NULL", function(.data, conn) {
  # Emulate product of DBI::dbDataType
  signature <- dplyr::summarise(.data, dplyr::across(tidyselect::everything(), ~ class(.)[1]))

  stats::setNames(as.character(signature), names(signature))

  return(signature)
})


#' Create a table with the mg log structure if it does not exists
#' @template conn
#' @param log_table A specification of where the logs should exist ("schema.table")
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
                                log_file = NA_character_) |>
      utils::head(0)

    DBI::dbWriteTable(conn, id(log_table, conn), log_signature)
  }

  return(dplyr::tbl(conn, id(log_table, conn)))
}
