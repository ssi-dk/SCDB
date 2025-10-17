#' Get (and override) the column types for a table for a given connection
#'
#' @description
#'   The column types are determined via "DBI::dbDataType".
#'   If the last three columns are named "checksum", "from_ts", and "until_ts",
#'   we override the column types with backend-specific types.
#' @template .data
#' @template conn
#' @importFrom methods setGeneric
#' @noRd
methods::setGeneric("getTableSignature",
                    function(.data, conn = NULL) standardGeneric("getTableSignature"),
                    signature = "conn")

#' @importMethodsFrom RJDBC dbDataType
#' @importMethodsFrom odbc dbDataType
#' @importClassesFrom DBI DBIConnection
methods::setMethod("getTableSignature", "DBIConnection", function(.data, conn) {

  # Retrieve the translated data types
  signature <- purrr::map(utils::head(.data, 0), ~ DBI::dbDataType(conn, .))

  # Define the column types to be updated based on backend class
  backend_coltypes <- list(
    "SQLiteConnection" = c(
      checksum = "TEXT",
      from_ts  = "TIMESTAMP", # Stored internally as TEXT
      until_ts = "TIMESTAMP"  # Stored internally as TEXT
    ),
    "PqConnection" = c(
      checksum = "CHAR(32)",
      from_ts  = "TIMESTAMP",
      until_ts = "TIMESTAMP"
    ),
    "Microsoft SQL Server" = c(
      checksum = "CHAR(64)",
      from_ts  = "DATETIME",
      until_ts = "DATETIME"
    ),
    "duckdb_connection" = c(
      checksum = "char(32)",
      from_ts  = "TIMESTAMP",
      until_ts = "TIMESTAMP"
    ),
    "Oracle_JDBC" = c(
      checksum = "CHAR(32)",
      from_ts  = "TIMESTAMP",
      until_ts = "TIMESTAMP"
    )
  )

  checkmate::assert_choice(class(conn), names(backend_coltypes))

  # Replace elements in signature with backend-specific types if names match
  special_cols <- backend_coltypes[[class(conn)]]
  if (identical(colnames(.data)[(length(.data) - 2):length(.data)], names(special_cols))) {
    signature[(length(.data) - 2):length(.data)] <- special_cols
  }

  return(unlist(signature))
})

methods::setMethod("getTableSignature", "NULL", function(.data, conn) {
  # Emulate product of DBI::dbDataType
  signature <- purrr::map(.data, ~ purrr::pluck(., class, 1))
  stats::setNames(signature, colnames(.data))

  # Update columns with indices instead of names to avoid conflicts
  special_cols <- c(
    checksum = "character",
    from_ts  = "POSIXct",
    until_ts = "POSIXct"
  )

  # Replace elements in signature with backend-specific types if names match
  if (identical(colnames(.data)[(length(.data) - 2):length(.data)], names(special_cols))) {
    signature[(length(.data) - 2):length(.data)] <- special_cols
  }

  return(unlist(signature))
})
