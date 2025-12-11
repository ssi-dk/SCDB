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
methods::setGeneric(
  "getTableSignature",                                                                                                  # nolint: object_name_linter
  function(.data, conn = NULL) standardGeneric("getTableSignature"),
  signature = "conn"
)

#' @importClassesFrom DBI DBIConnection
methods::setMethod("getTableSignature", "DBIConnection", function(.data, conn) {

  # Retrieve the translated data types
  signature <- as.list(DBI::dbDataType(conn, dplyr::collect(utils::head(.data, 0))))

  # Early return if there is no possibility of our special columns existing
  if (length(.data) < 3) {
    return(unlist(signature))
  }

  # Define the column types to be updated based on backend class
  backend_coltypes <- list(
    "SQLiteConnection" = list(
      checksum = "TEXT",
      from_ts  = "TIMESTAMP",
      until_ts = "TIMESTAMP"
    ),
    "PqConnection" = list(
      checksum = "CHAR(32)"
    ),
    "Microsoft SQL Server" = list(
      checksum = "CHAR(64)"
    ),
    "duckdb_connection" = list(
      checksum = "char(32)"
    )
  )

  checkmate::assert_choice(class(conn), names(backend_coltypes))


  # Determine the signature of the special columns
  special_cols_signature <- utils::modifyList(
    list(
      "checksum" = "", # Placeholder to set element as first in list
      "from_ts" = DBI::dbDataType(conn, as.POSIXct(character(0))),
      "until_ts" = DBI::dbDataType(conn, as.POSIXct(character(0)))
    ),
    backend_coltypes[[class(conn)]]
  )


  # Use the signatures of the special columns
  signature <- c(
    utils::head(signature, -3), # Our special columns must be in the last three
    utils::head(utils::modifyList(utils::tail(signature, 3), special_cols_signature), 3)
  )

  return(unlist(signature))
})

methods::setMethod("getTableSignature", "NULL", function(.data, conn) {
  # Emulate product of DBI::dbDataType
  signature <- purrr::map(.data, ~ purrr::pluck(., class, 1))
  stats::setNames(signature, colnames(.data))

  # Update columns with indices instead of names to avoid conflicts
  special_cols <- list(
    checksum = "character",
    from_ts  = "POSIXct",
    until_ts = "POSIXct"
  )

  # Use the signatures of the special columns
  signature <- c(
    utils::head(signature, -3), # Our special columns must be in the last three
    utils::head(utils::modifyList(utils::tail(signature, 3), special_cols), 3)
  )

  return(unlist(signature))
})
