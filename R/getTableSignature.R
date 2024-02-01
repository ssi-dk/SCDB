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
