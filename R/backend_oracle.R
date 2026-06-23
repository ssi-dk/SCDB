#' @exportS3Method dbplyr::sql_dialect
sql_dialect.JDBCConnection <- function(con) {
  dialect <- dbplyr::sql_dialect(dbplyr::simulate_oracle())
  class(dialect) <- c("sql_dialect_scdb_oracle_jdbc", class(dialect))
  return(dialect)
}

#' @exportS3Method dbplyr::sql_table_analyze
sql_table_analyze.sql_dialect_scdb_oracle_jdbc <- function(con, table, ...) {
  NULL
}

oracle_jdbc_table_exists <- function(conn, name) {
  db_table_id <- id(name, conn)

  schema_name <- purrr::pluck(db_table_id, "name", "schema")
  table_name <- purrr::pluck(db_table_id, "name", "table")

  if (is.null(schema_name) || is.na(schema_name)) {
    schema_name <- get_schema(conn)
  }

  schema_candidates <- unique(c(schema_name, toupper(schema_name)))
  table_candidates <- unique(c(table_name, toupper(table_name)))

  quote_chr <- function(x) {
    paste(as.character(DBI::dbQuoteString(conn, x)), collapse = ", ")
  }

  query <- paste(
    "SELECT COUNT(*) AS n",
    "FROM all_objects",
    "WHERE object_type IN ('TABLE', 'VIEW')",
    "AND owner IN (", quote_chr(schema_candidates), ")",
    "AND object_name IN (", quote_chr(table_candidates), ")"
  )

  result <- DBI::dbGetQuery(conn, query)

  return(as.integer(result[[1]][[1]]) > 0L)
}


#' @importFrom DBI dbExistsTable
#' @export
methods::setMethod(
  "dbExistsTable",
  signature(conn = "JDBCConnection", name = "Id"),
  function(conn, name, ...) {
    oracle_jdbc_table_exists(conn, name)
  }
)




# Oracle/JDBC table writing ------------------------------------------------

oracle_jdbc_quote_identifier <- function(x) {
  x <- as.character(x)
  paste0('"', gsub('"', '""', x, fixed = TRUE), '"')
}

oracle_jdbc_table_sql <- function(conn, name) {
  if (inherits(name, "SQL")) {
    return(as.character(name))
  }

  db_table_id <- id(name, conn)

  catalog <- purrr::pluck(db_table_id, "name", "catalog")
  schema <- purrr::pluck(db_table_id, "name", "schema")
  table <- purrr::pluck(db_table_id, "name", "table")

  if (!is.null(catalog) && !is.na(catalog)) {
    rlang::abort("Oracle/JDBC table identifiers cannot include a catalog.")
  }

  paste(
    purrr::map_chr(
      purrr::discard(list(schema, table), is.null),
      oracle_jdbc_quote_identifier
    ),
    collapse = "."
  )
}

oracle_jdbc_column_sql <- function(columns) {
  paste(
    purrr::map_chr(columns, oracle_jdbc_quote_identifier),
    collapse = ", "
  )
}

oracle_jdbc_field_sql <- function(fields) {
  if (inherits(fields, "data.frame")) {
    rlang::abort("Internal error: fields must be resolved before oracle_jdbc_field_sql().")
  }

  fields <- unlist(fields)

  if (is.null(names(fields)) || any(names(fields) == "")) {
    rlang::abort("Oracle/JDBC fields must be a named character vector.")
  }

  if (anyDuplicated(names(fields))) {
    rlang::abort("Oracle/JDBC fields must have unique names.")
  }

  paste(
    paste(
      purrr::map_chr(names(fields), oracle_jdbc_quote_identifier),
      unname(fields)
    ),
    collapse = ", "
  )
}

oracle_jdbc_insert_values <- function(value) {
  value <- as.data.frame(value)

  # Match RJDBC's dbWriteTable() behaviour: keep numeric values as-is and
  # coerce other columns to character before passing to JDBC prepared statements.
  value[] <- purrr::map(
    value,
    function(column) {
      if (is.numeric(column)) {
        column
      } else {
        as.character(column)
      }
    }
  )

  value
}

oracle_jdbc_send_update <- function(conn, statement, value = NULL, max.batch = 10000L) {
  dbSendUpdate <- getExportedValue("RJDBC", "dbSendUpdate")

  if (is.null(value)) {
    dbSendUpdate(conn, statement)
  } else {
    dbSendUpdate(
      conn,
      statement,
      list = unname(as.list(value)),
      max.batch = max.batch
    )
  }
}

#' @importFrom DBI dbWriteTable
#' @export
methods::setMethod(
  "dbWriteTable",
  signature(conn = "JDBCConnection"),
  function(
    conn,
    name,
    value,
    overwrite = FALSE,
    append = FALSE,
    ...,
    row.names = FALSE,
    field.types = NULL,
    temporary = FALSE,
    max.batch = 10000L
  ) {
    overwrite <- isTRUE(overwrite)
    append <- isTRUE(append)

    if (overwrite && append) {
      rlang::abort("overwrite = TRUE and append = TRUE are mutually exclusive.")
    }

    value <- as.data.frame(value)

    if (!identical(row.names, FALSE) && !is.null(row.names)) {
      value <- DBI::sqlRownamesToColumn(value, row.names)
    }

    db_table_id <- id(name, conn)

    exists <- DBI::dbExistsTable(conn, db_table_id)

    if (exists && overwrite) {
      DBI::dbRemoveTable(conn, db_table_id)
      exists <- FALSE
    }

    if (exists && !append) {
      rlang::abort(paste0("Table ", as.character(db_table_id), " already exists."))
    }

    if (!exists) {
      fields <- DBI::dbDataType(conn, value)

      if (!is.null(field.types)) {
        fields[names(field.types)] <- field.types
      }

      DBI::dbCreateTable(
        conn = conn,
        name = db_table_id,
        fields = fields,
        temporary = temporary
      )
    }

    DBI::dbAppendTable(
      conn = conn,
      name = db_table_id,
      value = value,
      max.batch = max.batch
    )

    invisible(TRUE)
  }
)
