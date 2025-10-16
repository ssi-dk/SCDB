# dbplyr needs additional implementation for Oracle to work.

#' @importClassesFrom RJDBC JDBCConnection
#' @export
#' @noRd
methods::setClass("Oracle", contains = "JDBCConnection")



#' @importFrom DBI dbExistsTable
NULL

#' @exportMethod dbExistsTable
setMethod("dbExistsTable", signature("JDBCConnection", "Id"),
  function(conn, name, ...) {
    methods::callNextMethod() # Remove ambiguity
  }
)

#' @importFrom DBI dbGetRowsAffected
NULL

#' @importFrom rJava .jcall
#' @importFrom methods setMethod
#' @exportMethod dbGetRowsAffected
#' @noRd
setMethod("dbGetRowsAffected", "JDBCResult", function(res, ...) {
  if (!is.null(res@stat)) {
    tryCatch({
      cnt <- rJava::.jcall(res@stat, "I", "getUpdateCount")
      return(if (cnt < 0) 0L else as.integer(cnt))
    }, error = function(e) {
      return(NA_integer_)
    })
  }
  return(NA_integer_)
})


#' @importFrom DBI dbQuoteIdentifier
NULL

#' @exportMethod dbQuoteIdentifier
#' @noRd
setMethod("dbQuoteIdentifier", signature("JDBCConnection", "character"),
  function(conn, x, ...) {
    x <- enc2utf8(x)

    reserved_words <- c("DATE", "NUMBER", "VARCHAR")

    needs_escape <- (grepl("^[a-zA-Z_]", x) & toupper(x) != x) |  tolower(x) %in% reserved_words

    x[needs_escape] <- paste0("\"", gsub("\"", "\"\"", x[needs_escape]), "\"")

    return(DBI::SQL(x, names = names(x)))
  }
)

#' @exportMethod dbQuoteIdentifier
#' @noRd
setMethod("dbQuoteIdentifier", signature("JDBCConnection", "SQL"),
  function(conn, x, ...) {
    return(x) # Remove ambiguity (also assume already quoted)
  }
)

#' @exportMethod dbQuoteIdentifier
#' @noRd
setMethod("dbQuoteIdentifier", signature("Oracle", "Id"),
  function(conn, x, ...) {

    # For `Id`, run on each non-NA element
      return(DBI::SQL(paste0(DBI::dbQuoteIdentifier(conn, purrr::discard(x@name, is.na)), collapse = ".")))
  }
)

#' @importFrom DBI dbCreateTable
NULL

#' @exportMethod dbCreateTable
#' @noRd
methods::setMethod("dbCreateTable", methods::signature("Oracle", "Id"),
  function(conn, name, fields, ..., row.names = NULL, temporary = FALSE) {
    stopifnot(is.null(row.names))
    stopifnot(is.logical(temporary), length(temporary) == 1L)
    query <- DBI::sqlCreateTable(con = conn, table = name, fields = fields,
        row.names = row.names, temporary = temporary, ...)
    print(query)
    DBI::dbExecute(conn, query)
    invisible(TRUE)
  }
)

#' @importFrom DBI dbWriteTable
NULL

#' @importFrom methods setMethod
#' @exportMethod dbWriteTable
#' @noRd
setMethod("dbWriteTable", signature("JDBCConnection", "SQL", "data.frame"),
  function(conn, name, value, ...) {

    method <- getMethod(dbWriteTable, signature(conn = "JDBCConnection", name = "ANY", value = "ANY"))


    # Manually quote column names
    names(value) <- as.character(DBI::dbQuoteIdentifier(conn, names(value)))

    method@.Data(conn, name, value, ...)

  }
)
