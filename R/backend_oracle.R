# dbplyr needs additional implementation for Oracle to work.

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
