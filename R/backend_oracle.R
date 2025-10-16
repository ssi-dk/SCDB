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
