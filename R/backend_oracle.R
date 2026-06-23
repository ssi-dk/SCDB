# dbplyr needs additional implementation for Oracle to work.

#' @importClassesFrom RJDBC JDBCConnection
#' @importClassesFrom odbc Oracle
setClass(
  "OracleJDBCConnection",
  contains = "JDBCConnection"
)
