# Ensure the target connections are empty and configured correctly
coll <- checkmate::makeAssertCollection()
conns <- get_test_conns()
for (conn_id in seq_along(conns)) {

  conn <- conns[[conn_id]]

  # Ensure connections are valid
  if (is.null(conn) || !DBI::dbIsValid(conn)) {
    coll$push(glue::glue("Connection could not be made to backend ({names(conns)[[conn_id]]})."))
  }


  # Check schemas are configured correctly
  if (!schema_exists(conn, "test") && names(conns)[conn_id] != "SQLite") {
    coll$push(glue::glue("Tests require the schema 'test' to exist in connection ({names(conns)[[conn_id]]})."))
  }

  if (!schema_exists(conn, "test.one") && names(conns)[conn_id] != "SQLite") {
    coll$push(glue::glue("Tests require the schema 'test.one' to exist in connection ({names(conns)[[conn_id]]})."))
  }

  DBI::dbDisconnect(conn)
}
checkmate::reportAssertions(coll)


# Configure the data bases
for (conn in get_test_conns()) {

  # Start with some clean up
  purrr::walk(
    c("test.mtcars", "__mtcars", "__mtcars_historical", "test.mtcars_modified", "mtcars_modified",
      "test.SCDB_logs", "test.SCDB_logger", "test.SCDB_tmp1", "test.SCDB_tmp2", "test.SCDB_tmp3",
      "test.SCDB_t0", "test.SCDB_t1", "test.SCDB_t2"
    ),
    ~ if (DBI::dbExistsTable(conn, id(., conn))) DBI::dbRemoveTable(conn, id(., conn))
  )

  purrr::walk(
    c(DBI::Id(schema = "test", table = "one.two"), DBI::Id(schema = "test.one", table = "two")),
    ~ if (schema_exists(conn, .@name[["schema"]]) && DBI::dbExistsTable(conn, .)) DBI::dbRemoveTable(conn, .)
  )

  # Copy mtcars to conn
  dplyr::copy_to(
    conn, mtcars %>% dplyr::mutate(name = rownames(mtcars)),
    name = id("test.mtcars", conn),
    temporary = FALSE,
    overwrite = TRUE,
    analyze = FALSE
  )

  dplyr::copy_to(
    conn, mtcars %>% dplyr::mutate(name = rownames(mtcars)),
    name = id("__mtcars", conn),
    temporary = FALSE,
    overwrite = TRUE,
    analyze = FALSE
  )

  dplyr::copy_to(
    conn,
    mtcars %>%
      dplyr::mutate(name = rownames(mtcars)) %>%
      digest_to_checksum() %>%
      dplyr::mutate(
        from_ts = as.POSIXct("2020-01-01 09:00:00"),
        until_ts = as.POSIXct(NA)
      ),
    name = id("__mtcars_historical", conn),
    temporary = FALSE,
    overwrite = TRUE,
    analyze = FALSE
  )

  DBI::dbDisconnect(conn)
}


#' Clean up and test function
#' @description
#'   This function checks for the existence of "dbplyr_###" tables on the connection before closing the connection
#' @template conn
#' @return NULL (called for side effects)
#' @import rlang .data
#' @importFrom magrittr %>%
#' @noRd
connection_clean_up <- function(conn) {
  dbplyr_tables <- get_tables(conn, show_temporary = TRUE) %>%
    dplyr::pull("table") %>%
    purrr::keep(~ stringr::str_detect(., "^#?dbplyr_"))

  if (length(dbplyr_tables) > 0) {
    warning("Temporary dbplyr tables ('dbplyr_###') are not cleaned up!", call. = FALSE)
  }
  DBI::dbDisconnect(conn)
}
