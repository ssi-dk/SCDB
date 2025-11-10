# Configure the data bases
for (conn in get_test_conns()) {

  print("## Debug flag 1")

  # Debug
  purrr::map(
    c("test.mtcars", "__mtcars", "__mtcars_historical", "test.mtcars_modified", "mtcars_modified",
      "test.SCDB_logs", "test.SCDB_logger", "test.SCDB_tmp1", "test.SCDB_tmp2", "test.SCDB_tmp3",
      "test.SCDB_t0", "test.SCDB_t1", "test.SCDB_t2"
    ),
    ~ id(., conn)
  )

  # Start with some clean up
  purrr::walk(
    c("test.mtcars", "__mtcars", "__mtcars_historical", "test.mtcars_modified", "mtcars_modified",
      "test.SCDB_logs", "test.SCDB_logger", "test.SCDB_tmp1", "test.SCDB_tmp2", "test.SCDB_tmp3",
      "test.SCDB_t0", "test.SCDB_t1", "test.SCDB_t2"
    ),
    ~ if (DBI::dbExistsTable(conn, id(., conn))) DBI::dbRemoveTable(conn, id(., conn))
  )

  print("## Debug flag 2")
  purrr::walk(
    c(DBI::Id(schema = "test", table = "one.two"), DBI::Id(schema = "test.one", table = "two")),
    ~ if (schema_exists(conn, .@name[["schema"]]) && DBI::dbExistsTable(conn, .)) DBI::dbRemoveTable(conn, .)
  )

  print("## Debug flag 3")
  # Copy mtcars to conn
  dplyr::copy_to(
    conn, mtcars %>% dplyr::mutate(name = rownames(mtcars)),
    name = id("test.mtcars", conn),
    temporary = FALSE,
    overwrite = TRUE,
    analyze = FALSE
  )

  print("## Debug flag 4")
  dplyr::copy_to(
    conn, mtcars %>% dplyr::mutate(name = rownames(mtcars)),
    name = id("__mtcars", conn),
    temporary = FALSE,
    overwrite = TRUE,
    analyze = FALSE
  )

  print("## Debug flag 5")
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

  print("## Debug flag 6")
  dplyr::copy_to(
    conn,
    mtcars %>%
      dplyr::mutate(name = rownames(mtcars)) %>%
      digest_to_checksum() %>%
      dplyr::mutate(
        from_ts = as.POSIXct("2020-01-01 09:00:00"),
        until_ts = as.POSIXct(NA)
      ),
    name = id("MTCARS", conn),
    temporary = FALSE,
    overwrite = TRUE,
    analyze = FALSE
  )
  print("## Debug flag 7")

  DBI::dbDisconnect(conn)
  print("## Debug flag 8")
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
