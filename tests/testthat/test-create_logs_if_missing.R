test_that("create_logs_if_missing() can create logs in default and test schema", {
  for (conn in get_test_conns()) {
    for (schema in list(NULL, "test")) {

      # Generate table in schema that does not exist
      k <- 0
      while (k < 100) {
        logs_id <- paste(c(schema, paste(sample(letters, size = 16, replace = TRUE), collapse = "")), collapse = ".")
        k <- k + 1
        if (DBI::dbExistsTable(conn, id(logs_id, conn))) next
        break
      }

      if (k < 100) {

        # We know table does not exists
        expect_false(table_exists(conn, logs_id))

        # We create the missing log table
        expect_no_error(create_logs_if_missing(conn, log_table = logs_id))

        # And check it conforms with the requirements
        expect_true(table_exists(conn, logs_id))
        expect_identical(nrow(dplyr::tbl(conn, id(logs_id, conn))), 0)

        log_signature <- data.frame(
          date = as.POSIXct(character(0)),
          catalog = character(0),
          schema = character(0),
          table = character(0),
          n_insertions = integer(0),
          n_deactivations = integer(0),
          start_time = as.POSIXct(character(0)),
          end_time = as.POSIXct(character(0)),
          duration = character(0),
          success = logical(),
          message = character(0),
          log_file = character(0)
        )

        if (!checkmate::test_multi_class(conn, c("Microsoft SQL Server", "duckdb_connection"))) {
          log_signature <- dplyr::select(log_signature, !"catalog")
        }

        log_signature <- log_signature |>
          dplyr::copy_to(conn, df = _, unique_table_name()) |>
          dplyr::collect()

        expect_identical(
          dplyr::collect(dplyr::tbl(conn, id(logs_id, conn))),
          log_signature
        )

        # Attempting to recreate the logs table should not change anything
        expect_no_error(create_logs_if_missing(conn, log_table = logs_id))
        expect_true(table_exists(conn, logs_id))
        expect_identical(nrow(dplyr::tbl(conn, id(logs_id, conn))), 0)

      } else {
        warning("Non-existing table in default schema could not be generated!")
      }

    }
    connection_clean_up(conn)
  }
})
