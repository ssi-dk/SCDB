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
        expect_no_error(create_logs_if_missing(log_table = logs_id,  conn))

        # And check it conforms with the requirements
        expect_true(table_exists(conn, logs_id))
        expect_true(nrow(dplyr::tbl(conn, id(logs_id, conn))) == 0)

        log_signature <- data.frame(
          date = as.POSIXct(NA),
          schema = NA_character_,
          table = NA_character_,
          n_insertions = NA_integer_,
          n_deactivations = NA_integer_,
          start_time = as.POSIXct(NA),
          end_time = as.POSIXct(NA),
          duration = NA_character_,
          success = NA,
          message = NA_character_,
          log_file = NA_character_
        ) |>
          dplyr::copy_to(conn, df = _, "SCDB_tmp", overwrite = TRUE) |>
          utils::head(0) |>
          dplyr::collect()

        expect_identical(
          dplyr::collect(dplyr::tbl(conn, id(logs_id, conn))),
          log_signature
        )

        # Attempting to recreate the logs table should not change anything
        expect_no_error(create_logs_if_missing(log_table = logs_id,  conn))
        expect_true(table_exists(conn, logs_id))
        expect_true(nrow(dplyr::tbl(conn, id(logs_id, conn))) == 0)

      } else {
        warning("Non-existing table in default schema could not be generated!")
      }

    }
    DBI::dbDisconnect(conn)
  }
})
