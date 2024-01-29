test_that("create_table() refuses a historical table", {
  expect_error(
    cars |>
      dplyr::mutate(from_ts = NA) |>
      create_table(db_table_id = "fail.cars"),
    r"{checksum/from_ts/until_ts column\(s\) already exist\(s\) in .data!}"
  )
})


test_that("create_table() can create temporary tables", {
  for (conn in get_test_conns()) {

    table <- create_table(cars, db_table_id = "cars", conn = conn, temporary = TRUE)

    expect_identical(colnames(table), c(colnames(cars), "checksum", "from_ts", "until_ts"))
    expect_identical(
      dplyr::collect(dplyr::select(table, -tidyselect::all_of(c("checksum", "from_ts", "until_ts")))),
      dplyr::collect(dplyr::copy_to(conn, cars, "cars_ref"))
    )

    DBI::dbDisconnect(conn)
  }
})


test_that("create_table() can create tables in default schema", {
  for (conn in get_test_conns()) {

    table <- create_table(cars, db_table_id = "cars", conn = conn, temporary = FALSE)

    expect_identical(colnames(table), c(colnames(cars), "checksum", "from_ts", "until_ts"))
    expect_identical(
      dplyr::collect(dplyr::select(table, -tidyselect::all_of(c("checksum", "from_ts", "until_ts")))),
      dplyr::collect(dplyr::copy_to(conn, cars, "cars_ref"))
    )


    DBI::dbRemoveTable(conn, id(table))
    DBI::dbDisconnect(conn)
  }
})


test_that("create_table() can create tables in non default schema", {
  for (conn in get_test_conns()) {

    table <- create_table(cars, db_table_id = id("test.cars", conn), conn = conn, temporary = FALSE)

    expect_identical(colnames(table), c(colnames(cars), "checksum", "from_ts", "until_ts"))
    expect_identical(
      dplyr::collect(dplyr::select(table, -tidyselect::all_of(c("checksum", "from_ts", "until_ts")))),
      dplyr::collect(dplyr::copy_to(conn, cars, "cars_ref"))
    )

    DBI::dbRemoveTable(conn, id("test.cars", conn))
    DBI::dbDisconnect(conn)
  }
})


test_that("create_table() works with no conn", {
  table <- create_table(cars, db_table_id = "cars", conn = NULL)

  expect_identical(colnames(table), c(colnames(cars), "checksum", "from_ts", "until_ts"))
  expect_identical(
    dplyr::select(table, -tidyselect::all_of(c("checksum", "from_ts", "until_ts"))),
    cars
  )
})


test_that("create_table() does not overwrite tables", {
  for (conn in get_test_conns()) {

    table <- create_table(utils::head(cars, 10), db_table_id = "cars", conn = conn, temporary = TRUE)

    expect_error(
      create_table(utils::head(cars, 20), db_table_id = "cars", conn = conn, temporary = TRUE),
      'Table (<Id> )?["`]#?cars["`] exists in database'
    )

    expect_equal(nrow(table), 10)

    DBI::dbDisconnect(conn)
  }
})


test_that("getTableSignature() generates a signature for NULL connections", {
  expect_identical(
    lapply(cars, class),
    as.list(getTableSignature(cars, conn = NULL))
  )
})


test_that("create_logs_if_missing() can create logs in default and test schema", {
  for (conn in get_test_conns()) {
    for (schema in list(NULL, "test")) {

      # Generate table in schema that does not exist
      k <- 0
      while (TRUE && k < 100) {
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
