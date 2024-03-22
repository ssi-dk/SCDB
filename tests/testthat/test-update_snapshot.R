test_that("update_snapshot() can handle first snapshot", {
  for (conn in get_test_conns()) {

    if (DBI::dbExistsTable(conn, id("test.SCDB_tmp1", conn))) DBI::dbRemoveTable(conn, id("test.SCDB_tmp1", conn))
    if (DBI::dbExistsTable(conn, id("test.SCDB_logs", conn))) DBI::dbRemoveTable(conn, id("test.SCDB_logs", conn))
    expect_false(table_exists(conn, "test.SCDB_tmp1"))
    expect_false(table_exists(conn, "test.SCDB_logs"))

    target <- mtcars |>
      dplyr::copy_to(conn, df = _, name = unique_table_name()) |>
      digest_to_checksum(col = "checksum") |>
      dplyr::mutate(from_ts  = !!db_timestamp("2022-10-01 09:00:00", conn),
                    until_ts = !!db_timestamp(NA, conn))

    # Copy target to conn
    target <- dplyr::copy_to(conn, target, name = id("test.SCDB_tmp1", conn), overwrite = TRUE, temporary = FALSE)

    close_connection(conn)
  }
})


test_that("update_snapshot() can add a new snapshot", {
  for (conn in get_test_conns()) {

    # Modify snapshot and run update step
    .data <- mtcars |>
      dplyr::mutate(hp = dplyr::if_else(hp > 130, hp - 10, hp)) |>
      dplyr::copy_to(conn, df = _, name = unique_table_name())

    # This is a simple update where 23 rows are replaced with 23 new ones on the given date
    db_table <- "test.SCDB_tmp1"
    timestamp <- "2022-10-03 09:00:00"
    log_path <- tempdir()

    logger <- Logger$new(
      db_table = db_table,
      timestamp = timestamp,
      log_path = log_path,
      log_table_id = "test.SCDB_logs",
      log_conn = conn,
      output_to_console = FALSE
    )

    # Ensure all logs are removed before starting
    dir(log_path) |>
      purrr::keep(~ endsWith(., ".log")) |>
      purrr::walk(~ unlink(file.path(log_path, .)))


    # Update
    update_snapshot(.data, conn, db_table, timestamp, logger = logger)

    # Check the snapshot has updated correctly
    target <- dplyr::tbl(conn, id(db_table, conn))
    expect_identical(
      slice_time(target, "2022-10-01 09:00:00") |>
        dplyr::select(!c("from_ts", "until_ts", "checksum")) |>
        dplyr::collect() |>
        dplyr::arrange(wt, qsec),
      mtcars |>
        dplyr::arrange(wt, qsec) |>
        tibble::as_tibble()
    )
    expect_equal(
      nrow(slice_time(target, "2022-10-01 09:00:00")),
      nrow(mtcars)
    )

    expect_identical(
      slice_time(target, "2022-10-03 09:00:00") |>
        dplyr::select(!c("from_ts", "until_ts", "checksum")) |>
        dplyr::collect() |>
        dplyr::arrange(wt, qsec),
      .data |>
        dplyr::collect() |>
        dplyr::arrange(wt, qsec)
    )
    expect_equal(
      nrow(slice_time(target, "2022-10-03 09:00:00")),
      nrow(mtcars)
    )

    # Check file log outputs exists
    log_pattern <- glue::glue("{stringr::str_replace_all(as.Date(timestamp), '-', '_')}.{id(db_table, conn)}.log")
    log_file <- purrr::keep(dir(log_path), ~stringr::str_detect(., log_pattern))
    expect_length(log_file, 1)
    expect_gt(file.info(file.path(log_path, log_file))$size, 0)
    expect_identical(nrow(get_table(conn, "test.SCDB_logs")), 1)

    db_logs_with_log_file <- get_table(conn, "test.SCDB_logs") |>
      dplyr::filter(!is.na(.data$log_file))
    expect_identical(nrow(db_logs_with_log_file), 1)

    # Check database log output
    logs <- get_table(conn, "test.SCDB_logs") |> dplyr::collect()

    # The logs should have specified data types
    types <- c(
      "date" = "POSIXct",
      "catalog" = "character",
      "schema" = "character",
      "table" = "character",
      "n_insertions" = "numeric",
      "n_deactivations" = "numeric",
      "start_time" = "POSIXct",
      "end_time" = "POSIXct",
      "duration" = "character",
      "success" = "logical",
      "message" = "character"
    )

    if (inherits(conn, "SQLiteConnection")) {
      types <- types |>
        purrr::map_if(~ identical(., "POSIXct"), "character") |> # SQLite does not support POSIXct
        purrr::map_if(~ identical(., "logical"), "numneric") |>  # SQLite does not support logical
        as.character()
    }

    checkmate::expect_data_frame(logs, nrows = 1, types)

    close_connection(conn)
  }
})


test_that("update_snapshot() can update a snapshot on an existing date", {
  for (conn in get_test_conns()) {

    # We now attempt to do another update on the same date
    .data <- mtcars |>
      dplyr::mutate(hp = dplyr::if_else(hp > 100, hp - 10, hp)) |>
      dplyr::copy_to(conn, df = _, name = unique_table_name())

    update_snapshot(.data, conn, "test.SCDB_tmp1", "2022-10-03 09:00:00", logger = LoggerNull$new())

    # Even though we insert twice on the same date, we expect the data to be minimal (compacted)
    target <- dplyr::tbl(conn, id("test.SCDB_tmp1", conn))
    expect_identical(
      slice_time(target, "2022-10-01 09:00:00") |>
        dplyr::select(!c("from_ts", "until_ts", "checksum")) |>
        dplyr::collect() |>
        dplyr::arrange(wt, qsec),
      mtcars |>
        dplyr::arrange(wt, qsec) |>
        tibble::tibble()
    )
    expect_equal(
      nrow(slice_time(target, "2022-10-01 09:00:00")),
      nrow(mtcars)
    )

    expect_identical(
      slice_time(target, "2022-10-03 09:00:00") |>
        dplyr::select(!c("from_ts", "until_ts", "checksum")) |>
        dplyr::collect() |>
        dplyr::arrange(wt, qsec),
      .data |>
        dplyr::collect() |>
        dplyr::arrange(wt, qsec)
    )
    expect_equal(
      nrow(slice_time(target, "2022-10-03 09:00:00")),
      nrow(mtcars)
    )


    close_connection(conn)
  }
})


test_that("update_snapshot() can insert a snapshot between existing dates", {
  for (conn in get_test_conns()) {

    # We now attempt to an update between these two updates
    .data <- mtcars |>
      dplyr::mutate(hp = dplyr::if_else(hp > 150, hp - 10, hp)) |>
      dplyr::copy_to(conn, df = _, name = unique_table_name())

    # This should fail if we do not specify "enforce_chronological_order = FALSE"
    expect_error(
      update_snapshot(.data, conn, "test.SCDB_tmp1", "2022-10-02 09:00:00", logger = LoggerNull$new()),
      regexp = "Given timestamp 2022-10-02 09:00:00 is earlier"
    )

    # But not with the variable set
    update_snapshot(.data, conn, "test.SCDB_tmp1", "2022-10-02 09:00:00",
                    logger = LoggerNull$new(), enforce_chronological_order = FALSE)


    target <- dplyr::tbl(conn, id("test.SCDB_tmp1", conn))
    expect_identical(
      slice_time(target, "2022-10-01 09:00:00") |>
        dplyr::select(!c("from_ts", "until_ts", "checksum")) |>
        dplyr::collect() |>
        dplyr::arrange(wt, qsec),
      mtcars |>
        dplyr::arrange(wt, qsec) |>
        tibble::tibble()
    )
    expect_equal(
      nrow(slice_time(target, "2022-10-01 09:00:00")),
      nrow(mtcars)
    )

    expect_identical(
      slice_time(target, "2022-10-02 09:00:00") |>
        dplyr::select(!c("from_ts", "until_ts", "checksum")) |>
        dplyr::collect() |>
        dplyr::arrange(wt, qsec),
      .data |>
        dplyr::collect() |>
        dplyr::arrange(wt, qsec)
    )
    expect_equal(
      nrow(slice_time(target, "2022-10-02 09:00:00")),
      nrow(mtcars)
    )

    close_connection(conn)
  }
})


test_that("update_snapshot() works (holistic test 1)", {
  for (conn in get_test_conns()) {

    if (DBI::dbExistsTable(conn, id("test.SCDB_tmp1", conn))) DBI::dbRemoveTable(conn, id("test.SCDB_tmp1", conn))
    expect_false(table_exists(conn, "test.SCDB_tmp1"))


    # Create test data for the test
    t0 <- data.frame(col1 = c("A", "B"),      col2 = c(NA_real_, NA_real_))
    t1 <- data.frame(col1 = c("A", "B", "C"), col2 = c(1,        NA_real_, NA_real_))
    t2 <- data.frame(col1 = c("A", "B", "C"), col2 = c(1,        2,        3))

    # Copy t0, t1, and t2 to conn
    t0 <- dplyr::copy_to(conn, t0, name = id("test.SCDB_t0", conn), overwrite = TRUE, temporary = FALSE)
    t1 <- dplyr::copy_to(conn, t1, name = id("test.SCDB_t1", conn), overwrite = TRUE, temporary = FALSE)
    t2 <- dplyr::copy_to(conn, t2, name = id("test.SCDB_t2", conn), overwrite = TRUE, temporary = FALSE)

    logger <- LoggerNull$new()
    update_snapshot(t0, conn, "test.SCDB_tmp1", "2022-01-01", logger = logger)
    expect_identical(dplyr::collect(t0) |> dplyr::arrange(col1),
                     dplyr::collect(get_table(conn, "test.SCDB_tmp1")) |> dplyr::arrange(col1))

    update_snapshot(t1, conn, "test.SCDB_tmp1", "2022-02-01", logger = logger)
    expect_identical(dplyr::collect(t1) |> dplyr::arrange(col1),
                     dplyr::collect(get_table(conn, "test.SCDB_tmp1")) |> dplyr::arrange(col1))

    update_snapshot(t2, conn, "test.SCDB_tmp1", "2022-02-01", logger = logger)
    expect_identical(dplyr::collect(t2) |> dplyr::arrange(col1),
                     dplyr::collect(get_table(conn, "test.SCDB_tmp1")) |> dplyr::arrange(col1))

    t <- list(t0, t1, t2) |>
      purrr::reduce(dplyr::union) |>
      dplyr::collect() |>
      dplyr::mutate(col2 = as.character(col2)) |>
      dplyr::arrange(col1, col2) |>
      utils::head(5)

    t_ref <- get_table(conn, "test.SCDB_tmp1", slice_ts = NULL) |>
      dplyr::select(!any_of(c("from_ts", "until_ts", "checksum"))) |>
      dplyr::collect() |>
      dplyr::mutate(col2 = as.character(col2)) |>
      dplyr::arrange(col1, col2)

    expect_identical(t, t_ref)

    close_connection(conn)
  }
})



test_that("update_snapshot() works (holistic test 2)", {
  for (conn in get_test_conns()) {

    if (DBI::dbExistsTable(conn, id("test.SCDB_tmp1", conn))) DBI::dbRemoveTable(conn, id("test.SCDB_tmp1", conn))
    expect_false(table_exists(conn, "test.SCDB_tmp1"))


    # Create test data for the test
    t0 <- data.frame(col1 = c("A", "B"),      col2 = c(NA_real_, NA_real_))
    t1 <- data.frame(col1 = c("A", "B", "C"), col2 = c(1,        NA_real_, NA_real_))
    t2 <- data.frame(col1 = c("A", "B", "C"), col2 = c(1,        2,        3))

    # Copy t0, t1, and t2 to conn (and suppress check_from message)
    t0 <- dplyr::copy_to(conn, t0, name = id("test.SCDB_t0", conn), overwrite = TRUE, temporary = FALSE)
    t1 <- dplyr::copy_to(conn, t1, name = id("test.SCDB_t1", conn), overwrite = TRUE, temporary = FALSE)
    t2 <- dplyr::copy_to(conn, t2, name = id("test.SCDB_t2", conn), overwrite = TRUE, temporary = FALSE)


    # Check non-chronological insertion
    logger <- LoggerNull$new()
    update_snapshot(t0, conn, "test.SCDB_tmp1", "2022-01-01", logger = logger)
    expect_identical(dplyr::collect(t0) |> dplyr::arrange(col1),
                     dplyr::collect(get_table(conn, "test.SCDB_tmp1")) |> dplyr::arrange(col1))

    update_snapshot(t2, conn, "test.SCDB_tmp1", "2022-03-01", logger = logger)
    expect_identical(dplyr::collect(t2) |> dplyr::arrange(col1),
                     dplyr::collect(get_table(conn, "test.SCDB_tmp1")) |> dplyr::arrange(col1))

    update_snapshot(t1, conn, "test.SCDB_tmp1", "2022-02-01", logger = logger, enforce_chronological_order = FALSE)
    expect_identical(dplyr::collect(t1) |> dplyr::arrange(col1),
                     dplyr::collect(get_table(conn, "test.SCDB_tmp1", slice_ts = "2022-02-01")) |> dplyr::arrange(col1))

    t_ref <-
      tibble::tibble(col1     = c("A",          "B",          "A",          "C",          "B",          "C"),
                     col2     = c(NA_real_,     NA_real_,     1,            NA_real_,     2,            3),
                     from_ts  = c("2022-01-01", "2022-01-01", "2022-02-01", "2022-02-01", "2022-03-01", "2022-03-01"),
                     until_ts = c("2022-02-01", "2022-03-01", NA,           "2022-03-01", NA,           NA))

    expect_identical(
      get_table(conn, "test.SCDB_tmp1", slice_ts = NULL) |>
        dplyr::select(!"checksum") |>
        dplyr::collect() |>
        dplyr::mutate(from_ts  = strftime(from_ts),
                      until_ts = strftime(until_ts)) |>
        dplyr::arrange(col1, from_ts),
      t_ref |>
        dplyr::arrange(col1, from_ts)
    )

    if (DBI::dbExistsTable(conn, id("test.SCDB_tmp1", conn))) DBI::dbRemoveTable(conn, id("test.SCDB_tmp1", conn))

    connection_clean_up(conn)
  }
})


test_that("update_snapshot() works with Id objects", {
  withr::local_options("SCDB.log_path" = NULL) # No file logging

  for (conn in get_test_conns()) {

    target_table <- id("test.mtcars_modified", conn)

    logger <- Logger$new(output_to_console = FALSE,
                         timestamp = Sys.time(),
                         db_table = "test.mtcars_modified",
                         log_conn = NULL,
                         log_table_id = NULL,
                         warn = FALSE)

    expect_no_error(
      mtcars |>
        dplyr::mutate(disp = sample(mtcars$disp, nrow(mtcars))) |>
        dplyr::copy_to(dest = conn,
                       df = _,
                       name = unique_table_name()) |>
        update_snapshot(
          conn = conn,
          db_table = target_table,
          logger = logger,
          timestamp = format(Sys.time())
        )
    )

    connection_clean_up(conn)
  }
})


test_that("update_snapshot() checks table formats", {

  withr::local_options("SCDB.log_path" = tempdir())

  for (conn in get_test_conns()) {

    mtcars_table <- dplyr::tbl(conn, id("__mtcars_historical", conn = conn))
    timestamp <- Sys.time()

    expect_warning(
      logger <- Logger$new(log_path = NULL, log_table_id = NULL, output_to_console = FALSE),                            # nolint: implicit_assignment_linter
      "NO file or database logging will be done."
    )

    # Test columns not matching
    broken_table <- dplyr::copy_to(conn, dplyr::select(mtcars, !"mpg"), name = "mtcars_broken", overwrite = TRUE)

    expect_error(
      update_snapshot(
        .data = broken_table,
        conn = conn,
        db_table = mtcars_table,
        timestamp = timestamp,
        logger = logger
      ),
      "Columns do not match!"
    )

    file.remove(list.files(getOption("SCDB.log_path"), pattern = format(timestamp, "^%Y%m%d.%H%M"), full.names = TRUE))

    # Test target table not being a historical table
    expect_error(
      update_snapshot(
        dplyr::tbl(conn, id("__mtcars", conn = conn)),
        conn,
        id("__mtcars", conn = conn),
        timestamp = timestamp,
        logger = logger
      ),
      "Table does not seem like a historical table"
    )

    connection_clean_up(conn)
  }
})


test_that("update_snapshot() works with across connection", {
  skip_if_not_installed("RSQLite")

  withr::local_options("SCDB.log_path" = NULL) # No file logging

  # Test a data transfer from a local SQLite to the test connection
  source_conn <- DBI::dbConnect(RSQLite::SQLite())

  # Create a table for the tests
  mtcars_modified <- mtcars |>
    dplyr::mutate(name = rownames(mtcars))

  # Copy table to the source
  .data <- dplyr::copy_to(dest = source_conn, df = mtcars_modified, name = unique_table_name())

  # For each conn, we test if update_snapshot preserves data types
  for (target_conn in get_test_conns()) {

    target_table <- id("test.mtcars_modified", target_conn)
    if (DBI::dbExistsTable(target_conn, target_table)) DBI::dbRemoveTable(target_conn, target_table)

    logger <- LoggerNull$new()

    # Check we can transfer without error
    expect_no_error(
      update_snapshot(
        .data,
        conn = target_conn,
        db_table = target_table,
        logger = logger,
        timestamp = format(Sys.time())
      )
    )

    # Check that if we collect the table, the signature will match the original
    table_signature <- get_table(target_conn, target_table) |>
      dplyr::collect() |>
      dplyr::summarise(dplyr::across(tidyselect::everything(), ~ class(.)[1])) |>
      as.data.frame()

    expect_identical(
      table_signature,
      dplyr::summarise(mtcars_modified, dplyr::across(tidyselect::everything(), ~ class(.)[1]))
    )


    DBI::dbRemoveTable(target_conn, target_table)
    connection_clean_up(target_conn)
    rm(logger)
    invisible(gc())
  }
  connection_clean_up(source_conn)


  ## Now we test the reverse transfer

  # Test a data transfer from the test connection to a local SQLite
  target_conn <- DBI::dbConnect(RSQLite::SQLite())

  # For each conn, we test if update_snapshot preserves data types
  for (source_conn in get_test_conns()) {

    .data <- dplyr::copy_to(dest = source_conn, df = mtcars_modified, name = unique_table_name())

    target_table <- id("mtcars_modified", target_conn)
    if (DBI::dbExistsTable(target_conn, target_table)) DBI::dbRemoveTable(target_conn, target_table)

    logger <- LoggerNull$new()

    # Check we can transfer without error
    expect_no_error(
      update_snapshot(
        .data,
        conn = target_conn,
        db_table = target_table,
        logger = logger,
        timestamp = format(Sys.time())
      )
    )

    # Check that if we collect the table, the signature will match the original
    table_signature <- get_table(target_conn, target_table) |>
      dplyr::collect() |>
      dplyr::summarise(dplyr::across(tidyselect::everything(), ~ class(.)[1])) |>
      as.data.frame()

    expect_identical(
      table_signature,
      dplyr::summarise(mtcars_modified, dplyr::across(tidyselect::everything(), ~ class(.)[1]))
    )


    connection_clean_up(source_conn)
    rm(logger)
    invisible(gc())
  }
  connection_clean_up(target_conn)
})
