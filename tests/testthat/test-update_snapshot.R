test_that("update_snapshot() works", {
  for (conn in get_test_conns()) {

    if (DBI::dbExistsTable(conn, id("test.SCDB_tmp1", conn))) DBI::dbRemoveTable(conn, id("test.SCDB_tmp1", conn))
    if (DBI::dbExistsTable(conn, id("test.SCDB_logs", conn))) DBI::dbRemoveTable(conn, id("test.SCDB_logs", conn))

    target <- mtcars |>
      dplyr::copy_to(conn, df = _, name = unique_table_name()) |>
      digest_to_checksum(col = "checksum") |>
      dplyr::mutate(from_ts  = !!db_timestamp("2022-10-01 09:00:00", conn),
                    until_ts = !!db_timestamp(NA, conn))

    # Copy target to conn (and suppress check_from message)
    target <- suppressMessages(
      dplyr::copy_to(conn, target, name = id("test.SCDB_tmp1", conn), overwrite = TRUE, temporary = FALSE)
    )

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

    dir(log_path) |>
      purrr::keep(~ endsWith(., ".log")) |>
      purrr::walk(~ unlink(file.path(log_path, .)))

    update_snapshot(.data, conn, db_table, timestamp, logger = logger)

    expect_identical(slice_time(target, "2022-10-01 09:00:00") |>
                       dplyr::select(!c("from_ts", "until_ts", "checksum")) |>
                       dplyr::collect() |>
                       dplyr::arrange(wt, qsec),
                     mtcars |> dplyr::arrange(wt, qsec) |> tibble::tibble())
    expect_equal(nrow(slice_time(target, "2022-10-01 09:00:00")),
                 nrow(mtcars))

    expect_identical(slice_time(target, "2022-10-03 09:00:00") |>
                       dplyr::select(!c("from_ts", "until_ts", "checksum")) |>
                       dplyr::collect() |>
                       dplyr::arrange(wt, qsec),
                     .data |> dplyr::collect() |> dplyr::arrange(wt, qsec))
    expect_equal(nrow(slice_time(target, "2022-10-03 09:00:00")),
                 nrow(mtcars))

    # Check file log outputs exists
    log_pattern <- glue::glue("{stringr::str_replace_all(as.Date(timestamp), '-', '_')}.{id(db_table, conn)}.log")
    log_file <- purrr::keep(dir(log_path), ~stringr::str_detect(., log_pattern))
    expect_length(log_file, 1)
    expect_gt(file.info(file.path(log_path, log_file))$size, 0)
    expect_identical(nrow(get_table(conn, "test.SCDB_logs")), 1)

    db_logs_with_log_file <- get_table(conn, "test.SCDB_logs") |>
      dplyr::filter(!is.na(.data$log_file))
    expect_identical(nrow(db_logs_with_log_file), 1)

    # Check db log output
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


    # We now attempt to do another update on the same date
    .data <- mtcars |>
      dplyr::mutate(hp = dplyr::if_else(hp > 100, hp - 10, hp)) |>
      dplyr::copy_to(conn, df = _, name = unique_table_name())

    update_snapshot(.data, conn, "test.SCDB_tmp1", "2022-10-03 09:00:00", logger = logger)

    # Even though we insert twice on the same date, we expect the data to be minimal (compacted)
    expect_identical(slice_time(target, "2022-10-01 09:00:00") |>
                       dplyr::select(!c("from_ts", "until_ts", "checksum")) |>
                       dplyr::collect() |>
                       dplyr::arrange(wt, qsec),
                     mtcars |> dplyr::arrange(wt, qsec) |> tibble::tibble())
    expect_equal(nrow(slice_time(target, "2022-10-01 09:00:00")),
                 nrow(mtcars))

    expect_identical(slice_time(target, "2022-10-03 09:00:00") |>
                       dplyr::select(!c("from_ts", "until_ts", "checksum")) |>
                       dplyr::collect() |>
                       dplyr::arrange(wt, qsec),
                     .data |> dplyr::collect() |> dplyr::arrange(wt, qsec))
    expect_equal(nrow(slice_time(target, "2022-10-03 09:00:00")),
                 nrow(mtcars))


    # We now attempt to an update between these two updates
    .data <- mtcars |>
      dplyr::mutate(hp = dplyr::if_else(hp > 150, hp - 10, hp)) |>
      dplyr::copy_to(conn, df = _, name = unique_table_name())

    # This should fail if we do not specify "enforce_chronological_order = FALSE"
    expect_error(
      update_snapshot(.data, conn, "test.SCDB_tmp1", "2022-10-02 09:00:00", logger = logger),
      regexp = "Given timestamp 2022-10-02 09:00:00 is earlier"
    )

    # But not with the variable set
    update_snapshot(.data, conn, "test.SCDB_tmp1", "2022-10-02 09:00:00",
                    logger = logger, enforce_chronological_order = FALSE)

    expect_identical(slice_time(target, "2022-10-01 09:00:00") |>
                       dplyr::select(!c("from_ts", "until_ts", "checksum")) |>
                       dplyr::collect() |>
                       dplyr::arrange(wt, qsec),
                     mtcars |> dplyr::arrange(wt, qsec) |> tibble::tibble())
    expect_equal(nrow(slice_time(target, "2022-10-01 09:00:00")),
                 nrow(mtcars))

    expect_identical(slice_time(target, "2022-10-02 09:00:00") |>
                       dplyr::select(!c("from_ts", "until_ts", "checksum")) |>
                       dplyr::collect() |>
                       dplyr::arrange(wt, qsec),
                     .data |> dplyr::collect() |> dplyr::arrange(wt, qsec))
    expect_equal(nrow(slice_time(target, "2022-10-02 09:00:00")),
                 nrow(mtcars))


    if (DBI::dbExistsTable(conn, id("test.SCDB_tmp1", conn))) DBI::dbRemoveTable(conn, id("test.SCDB_tmp1", conn))
    expect_false(table_exists(conn, "test.SCDB_tmp1"))

    # Check deletion of redundant rows
    t0 <- data.frame(col1 = c("A", "B"),      col2 = c(NA_real_, NA_real_))
    t1 <- data.frame(col1 = c("A", "B", "C"), col2 = c(1,        NA_real_, NA_real_))
    t2 <- data.frame(col1 = c("A", "B", "C"), col2 = c(1,        2,        3))

    # Copy t0, t1, and t2 to conn (and suppress check_from message)
    t0 <- suppressMessages(
      dplyr::copy_to(conn, t0, name = id("test.SCDB_t0", conn), overwrite = TRUE, temporary = FALSE)
    )
    t1 <- suppressMessages(
      dplyr::copy_to(conn, t1, name = id("test.SCDB_t1", conn), overwrite = TRUE, temporary = FALSE)
    )
    t2 <- suppressMessages(
      dplyr::copy_to(conn, t2, name = id("test.SCDB_t2", conn), overwrite = TRUE, temporary = FALSE)
    )


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
      purrr::reduce(union) |>
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

    if (DBI::dbExistsTable(conn, id("test.SCDB_tmp1", conn))) DBI::dbRemoveTable(conn, id("test.SCDB_tmp1", conn))



    # Check non-chronological insertion
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

    expect_identical(get_table(conn, "test.SCDB_tmp1", slice_ts = NULL) |>
                       dplyr::select(!"checksum") |>
                       dplyr::collect() |>
                       dplyr::mutate(from_ts  = strftime(from_ts),
                                     until_ts = strftime(until_ts)) |>
                       dplyr::arrange(col1, from_ts),
                     t_ref |>
                       dplyr::arrange(col1, from_ts))

    if (file.exists(logger$log_realpath)) file.remove(logger$log_realpath)

    if (DBI::dbExistsTable(conn, id("test.SCDB_tmp1", conn))) DBI::dbRemoveTable(conn, id("test.SCDB_tmp1", conn))

    connection_clean_up(conn)
  }
})


test_that("update_snapshot works with Id objects", {
  withr::local_options("SCDB.log_path" = tempdir())

  for (conn in get_test_conns()) {

    target_table <- id("test.mtcars_modified", conn)

    logger <- Logger$new(output_to_console = FALSE,
                         timestamp = Sys.time(),
                         db_table = "test.mtcars_modified",
                         log_conn = NULL,
                         log_table_id = NULL)

    expect_no_error(
      mtcars |>
        dplyr::mutate(disp = sample(mtcars$disp, nrow(mtcars))) |>
        dplyr::copy_to(dest = conn,
                       df = _,
                       name = "mtcars_modified") |>
        update_snapshot(
          conn = conn,
          db_table = target_table,
          logger = logger,
          timestamp = format(Sys.time())
        )
    )

    unlink(logger$log_realpath)
    connection_clean_up(conn)
  }
})


test_that("update_snapshot checks table formats", {

  withr::local_options("SCDB.log_path" = tempdir())

  for (conn in get_test_conns()) {

    mtcars_table <- dplyr::tbl(conn, id("__mtcars_historical", conn = conn))
    timestamp <- Sys.time()

    expect_warning(
      logger <- Logger$new(log_path = NULL, log_table_id = NULL, output_to_console = FALSE),                            # nolint: implicit_assignment_linter
      "NO file or DB logging will be done."
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
