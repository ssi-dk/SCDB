test_that("Logger works", {
  for (conn in get_test_conns()) {

    # Logger only handles tables updates with timestamps
    db_tablestring <- "test.SCDB_logger"
    ts <- "2022-01-01 09:00:00"

    # minimal logger
    expect_warning(
      logger <- Logger$new(log_table_id = NULL, log_path = NULL),
      regexp = "NO LOGGING WILL BE DONE"
    )
    expect_null(logger$log_path)
    expect_null(logger$log_tbl)

    ts_str <- stringr::str_replace(format(logger$start_time, "%F %H:%M:%OS3", locale = "en"), "[.]", ",")
    expect_equal(utils::capture.output(logger$log_info("test", tic = logger$start_time)),
                 glue::glue("{ts_str} - {Sys.info()[['user']]} - INFO - test"))
    expect_warning(utils::capture.output(logger$log_warn("test", tic = logger$start_time)),
                   regexp = glue::glue("{ts_str} - {Sys.info()[['user']]} - WARNING - test"))
    expect_error(utils::capture.output(logger$log_error("test", tic = logger$start_time)),
                 regexp = glue::glue("{ts_str} - {Sys.info()[['user']]} - ERROR - test"))



    # Empty logger should use default value
    logger <- Logger$new(db_tablestring = db_tablestring, ts = ts, warn = FALSE)
    expect_equal(logger$log_path, getOption("SCDB.log_path"))
    expect_null(logger$log_tbl) # log_table_id is NOT defined here, despite the option existing
    # the logger does not have the connection, so cannot pull the table from conn



    # Test file logging
    log_path <- tempdir(check = TRUE)
    logger <- Logger$new(db_tablestring = db_tablestring, ts = ts, log_path = log_path, warn = FALSE)
    expect_equal(logger$log_path, log_path)
    expect_null(logger$log_tbl)
    expect_equal(logger$log_filename,
                 glue::glue("{format(logger$start_time, '%Y%m%d.%H%M')}.",
                            "{format(as.POSIXct(ts), '%Y_%m_%d')}.",
                            "{db_tablestring}.log"))
    tic <- Sys.time()
    utils::capture.output(logger$log_info("test - filewriting", tic = tic))
    ts_str <- stringr::str_replace(format(tic, "%F %H:%M:%OS3", locale = "en"), "[.]", ",")
    expect_true(logger$log_filename %in% dir(log_path))
    expect_equal(readLines(file.path(log_path, logger$log_filename)),
                 glue::glue("{ts_str} - {Sys.info()[['user']]} - INFO - test - filewriting"))


    # Test file logging - with posix ts
    ts <- "2022-02-01 09:00:00"
    logger <- Logger$new(db_tablestring = db_tablestring, ts = as.POSIXct(ts), log_path = log_path, warn = FALSE)
    expect_equal(logger$log_path, log_path)
    expect_null(logger$log_tbl)
    expect_equal(logger$log_filename,
                 glue::glue("{format(logger$start_time, '%Y%m%d.%H%M')}.",
                            "{format(as.POSIXct(ts), '%Y_%m_%d')}.",
                            "{db_tablestring}.log"))




    # Test db logging
    if (DBI::dbExistsTable(conn, id("test.SCDB_logs", conn))) DBI::dbRemoveTable(conn, id("test.SCDB_logs", conn))
    logger <- Logger$new(db_tablestring = db_tablestring,
                         ts = ts,
                         log_table_id = "test.SCDB_logs",
                         log_conn = conn,
                         warn = FALSE)
    log_table_id <- dplyr::tbl(conn, id("test.SCDB_logs", conn), check_from = FALSE)
    expect_equal(logger$log_tbl, log_table_id)
    logger$log_to_db(n_insertions = 42)
    expect_equal(nrow(log_table_id), 1)
    expect_equal(dplyr::pull(log_table_id, "n_insertions"), 42)
    logger$log_to_db(n_deactivations = 60)
    expect_equal(nrow(log_table_id), 1)
    expect_equal(dplyr::pull(log_table_id, "n_deactivations"), 60)
    logger$log_to_db(schema = NULL, table = "foo")
    expect_equal(nrow(log_table_id), 1)
    expect_true(is.na(dplyr::pull(log_table_id, "schema")))
    expect_equal(dplyr::pull(log_table_id, "table"), "foo")



    # Test db logging with file logging
    ts <- "2022-03-01 09:00:00"

    # set warn = FALSE as we've already tested this
    logger <- Logger$new(db_tablestring = db_tablestring, ts = ts, log_path = log_path,
                         log_table_id = "test.SCDB_logs", log_conn = conn, warn = FALSE)
    expect_equal(logger$log_path, log_path)
    expect_equal(logger$log_tbl, log_table_id)
    expect_equal(logger$log_filename,
                 glue::glue("{format(logger$start_time, '%Y%m%d.%H%M')}.",
                            "{format(as.POSIXct(ts), '%Y_%m_%d')}.",
                            "{db_tablestring}.log"))
    tic <- Sys.time()
    utils::capture.output(logger$log_info("test - filewriting", tic = tic))
    ts_str <- stringr::str_replace(format(tic, "%F %H:%M:%OS3", locale = "en"), "[.]", ",")
    expect_true(logger$log_filename %in% dir(log_path))
    expect_equal(readLines(file.path(log_path, logger$log_filename)),
                 glue::glue("{ts_str} - {Sys.info()[['user']]} - INFO - test - filewriting"))

    logger$log_to_db(n_insertions = 13)
    expect_equal(nrow(log_table_id), 2)
    expect_equal(dplyr::pull(log_table_id, "n_insertions"), c(42, 13))
    logger$log_to_db(n_deactivations = 37)
    expect_equal(nrow(log_table_id), 2)
    expect_equal(dplyr::pull(log_table_id, "n_deactivations"), c(60, 37))

    # Cleanup
    dir(log_path) |>
      purrr::keep(~ endsWith(., ".log")) |>
      purrr::walk(~ unlink(file.path(log_path, .)))

    connection_clean_up(conn)
  }
})

test_that("Logger stops if file exists", {

  db_tablestring <- "test.SCDB_logger"
  ts <- Sys.time()
  log_path <- tempdir()
  logger <- Logger$new(
    db_tablestring = db_tablestring,
    ts = ts,
    log_table_id = NULL,
    log_path = log_path
  )

  utils::capture.output(logger$log_info("test message"))

  logger2 <- Logger$new(
    db_tablestring = db_tablestring,
    ts = ts,
    start_time = ts,
    log_table_id = NULL,
    log_path = log_path
  )

  expect_error(
    logger2$log_info("test message")
  )

  file.remove(file.path(log_path, logger$log_filename))

  rm(logger, logger2)
  gc()
})

test_that("Logger console output may be suppressed", {
  db_tablestring <- "test.SCDB_logger"
  ts <- Sys.time()
  log_path <- tempdir()

  # First test cases with output_to_console == FALSE
  # Here, only print when explicitly stated
  logger <- Logger$new(
    db_tablestring = db_tablestring,
    ts = ts,
    log_table_id = NULL,
    log_path = log_path,
    output_to_console = FALSE
  )

  # In lieu of testthat::expect_no_output...
  expect_identical(
    utils::capture.output(logger$log_info("Whoops! This should not have been printed!"), type = "output"),
    character(0)
  )
  expect_identical(
    utils::capture.output(logger$log_info("Whoops! This should not have been printed either!",
                                          output_to_console = FALSE),
                          type = "output"),
    character(0)
  )

  expect_output(
    logger$log_info("This line should be printed",
                    output_to_console = TRUE),
    regex = "This line should be printed$"
  )

  # ...and now, only suppress printing when explicitly stated
  logger$output_to_console <- TRUE

  expect_output(
    logger$log_info("This line should be printed"),
    regex = "This line should be printed$"
  )
  expect_output(
    logger$log_info("This line should also be printed",
                    output_to_console = TRUE),
    regex = "This line should also be printed$"
  )

  expect_identical(
    utils::capture.output(logger$log_info("Whoops! This should not have been printed at all!",
                                          output_to_console = FALSE),
                          type = "output"),
    character(0)
  )
})

test_that("Logger sets log_file to NULL in DB if not writing to file", {
  for (conn in get_test_conns()) {

    logger <- Logger$new(log_conn = conn,
                         log_table_id = "logs",
                         log_path = NULL)

    db_log_file <- dplyr::pull(dplyr::filter(logger$log_tbl, log_file == !!logger$log_filename))
    expect_length(db_log_file, 1)
    expect_match(db_log_file, "^.+$")

    logger$finalize()
    gc()

    connection_clean_up(conn)
  }
})

test_that("Logger$finalize handles log table is at some point deleted", {
  for (conn in get_test_conns()) {

    log_table_id <- "expendable_log_table"
    logger <- Logger$new(log_conn = conn,
                         log_table_id = log_table_id,
                         log_path = NULL)

    DBI::dbRemoveTable(conn, log_table_id)

    expect_no_error(logger$finalize())
    gc()

    connection_clean_up(conn)
  }
})
