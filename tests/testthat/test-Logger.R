# Ensure the options that can be set are NULL for these tests
withr::local_options("SCDB.log_table_id" = NULL, "SCDB.log_path" = NULL)

test_that("Logger: logging to console works", {

  # Create logger and test configuration
  expect_warning(
    logger <- Logger$new(),
    regexp = "NO LOGGING WILL BE DONE"
  )
  expect_null(logger$log_path)
  expect_null(logger$log_tbl)

  # Test logging to console has the right formatting and message type
  ts_str <- format(logger$start_time, "%F %R:%OS3")
  expect_message(
    logger$log_info("test console", tic = logger$start_time),
    glue::glue("{ts_str} - {Sys.info()[['user']]} - INFO - test console")
  )
  expect_warning(
    logger$log_warn("test console", tic = logger$start_time),
    glue::glue("{ts_str} - {Sys.info()[['user']]} - WARNING - test console")
  )
  expect_error(
    logger$log_error("test console", tic = logger$start_time),
    glue::glue("{ts_str} - {Sys.info()[['user']]} - ERROR - test console")
  )

  rm(logger)
  invisible(gc())
})


test_that("Logger: all (non-warning, non-error) logging to console can be disabled", {

  # Create logger
  expect_warning(
    logger <- Logger$new(output_to_console = FALSE),
    regexp = "NO LOGGING WILL BE DONE"
  )

  # Test INFO-logging to console is disabled
  ts_str <- format(logger$start_time, "%F %R:%OS3")
  expect_no_message(logger$log_info("test", tic = logger$start_time))

  rm(logger)
  invisible(gc())
})


test_that("Logger: logging to file works", {

  # Set options for the test
  log_path <- tempdir(check = TRUE)
  db_table <- "test.SCDB_logger"


  # Create logger and test configuration
  # Empty logger should use default value from options
  withr::with_options(
    list("SCDB.log_path" = "local/path"),
    {
      logger <- Logger$new(db_table = db_table, warn = FALSE)
      expect_equal(logger$log_path, "local/path")

      rm(logger)
      invisible(gc())
    }
  )

  # Create logger and test configuration
  # Test file logging - with character ts
  ts <- "2022-01-01 09:00:00"
  logger <- Logger$new(
    db_table = db_table,
    ts = ts,
    log_path = log_path,
    output_to_console = FALSE,
    warn = FALSE
  )

  expect_equal(logger$log_path, log_path)
  expect_equal(
    logger$log_filename,
    glue::glue("{format(logger$start_time, '%Y%m%d.%H%M')}.",
               "{format(as.POSIXct(ts), '%Y_%m_%d')}.",
               "{db_table}.log")
  )


  # Test logging to file has the right formatting and message type
  suppressMessages(logger$log_info("test filewriting", tic = logger$start_time))
  tryCatch(logger$log_warn("test filewriting", tic = logger$start_time), warning = \(w) NULL)
  tryCatch(logger$log_error("test filewriting", tic = logger$start_time), error = \(e) NULL)

  ts_str <- format(logger$start_time, "%F %R:%OS3")
  expect_true(logger$log_filename %in% dir(log_path))
  expect_equal(
    readLines(logger$log_realpath),
    c(
      glue::glue("{ts_str} - {Sys.info()[['user']]} - INFO - test filewriting"),
      glue::glue("{ts_str} - {Sys.info()[['user']]} - WARNING - test filewriting"),
      glue::glue("{ts_str} - {Sys.info()[['user']]} - ERROR - test filewriting")
    )
  )

  file.remove(logger$log_realpath)
  rm(logger)
  invisible(gc())


  # Create logger and test configuration
  # Test file logging - with POSIX ts
  ts <- as.POSIXct("2022-02-01 09:00:00")
  logger <- Logger$new(db_table = db_table, ts = ts, log_path = log_path, warn = FALSE)

  expect_equal(logger$log_path, log_path)
  expect_equal(
    logger$log_filename,
    glue::glue("{format(logger$start_time, '%Y%m%d.%H%M')}.",
               "{format(as.POSIXct(ts), '%Y_%m_%d')}.",
               "{db_table}.log")
  )


  # Test logging to file still works
  suppressMessages(logger$log_info("test filewriting", tic = logger$start_time))

  ts_str <- format(logger$start_time, "%F %R:%OS3")
  expect_true(logger$log_filename %in% dir(log_path))
  expect_equal(
    readLines(logger$log_realpath),
    glue::glue(
      "{ts_str} - {Sys.info()[['user']]} - INFO - test filewriting"
    )
  )

  file.remove(logger$log_realpath)
  rm(logger)
  invisible(gc())
})


test_that("Logger: log_tbl is not set when conn = NULL", {

  # Set options for the test
  db_table <- "test.SCDB_logger"
  ts <- "2022-03-01 09:00:00"

  # Create logger and test configuration
  # Empty logger should use default value
  logger <- Logger$new(db_table = db_table, ts = ts, warn = FALSE)
  expect_null(logger$log_tbl) # log_table_id is NOT defined here, despite the option existing
  # the logger does not have the connection, so cannot pull the table from conn

  rm(logger)
  invisible(gc())
})


test_that("Logger: logging to db works", {
  for (conn in get_test_conns()) {

    # Set options for the test
    db_table <- "test.SCDB_logger"
    ts <- "2022-04-01 09:00:00"

    # Create logger and test configuration
    logger <- Logger$new(db_table = db_table,
                         ts = ts,
                         log_table_id = db_table,
                         log_conn = conn,
                         warn = FALSE)

    log_table_id <- dplyr::tbl(conn, id(db_table, conn), check_from = FALSE)
    expect_equal(logger$log_tbl, log_table_id)


    # Test logging to db writes to the correct fields
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


    # Clean up
    connection_clean_up(conn)
    rm(logger)
    invisible(gc())
  }
})


test_that("Logger: all logging simultanously works", {
  for (conn in get_test_conns()) {

    # Set options for the test
    log_path <- tempdir(check = TRUE)
    db_table <- "test.SCDB_logger"
    ts <- "2022-05-01 09:00:00"

    # Create logger and test configuration
    logger <- Logger$new(db_table = db_table, ts = ts, log_path = log_path,
                         log_table_id = db_table, log_conn = conn, warn = FALSE)

    log_table_id <- dplyr::tbl(conn, id(db_table, conn), check_from = FALSE)
    expect_equal(logger$log_path, log_path)
    expect_equal(logger$log_tbl, log_table_id)
    expect_equal(
      logger$log_filename,
      glue::glue("{format(logger$start_time, '%Y%m%d.%H%M')}.",
                 "{format(as.POSIXct(ts), '%Y_%m_%d')}.",
                 "{id(db_table, conn)}.log")
    )

    # Test logging to console has the right formatting and message type
    ts_str <- format(logger$start_time, "%F %R:%OS3")
    expect_message(
      logger$log_info("test console and filewriting", tic = logger$start_time),
      glue::glue("{ts_str} - {Sys.info()[['user']]} - INFO - test console and filewriting")
    )
    expect_warning(
      logger$log_warn("test console and filewriting", tic = logger$start_time),
      glue::glue("{ts_str} - {Sys.info()[['user']]} - WARNING - test console and filewriting")
    )
    expect_error(
      logger$log_error("test console and filewriting", tic = logger$start_time),
      glue::glue("{ts_str} - {Sys.info()[['user']]} - ERROR - test console and filewriting")
    )


    # Test logging to file has the right formatting and message type
    expect_true(logger$log_filename %in% dir(log_path))
    expect_equal(
      readLines(logger$log_realpath),
      c(
        glue::glue("{ts_str} - {Sys.info()[['user']]} - INFO - test console and filewriting"),
        glue::glue("{ts_str} - {Sys.info()[['user']]} - WARNING - test console and filewriting"),
        glue::glue("{ts_str} - {Sys.info()[['user']]} - ERROR - test console and filewriting")
      )
    )


    # Test logging to db writes to the correct fields
    logger$log_to_db(n_insertions = 13)
    expect_equal(nrow(log_table_id), 2)
    expect_equal(dplyr::pull(log_table_id, "n_insertions"), c(42, 13))

    logger$log_to_db(n_deactivations = 37)
    expect_equal(nrow(log_table_id), 2)
    expect_equal(dplyr::pull(log_table_id, "n_deactivations"), c(60, 37))


    # Clean up
    connection_clean_up(conn)
    file.remove(logger$log_realpath)
    rm(logger)
    invisible(gc())
  }
})


test_that("Logger: file logging stops if file exists", {

  # Set options for the test
  log_path <- tempdir(check = TRUE)
  db_table <- "test.SCDB_logger"
  ts <- Sys.time()

  # Create logger1 and logger2 which uses the same file
  # Since start_time is the same for both
  logger1 <- Logger$new(
    db_table = db_table,
    ts = ts,
    log_path = log_path,
    output_to_console = FALSE
  )

  logger2 <- Logger$new(
    db_table = db_table,
    ts = ts,
    start_time = ts,
    log_path = log_path,
    output_to_console = FALSE
  )


  # logger1 should be able to successfully write
  logger1$log_info("test message")


  # whereas logger2 should fail since the log file now exists
  expect_error(
    logger2$log_info("test message"),
    "already exists!"
  )

  file.remove(logger1$log_realpath)

  rm(logger1, logger2)
  invisible(gc())
})


test_that("Logger: console output may be disabled", {

  # First test cases with output_to_console == FALSE
  # Here, only print when explicitly stated
  expect_warning(
    logger <- Logger$new(output_to_console = FALSE),
    regexp = "NO LOGGING WILL BE DONE"
  )

  expect_no_message(logger$log_info("Whoops! This should not have been printed!"))

  expect_no_message(logger$log_info("Whoops! This should not have been printed either!", output_to_console = FALSE))

  expect_message(
    logger$log_info("This line should be printed", output_to_console = TRUE),
    "This line should be printed"
  )

  # ...and now, only suppress printing when explicitly stated
  logger$output_to_console <- TRUE

  expect_message(
    logger$log_info("This line should be printed"),
    "This line should be printed"
  )
  expect_message(
    logger$log_info("This line should also be printed", output_to_console = TRUE),
    "This line should also be printed"
  )

  expect_no_message(logger$log_info("Whoops! This should not have been printed at all!", output_to_console = FALSE))
})


test_that("Logger: log_file is NULL in DB if not writing to file", {
  for (conn in get_test_conns()) {

    # Set options for the test
    log_path <- tempdir(check = TRUE)
    db_table <- "test.SCDB_logger"
    ts <- "2022-06-01 09:00:00"

    # Create logger and test configuration
    logger <- Logger$new(log_conn = conn, log_table_id = "test.SCDB_logger")

    # While logger is active, log_file should be set as the random generated
    db_log_file <- dplyr::pull(dplyr::filter(logger$log_tbl, log_file == !!logger$log_filename))
    expect_length(db_log_file, 1)
    expect_match(db_log_file, "^.+$")

    # When finalising, log_file should be set to NULL
    logger$finalize()

    db_log_file <- dplyr::pull(dplyr::filter(logger$log_tbl, log_file == !!logger$log_filename))
    expect_length(db_log_file, 0)

    # Clean up
    connection_clean_up(conn)
    rm(logger)
    invisible(gc())
  }
})


test_that("Logger: $finalize handles log table is at some point deleted", {
  for (conn in get_test_conns()) {

    log_table_id <- "expendable_log_table"
    logger <- Logger$new(log_conn = conn,
                         log_table_id = log_table_id)

    DBI::dbRemoveTable(conn, log_table_id)

    expect_no_error(logger$finalize())

    # Clean up
    connection_clean_up(conn)
    rm(logger)
    invisible(gc())
  }
})

test_that("Logger: custom timestamp_format works", {

  # Create logger and test configuration
  expect_warning(
    logger <- Logger$new(),
    regexp = "NO LOGGING WILL BE DONE"
  )

  # Test logging to console has the right formatting and message type
  ts_str <- format(logger$start_time, "%F %R:%OS3")
  expect_message(
    logger$log_info("test console", tic = logger$start_time),
    glue::glue("{ts_str} - {Sys.info()[['user']]} - INFO - test console")
  )

  ts_str <- format(logger$start_time, "%F %R")
  expect_message(
    logger$log_info("test console", tic = logger$start_time, timestamp_format = "%F %R"),
    glue::glue("{ts_str} - {Sys.info()[['user']]} - INFO - test console")
  )

  ts_str <- format(logger$start_time, "%F")
  withr::local_options("SCDB.log_timestamp_format" = "%F")
  expect_message(
    logger$log_info("test console", tic = logger$start_time),
    glue::glue("{ts_str} - {Sys.info()[['user']]} - INFO - test console")
  )

  rm(logger)
  invisible(gc())
})
