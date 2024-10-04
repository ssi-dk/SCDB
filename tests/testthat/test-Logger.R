# Ensure the options that can be set are NULL for these tests
withr::local_options("SCDB.log_table_id" = NULL, "SCDB.log_path" = NULL)

test_that("Logger: logging to console works", {

  # Create logger and test configuration
  expect_warning(
    logger <- Logger$new(),                                                                                             # nolint: implicit_assignment_linter
    regexp = "NO file or database logging will be done."
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

  # Clean up
  rm(logger)
  invisible(gc())
})


test_that("Logger: all (non-warning, non-error) logging to console can be disabled", {

  # Create logger
  expect_warning(
    logger <- Logger$new(output_to_console = FALSE),                                                                    # nolint: implicit_assignment_linter
    regexp = "NO file or database logging will be done."
  )

  # Test INFO-logging to console is disabled
  ts_str <- format(logger$start_time, "%F %R:%OS3")
  expect_no_message(logger$log_info("test", tic = logger$start_time))

  # Clean up
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
  # Test file logging - with character timestamp
  timestamp <- "2022-01-01 09:00:00"
  logger <- Logger$new(
    db_table = db_table,
    timestamp = timestamp,
    log_path = log_path,
    output_to_console = FALSE,
    warn = FALSE
  )

  expect_equal(logger$log_path, log_path)
  expect_equal(
    logger$log_filename,
    glue::glue("{format(logger$start_time, '%Y%m%d.%H%M')}.",
               "{format(as.POSIXct(timestamp), '%Y_%m_%d')}.",
               "{db_table}.log")
  )


  # Test logging to file has the right formatting and message type
  logger$log_info("test filewriting", tic = logger$start_time)
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
  # Test file logging - with POSIX timestamp
  timestamp <- as.POSIXct("2022-02-01 09:00:00")
  logger <- Logger$new(
    db_table = db_table,
    timestamp = timestamp,
    log_path = log_path,
    output_to_console = FALSE,
    warn = FALSE
  )

  expect_equal(logger$log_path, log_path)
  expect_equal(
    logger$log_filename,
    glue::glue("{format(logger$start_time, '%Y%m%d.%H%M')}.",
               "{format(as.POSIXct(timestamp), '%Y_%m_%d')}.",
               "{db_table}.log")
  )


  # Test logging to file still works
  logger$log_info("test filewriting", tic = logger$start_time)

  ts_str <- format(logger$start_time, "%F %R:%OS3")
  expect_true(logger$log_filename %in% dir(log_path))
  expect_equal(
    readLines(logger$log_realpath),
    glue::glue(
      "{ts_str} - {Sys.info()[['user']]} - INFO - test filewriting"
    )
  )

  # Clean up
  file.remove(logger$log_realpath)
  rm(logger)
  invisible(gc())
})


test_that("Logger: log_tbl is not set when conn = NULL", {

  # Set options for the test
  db_table <- "test.SCDB_logger"
  timestamp <- "2022-03-01 09:00:00"

  # Create logger and test configuration
  # Empty logger should use default value
  logger <- Logger$new(db_table = db_table, timestamp = timestamp, warn = FALSE)
  expect_null(logger$log_tbl) # log_table_id is NOT defined here, despite the option existing
  # the logger does not have the connection, so cannot pull the table from conn

  # Clean up
  rm(logger)
  invisible(gc())
})


test_that("Logger: logging to database works", {
  for (conn in get_test_conns()) {

    # Set options for the test
    db_table <- "test.SCDB_logger"
    timestamp <- "2022-04-01 09:00:00"

    # Create logger and test configuration
    logger <- Logger$new(db_table = db_table,
                         timestamp = timestamp,
                         log_table_id = db_table,
                         log_conn = conn,
                         warn = FALSE)

    log_table_id <- dplyr::tbl(conn, id(db_table, conn))
    expect_equal(logger$log_tbl, log_table_id)


    # Test Logger has pre-filled some information in the logs
    db_table_id <- id(db_table, conn)
    expect_identical(as.character(dplyr::pull(log_table_id, "date")), timestamp)
    if ("catalog" %in% purrr::pluck(db_table_id, "name", names)) {
      expect_identical(dplyr::pull(log_table_id, "catalog"), purrr::pluck(db_table_id, "name", "catalog"))
    }
    expect_identical(dplyr::pull(log_table_id, "schema"), purrr::pluck(db_table_id, "name", "schema"))
    expect_identical(dplyr::pull(log_table_id, "table"), purrr::pluck(db_table_id, "name", "table"))
    expect_identical( # Transferring start_time to database can have some loss of information that we need to match
      format(as.POSIXct(dplyr::pull(log_table_id, "start_time")), "%F %R:%S"),
      format(logger$start_time, "%F %R:%S")
    )


    # Test logging to database writes to the correct fields
    logger$log_to_db(n_insertions = 42)
    expect_equal(nrow(log_table_id), 1)
    expect_equal(dplyr::pull(log_table_id, "n_insertions"), 42)

    logger$log_to_db(n_deactivations = 60)
    expect_equal(nrow(log_table_id), 1)
    expect_equal(dplyr::pull(log_table_id, "n_deactivations"), 60)


    # Clean up
    rm(logger)
    invisible(gc())

    connection_clean_up(conn)
  }
})


test_that("Logger: all logging simultaneously works", {
  for (conn in get_test_conns()) {

    # Set options for the test
    log_path <- tempdir(check = TRUE)
    db_table <- "test.SCDB_logger"
    timestamp <- "2022-05-01 09:00:00"

    # Create logger and test configuration
    logger <- Logger$new(db_table = db_table, timestamp = timestamp, log_path = log_path,
                         log_table_id = db_table, log_conn = conn, warn = FALSE)

    log_table_id <- dplyr::tbl(conn, id(db_table, conn))
    expect_equal(logger$log_path, log_path)
    expect_equal(logger$log_tbl, log_table_id)
    expect_equal(
      logger$log_filename,
      glue::glue("{format(logger$start_time, '%Y%m%d.%H%M')}.",
                 "{format(as.POSIXct(timestamp), '%Y_%m_%d')}.",
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


    # Test logging to database writes to the correct fields
    logger$log_to_db(n_insertions = 13)
    expect_equal(nrow(log_table_id), 2)
    expect_equal(dplyr::pull(log_table_id, "n_insertions"), c(42, 13))

    logger$log_to_db(n_deactivations = 37)
    expect_equal(nrow(log_table_id), 2)
    expect_equal(dplyr::pull(log_table_id, "n_deactivations"), c(60, 37))


    # Clean up
    file.remove(logger$log_realpath)
    rm(logger)
    invisible(gc())

    connection_clean_up(conn)
  }
})


test_that("Logger: file logging stops if file exists", {

  # Set options for the test
  log_path <- tempdir(check = TRUE)
  db_table <- "test.SCDB_logger"
  timestamp <- Sys.time()

  # Create logger1 and logger2 which uses the same file
  # Since start_time is the same for both
  logger1 <- Logger$new(
    db_table = db_table,
    timestamp = timestamp,
    start_time = timestamp,
    log_path = log_path,
    output_to_console = FALSE
  )

  logger2 <- Logger$new(
    db_table = db_table,
    timestamp = timestamp,
    start_time = timestamp,
    log_path = log_path,
    output_to_console = FALSE
  )


  # logger1 should be able to successfully write
  logger1$log_info("test message")


  # whereas logger2 should fail since the log file now exists
  expect_error(
    logger2$log_info("test message"),
    glue::glue("Log file '{logger1$log_filename}' already exists!")
  )

  # .. and it should do it persistently
  expect_error(
    logger2$log_info("test message"),
    glue::glue("Log file '{logger1$log_filename}' already exists!")
  )

  # Clean up
  file.remove(logger1$log_realpath)
  rm(logger1, logger2)
  invisible(gc())
})


test_that("Logger: console output may be disabled", {

  # First test cases with output_to_console == FALSE
  # Here, only print when explicitly stated
  expect_warning(
    logger <- Logger$new(output_to_console = FALSE),                                                                    # nolint: implicit_assignment_linter
    regexp = "NO file or database logging will be done."
  )

  expect_no_message(logger$log_info("Whoops! This should not have been printed!"))

  expect_no_message(logger$log_info("Whoops! This should not have been printed either!", output_to_console = FALSE))

  expect_message(
    logger$log_info("This line should be printed", output_to_console = TRUE),
    "This line should be printed"
  )

  rm(logger)

  # ...and now, only suppress printing when explicitly stated
  expect_warning(
    logger <- Logger$new(output_to_console = TRUE),                                                                     # nolint: implicit_assignment_linter
    regexp = "NO file or database logging will be done."
  )

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


test_that("Logger: log_file is NULL in database if not writing to file", {
  for (conn in get_test_conns()) {

    # Set options for the test
    db_table <- "test.SCDB_logger"
    timestamp <- "2022-06-01 09:00:00"

    # Create logger and test configuration
    logger <- Logger$new(db_table = db_table, timestamp = timestamp, log_conn = conn, log_table_id = "test.SCDB_logger")

    # While logger is active, log_file should be set as the random generated
    db_log_file <- dplyr::pull(dplyr::filter(logger$log_tbl, log_file == !!logger$log_filename))
    expect_length(db_log_file, 1)
    expect_match(db_log_file, "^.+$")

    # When finalising, log_file should be set to NULL
    logger$finalize()

    db_log_file <- dplyr::pull(dplyr::filter(logger$log_tbl, log_file == !!logger$log_filename))
    expect_length(db_log_file, 0)

    # Test that an error is thrown if the database record has been finalized
    expect_error(
      logger$log_to_db(message = "This should produce an error"),
      r"{Logger has already been finalized\. Cannot write to database log table\.}"
    )


    # Clean up
    rm(logger)
    invisible(gc())

    connection_clean_up(conn)
  }
})


test_that("Logger: $finalize() handles log table is at some point deleted", {
  for (conn in get_test_conns()) {

    # Set options for the test
    db_table <- "test.SCDB_logger"
    timestamp <- "2022-06-01 09:00:00"

    log_table_id <- "expendable_log_table"
    logger <- Logger$new(db_table = db_table, timestamp = timestamp, log_conn = conn, log_table_id = log_table_id)

    DBI::dbRemoveTable(conn, id(log_table_id, conn))

    expect_no_error(logger$log_to_db(n_insertions = 42))

    expect_no_error(logger$finalize())

    # Clean up
    rm(logger)
    invisible(gc())

    connection_clean_up(conn)
  }
})

test_that("Logger: custom timestamp_format works", {

  # Create logger and test configuration
  expect_warning(
    logger <- Logger$new(),                                                                                             # nolint: implicit_assignment_linter
    regexp = "NO file or database logging will be done."
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

  # Clean up
  rm(logger)
  invisible(gc())
})


test_that("LoggerNull: no console logging occurs", {

  # Create logger and test configuration
  logger <- expect_no_message(LoggerNull$new())

  ts_str <- format(logger$start_time, "%F %R:%OS3")
  expect_no_message(logger$log_info("test console", tic = logger$start_time))

  # Test logging to console has the right formatting and message type
  ts_str <- format(logger$start_time, "%F %R:%OS3")
  expect_no_message(
    logger$log_info("test console", tic = logger$start_time)
  )
  expect_warning(
    logger$log_warn("test console", tic = logger$start_time),
    glue::glue("{ts_str} - {Sys.info()[['user']]} - WARNING - test console")
  )
  expect_error(
    logger$log_error("test console", tic = logger$start_time),
    glue::glue("{ts_str} - {Sys.info()[['user']]} - ERROR - test console")
  )

  # Clean up
  rm(logger)
  invisible(gc())
})


test_that("LoggerNull: no file logging occurs", {
  withr::local_options("SCDB.log_path" = tempdir())

  # Create logger and test configuration
  logger <- expect_no_message(LoggerNull$new())

  expect_no_message(logger$log_info("test filewriting", tic = logger$start_time))
  expect_false(logger$log_filename %in% dir(getOption("SCDB.log_path")))

  # Clean up
  rm(logger)
  invisible(gc())
})


test_that("LoggerNull: no database logging occurs", {
  for (conn in get_test_conns()) {

    # Set options for the test
    db_table <- "test.SCDB_logger"
    timestamp <- "2022-06-01 09:00:00"

    # Count entries in log
    n_log_entries <- nrow(dplyr::tbl(conn, id("test.SCDB_logger", conn)))

    # Create LoggerNull and test configuration
    logger <- LoggerNull$new(
      db_table = db_table, timestamp = timestamp,
      log_conn = conn, log_table_id = "test.SCDB_logger"
    )

    expect_no_message(logger$log_to_db(n_insertions = 42))
    expect_no_message(logger$finalize_db_entry())
    expect_identical(nrow(dplyr::tbl(conn, id("test.SCDB_logger", conn))), n_log_entries)

    # Clean up
    rm(logger)
    invisible(gc())

    close_connection(conn)
  }
})
