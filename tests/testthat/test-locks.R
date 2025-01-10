test_that("lock helpers works in default and test schema", {
  for (conn in get_test_conns()) {
    for (schema in list(NULL, "test")) {

      # Define the testing tables
      test_table_id <- id(paste(c(schema, "mtcars"), collapse = "."), conn)
      lock_table_id <- id(paste(c(schema, "locks"), collapse = "."), conn)


      ## Check we can add locks
      expect_true(lock_table(conn, db_table = test_table_id, schema = schema))

      db_lock_table <- dplyr::tbl(conn, lock_table_id)
      expect_identical(colnames(db_lock_table), c("schema", "table", "user", "lock_start", "pid"))

      expect_identical(
        dplyr::collect(dplyr::select(db_lock_table, !"lock_start")),
        tibble::tibble(
          "schema" = purrr::pluck(test_table_id, "name", "schema"),
          "table"  = purrr::pluck(test_table_id, "name", "table"),
          "user" = Sys.info()[["user"]],
          "pid" = as.numeric(Sys.getpid())
        )
      )



      ## Check we can remove locks
      expect_null(unlock_table(conn, db_table = test_table_id, schema = schema))
      expect_identical(nrow(db_lock_table), 0L)



      # Add an invalid lock that we do not own
      dplyr::rows_append(
        db_lock_table,
        tibble::tibble(
          "schema" = purrr::pluck(test_table_id, "name", "schema"),
          "table"  = purrr::pluck(test_table_id, "name", "table"),
          "user"   = "some_other_user",
          "lock_start" = as.numeric(Sys.time()),
          "pid" = 0.5
        ),
        in_place = TRUE,
        copy = TRUE
      )
      expect_identical(nrow(db_lock_table), 1L)

      ## Check invalid lock owners are flagged
      not_on_cran <- interactive() || identical(Sys.getenv("NOT_CRAN"), "true") || identical(Sys.getenv("CI"), "true")
      if (not_on_cran) { # Detection of currently valid PID does not work on CRAN machines, therefore no error is thrown
        expect_error(
          lock_table(conn, test_table_id, schema = schema),
          glue::glue(
            "Active lock \\(user = some_other_user, PID = 0.5\\) on table {test_table_id} is no longer a valid PID! ",
            "Process likely crashed before completing."
          )
        )
      }

      # Remove the lock
      unlock_table(conn, db_table = test_table_id, schema = schema, pid = 0.5)
      expect_identical(nrow(db_lock_table), 0L)



      ## Check that we cannot steal locks
      # Get the PID of a background process that will linger for a while
      bg_process <- callr::r_bg(\() Sys.sleep(10))
      expect_false(bg_process$get_pid() == Sys.getpid())

      # Add a valid lock that we do not own
      dplyr::rows_append(
        db_lock_table,
        tibble::tibble(
          "schema" = purrr::pluck(test_table_id, "name", "schema"),
          "table"  = purrr::pluck(test_table_id, "name", "table"),
          "user"   = "some_other_user",
          "lock_start" = as.numeric(Sys.time()),
          "pid" = bg_process$get_pid()
        ),
        in_place = TRUE,
        copy = TRUE
      )

      ## Check we cannot achieve table lock
      expect_false(lock_table(conn, test_table_id, schema = schema))

      # Remove the lock
      unlock_table(conn, db_table = test_table_id, schema = schema, pid = bg_process$get_pid())
      expect_identical(nrow(db_lock_table), 0L)

      # Clean up
      DBI::dbRemoveTable(conn, lock_table_id)
    }
    close_connection(conn)
  }
})
