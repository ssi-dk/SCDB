test_that("lock helpers works in default and test schema", {
  for (conn in get_test_conns()) {
    for (schema in list(NULL, "test")) {

      # Define the testing tables
      test_table_id <- id(paste(c(schema, "mtcars"), collapse = "."), conn)
      lock_table_id <- id(paste(c(schema, "locks"), collapse = "."), conn)


      ## Check we can add locks
      add_table_lock(conn, db_table = test_table_id, schema = schema)
      lock_table <- dplyr::tbl(conn, lock_table_id)

      expect_identical(colnames(lock_table), c("schema", "table", "lock_start", "pid"))

      expect_identical(
        dplyr::collect(dplyr::select(lock_table, !"lock_start")),
        tibble::tibble(
          "schema" = purrr::pluck(test_table_id, "name", "schema"),
          "table"  = purrr::pluck(test_table_id, "name", "table"),
          "pid" = as.numeric(Sys.getpid())
        )
      )



      ## Check we are the lock owner
      expect_true(is_lock_owner(conn, db_table = test_table_id, schema = schema))
      expect_false(is_lock_owner(conn, db_table = lock_table_id, schema = schema))



      ## Check we can remove locks
      remove_table_lock(conn, db_table = test_table_id, schema = schema)
      expect_identical(nrow(lock_table), 0)



      ## Check locks with invalid PID are removed automatically
      dplyr::rows_append(
        lock_table,
        tibble::tibble(
          "schema" = purrr::pluck(test_table_id, "name", "schema"),
          "table"  = purrr::pluck(test_table_id, "name", "table"),
          "lock_start" = Sys.time(),
          "pid" = 0.5
        ),
        in_place = TRUE,
        copy = TRUE
      )
      expect_identical(nrow(lock_table), 1)


      remove_expired_locks(conn, schema = schema)
      expect_identical(nrow(lock_table), 0)




      ## Check locks can be timed out automatically (directly)
      add_table_lock(conn, db_table = test_table_id, schema = schema)
      expect_identical(nrow(lock_table), 1)

      # With long timer, lock should stay
      remove_expired_locks(conn, schema = schema, lock_wait_max = 30)
      expect_identical(nrow(lock_table), 1)

      Sys.sleep(1)
      remove_expired_locks(conn, schema = schema, lock_wait_max = 0)
      expect_identical(nrow(lock_table), 0)


      ## Check locks can be timed out automatically (through options)
      add_table_lock(conn, db_table = test_table_id, schema = schema)
      expect_identical(nrow(lock_table), 1)

      Sys.sleep(1)
      withr::with_options(
        list("SCDB.lock_wait_max" = 0),
        remove_expired_locks(conn, schema = schema)
      )
      expect_identical(nrow(lock_table), 0)



      # Clean up
      DBI::dbRemoveTable(conn, lock_table_id)
    }
    close_connection(conn)
  }
})
