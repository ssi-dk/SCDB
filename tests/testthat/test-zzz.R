test_that("close_connection() works", { for (conn in conns) { # nolint: brace_linter

  # Do cleanup here also
  purrr::walk(c("test.mtcars", "__mtcars", "__mtcars_historical",
                "test.SCDB_logs", "test.SCDB_tmp1", "test.SCDB_tmp2", "test.SCDB_tmp3",
                "test.SCDB_t0", "test.SCDB_t1", "test.SCDB_t2"),
              ~ if (DBI::dbExistsTable(conn, id(., conn))) DBI::dbRemoveTable(conn, id(., conn)))

  # Check that we can close the connection
  expect_true(DBI::dbIsValid(conn))
  close_connection(conn)
  expect_false(DBI::dbIsValid(conn))
}})
