test_that("close_connection() works", { for (conn in conns){

  # Do cleanup here also
  purrr::walk(c("test.mtcars", "__mtcars", "__mtcars_historical",
                "test.mg_logs", "test.mg_tmp1", "test.mg_tmp2", "test.mg_tmp3",
                "test.mg_t0", "test.mg_t1", "test.mg_t2"),
              ~ if (DBI::dbExistsTable(conn, id(., conn))) DBI::dbRemoveTable(conn, id(., conn)))

  # Check that we can close the connection
  expect_true(DBI::dbIsValid(conn))
  close_connection(conn)
  expect_false(DBI::dbIsValid(conn))
}})
