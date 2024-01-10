  # Do clean up on the connections
  for (conn in get_test_conns()) {
    purrr::walk(c("test.mtcars", "__mtcars", "__mtcars_historical",
                "test.SCDB_logs", "test.SCDB_tmp1", "test.SCDB_tmp2", "test.SCDB_tmp3",
                "test.SCDB_t0", "test.SCDB_t1", "test.SCDB_t2"),
              ~ if (DBI::dbExistsTable(conn, id(., conn))) DBI::dbRemoveTable(conn, id(., conn)))
  }
