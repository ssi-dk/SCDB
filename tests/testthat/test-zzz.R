# Do clean up on the connections
for (conn in get_test_conns()) {
  purrr::walk(c("test.mtcars", "__mtcars", "__mtcars_historical", "test.mtcars_modified", "mtcars_modified",
                "test.SCDB_logs", "test.SCDB_tmp1", "test.SCDB_tmp2", "test.SCDB_tmp3",
                "test.SCDB_t0", "test.SCDB_t1", "test.SCDB_t2"),
              ~ if (DBI::dbExistsTable(conn, id(., conn))) DBI::dbRemoveTable(conn, id(., conn)))

  purrr::walk(c(DBI::Id(schema = "test", table = "one.two"), DBI::Id(schema = "test.one", table = "two")),
              ~ if (schema_exists(conn, .@name[["schema"]]) && DBI::dbExistsTable(conn, .)) DBI::dbRemoveTable(conn, .))

  connection_clean_up(conn)
}
