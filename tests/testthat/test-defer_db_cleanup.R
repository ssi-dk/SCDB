test_that("`defer_db_cleanup()` deletes tables", {
  for (conn in get_test_conns()) {
    table <- dplyr::copy_to(conn, mtcars, "__mtcars_defer_db_cleanup")
    table_id <- id(table, conn)
    stop(table_id)

    # Table exists
    expect_true(DBI::dbExistsTable(conn, "__mtcars_defer_db_cleanup"))
    expect_true(DBI::dbExistsTable(conn, table_id))

    # Marking for deletion does not delete the table
    defer_db_cleanup(table_id)
    expect_true(DBI::dbExistsTable(conn, table_id))

    # Manually triggering deletion deletes the table
    withr::deferred_run()
    expect_false(DBI::dbExistsTable(conn, table_id))

    connection_clean_up(conn)
  }
})
