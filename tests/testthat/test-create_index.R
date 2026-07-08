test_that("create_index() quotes column identifiers", {
  weird_columns <- tibble::tibble(
    `space col` = c("a", "b"),
    `select` = c("x", "y"),
    `punctuation-col!` = c(1L, 2L)
  )

  duplicate_row <- weird_columns[1, , drop = FALSE]

  for (conn in get_test_conns()) {
    db_table <- id(unique_table_name("SCDB_create_index"), conn)

    remote_table <- dplyr::copy_to(
      dest = conn,
      df = weird_columns,
      name = db_table,
      temporary = FALSE,
      overwrite = TRUE
    )

    defer_db_cleanup(remote_table)

    expect_error(
      create_index(
        conn = conn,
        db_table = db_table,
        columns = c("space col", "select", "punctuation-col!")
      ),
      regexp = NA
    )

    expect_error(
      DBI::dbAppendTable(conn, db_table, duplicate_row),
      regexp = ".+"
    )

    expect_identical(
      nrow(dplyr::tbl(conn, db_table)),
      2L
    )

    connection_clean_up(conn)
  }
})
