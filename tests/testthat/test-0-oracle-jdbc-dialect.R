test_that("JDBCConnection uses the SCDB Oracle dbplyr dialect", {
  for (conn in get_test_conns()) {
    skip_if_not(
      inherits(conn, "JDBCConnection"),
      message = "Only relevant for Oracle/JDBC connections"
    )

    sql_dialect_method <- utils::getS3method(
      "sql_dialect",
      "JDBCConnection",
      optional = TRUE
    )

    expect_false(
      is.null(sql_dialect_method),
      info = "No S3 method registered for sql_dialect.JDBCConnection"
    )

    dialect <- dbplyr::sql_dialect(conn)

    expect_true(
      "sql_dialect_scdb_oracle_jdbc" %in% class(dialect),
      info = paste(
        "Expected sql_dialect_scdb_oracle_jdbc in dialect classes. Actual classes:",
        paste(class(dialect), collapse = ", ")
      )
    )
  }
})

test_that("JDBCConnection disables dbplyr table analysis", {
  for (conn in get_test_conns()) {
    skip_if_not(
      inherits(conn, "JDBCConnection"),
      message = "Only relevant for Oracle/JDBC connections"
    )

    analyze_method <- utils::getS3method(
      "sql_table_analyze",
      "sql_dialect_scdb_oracle_jdbc",
      optional = TRUE
    )

    expect_false(
      is.null(analyze_method),
      info = "No S3 method registered for sql_table_analyze.sql_dialect_scdb_oracle_jdbc"
    )

    analyze_sql <- dbplyr::sql_table_analyze(
      conn,
      id("test.SCDB_t0", conn)
    )

    expect_null(
      analyze_sql,
      info = paste(
        "Expected Oracle/JDBC table analysis to be disabled, but sql_table_analyze() returned:",
        paste(as.character(analyze_sql), collapse = "\n")
      )
    )
  }
})
