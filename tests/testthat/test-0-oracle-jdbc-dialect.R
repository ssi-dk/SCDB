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

test_that("Oracle/JDBC copy_to preserves column names in the database", {
  for (conn in get_test_conns()) {
    skip_if_not(
      inherits(conn, "JDBCConnection"),
      message = "Only relevant for Oracle/JDBC connections"
    )

    table_name <- unique_table_name("SCDB_column_case")

    table <- dplyr::copy_to(
      conn,
      mtcars,
      name = table_name,
      temporary = FALSE,
      analyze = FALSE
    )

    table_id <- id(table, conn)

    columns <- DBI::dbGetQuery(
      conn,
      paste(
        "SELECT column_name",
        "FROM all_tab_columns",
        "WHERE owner =",
        as.character(DBI::dbQuoteString(conn, purrr::pluck(table_id, "name", "schema"))),
        "AND table_name =",
        as.character(DBI::dbQuoteString(conn, purrr::pluck(table_id, "name", "table"))),
        "ORDER BY column_id"
      )
    )

    names(columns) <- tolower(names(columns))

    expect_equal(columns[["column_name"]], names(mtcars))
    expect_named(dplyr::collect(table), names(mtcars))

    DBI::dbRemoveTable(conn, table_id)
    connection_clean_up(conn)
  }
})
