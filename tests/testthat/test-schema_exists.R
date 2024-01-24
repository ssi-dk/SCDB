test_that("schema_exists() works", {
  conns <- get_test_conns()
  for (conn_id in seq_along(conns)) {

    conn <- conns[[conn_id]]

    # Not all data bases support schemas.
    # Here we filter out the data bases that do not support schema
    # NOTE: SQLite does support schema, but we test both with and without attaching schemas
    if (names(conns)[[conn_id]] != "SQLite") {
      expect_true(schema_exists(conn, "test"))

      random_string <- paste(sample(c(letters, LETTERS), size = 16, replace = TRUE), collapse = "")
      expect_false(schema_exists(conn, random_string))
    }

    connection_clean_up(conn)
  }
})
