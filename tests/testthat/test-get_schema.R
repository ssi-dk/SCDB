test_that("schema_exists() works", {
  for (conn in get_test_conns()) {

    expect_true(schema_exists(conn, "test"))

    random_string <- paste(sample(c(letters, LETTERS), size = 16, replace = TRUE), collapse = "")
    expect_false(schema_exists(conn, random_string))

    DBI::dbDisconnect(conn)
  }
})
