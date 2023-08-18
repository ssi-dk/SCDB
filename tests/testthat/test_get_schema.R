test_that("schema_exists() works", { for (conn in conns) { # nolint: brace_linter
  if (inherits(conn, "SQLiteConnection")) {
    expect_false(schema_exists(conn, "test"))
    next
  }
  expect_true(schema_exists(conn, "test"))

  random_string <- paste(sample(c(letters, LETTERS), size = 16, replace = TRUE), collapse = "")
  expect_false(schema_exists(conn, random_string))
}})
