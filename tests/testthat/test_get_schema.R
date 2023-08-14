test_that("schema_exists() works", { for (conn in conns) {
  if (inherits(conn, "SQLiteConnection")) next

  expect_true(schema_exists(conn, "test"))

  random_string <- paste(sample(c(letters, LETTERS), size = 16, replace = T), collapse = "")
  expect_false(schema_exists(conn, random_string))
}})
