test_that("get_connection() works", {
  for (conn in conns) expect_true(DBI::dbIsValid(conn))
})

test_that("id() works", { for (conn in conns) { # nolint: brace_linter
  expect_identical(id("test_mtcars"), DBI::Id(table = "test_mtcars"))
  expect_identical(id("test.mtcars"), DBI::Id(schema = "test", table = "mtcars"))

  if (inherits(conn, "SQLiteConnection")) {
    expect_identical(id("test.mtcars", conn), DBI::Id(table = "test.mtcars"))
  } else {
    expect_identical(id("test.mtcars", conn), DBI::Id(schema = "test", table = "mtcars"))
  }
}})

test_that("close_connection() works", { for (conn in conns) { # nolint: brace_linter
  # Identify driver of conn within conn_list
  conn_drv <- sapply(conns, \(.x) identical(.x, conn)) |>
    names() |>
    (\(.) purrr::pluck(conn_list, .))()

  conn2 <- get_driver(conn_drv)

  expect_identical(class(conn), class(conn2))
  expect_true(DBI::dbIsValid(conn2))

  close_connection(conn2)

  expect_false(DBI::dbIsValid(conn2))
  rm(conn2)
}})
