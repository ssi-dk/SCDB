test_that("get_connection() works", {
  for (conn in conns) expect_true(DBI::dbIsValid(conn))
})

test_that("get_connection notifies if connection fails", {
  for (i in 1:100) {
    random_string <- paste(sample(letters, size = 32, replace = TRUE), collapse = "")

    if (dir.exists(random_string)) next

    expect_error(get_connection(drv = RSQLite::SQLite(), dbname = paste0(random_string, "/invalid_path")),
                 regexp = "Could not connect to database:\nunable to open database file")
  }
})

test_that("get_connection warns about unsupported backend", {
  get_connection(drv = character(0)) |>
    expect_error(regex = paste("unable to find an inherited method for function",
                               "'dbCanConnect' for signature '\"character\"'")) |>
    expect_warning("Driver of class 'character' is currently not fully supported")
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
  conn_drv <- purrr::keep(conns, \(.x) identical(.x, conn)) |>
    names() |>
    (\(.) purrr::pluck(conn_list, .))()

  conn2 <- get_driver(conn_drv)

  expect_identical(class(conn), class(conn2))
  expect_true(DBI::dbIsValid(conn2))

  close_connection(conn2)

  expect_false(DBI::dbIsValid(conn2))
  rm(conn2)
}})
