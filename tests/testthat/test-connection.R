test_that("get_connection() works", {
  for (conn in get_test_conns()) {
    expect_true(DBI::dbIsValid(conn))

    connection_clean_up(conn)
  }
})


test_that("get_connection() notifies if connection fails", {
  for (i in 1:100) {
    random_string <- paste(sample(letters, size = 32, replace = TRUE), collapse = "")

    if (dir.exists(random_string)) next

    expect_error(get_connection(drv = RSQLite::SQLite(), dbname = file.path(random_string, "/invalid_path")),
                 regexp = "Could not connect to database:\nunable to open database file")
  }
})


test_that("close_connection() works", {
  for (conn in get_test_conns()) {

    # Check that we can close the connection
    expect_true(DBI::dbIsValid(conn))
    close_connection(conn)
    expect_false(DBI::dbIsValid(conn))
  }
})
