test_that("is.historical() works", {
  for (conn in get_test_conns()) {

    expect_true(is.historical(dplyr::tbl(conn, id("__mtcars_historical", conn))))
    expect_false(is.historical(dplyr::tbl(conn, id("__mtcars", conn))))

    connection_clean_up(conn)
  }
})
