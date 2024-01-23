test_that("slice_time() works", {
  for (conn in get_test_conns()) {

    # SQLite does not work with dates. But since we use ISO 8601 for dates, we can compare lexicographically
    xx <- get_table(conn, "__mtcars") |>
      dplyr::mutate(checksum = dplyr::row_number(),
                    from_ts = dplyr::if_else(checksum <= 20, "2022-06-01", "2022-06-15"),
                    until_ts = NA_character_)

    expect_equal(xx |> slice_time("2022-05-01") |> nrow(), 0)
    expect_equal(xx |> slice_time("2022-06-01") |> nrow(), 20)
    expect_equal(xx |> slice_time("2022-06-15") |> nrow(), nrow(mtcars))

    DBI::dbDisconnect(conn)
  }
})
