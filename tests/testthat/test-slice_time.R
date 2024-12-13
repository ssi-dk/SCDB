test_that("slice_time() works", {
  for (conn in get_test_conns()) {

    # SQLite does not work with dates. But since we use ISO 8601 for dates, we can compare lexicographically
    xx <- get_table(conn, "__mtcars") |>
      dplyr::mutate(checksum = dplyr::row_number(),
                    from_ts = dplyr::if_else(checksum <= 20, "2022-06-01", "2022-06-15"),
                    until_ts = NA_character_)

    expect_identical(nrow(slice_time(xx, "2022-05-01")), 0)
    expect_identical(nrow(slice_time(xx, "2022-06-01")), 20)
    expect_identical(nrow(slice_time(xx, "2022-06-15")), nrow(mtcars))

    connection_clean_up(conn)
  }
})


test_that("slice_time() works with non-standard columns", {
  for (conn in get_test_conns()) {

    # SQLite does not work with dates. But since we use ISO 8601 for dates, we can compare lexicographically
    xx <- get_table(conn, "__mtcars") |>
      dplyr::mutate(checksum = dplyr::row_number(),
                    valid_from = dplyr::if_else(checksum <= 20, "2022-06-01", "2022-06-15"),
                    valid_until = NA_character_)

    expect_identical(nrow(slice_time(xx, "2022-05-01", from_ts = "valid_from", until_ts = "valid_until")), 0)
    expect_identical(nrow(slice_time(xx, "2022-06-01", from_ts = "valid_from", until_ts = "valid_until")), 20)
    expect_identical(nrow(slice_time(xx, "2022-06-15", from_ts = "valid_from", until_ts = "valid_until")), nrow(mtcars))

    connection_clean_up(conn)
  }
})
