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

test_that("filter_keys() works", {
  for (conn in get_test_conns()) {

    x <- get_table(conn, "__mtcars")

    expect_equal(x,
                 x |> filter_keys(NULL))

    filter <- x |> utils::head(10) |> dplyr::select(name)
    expect_equal(x |>
                   dplyr::filter(name %in% !!dplyr::pull(filter, name)) |>
                   dplyr::collect(),
                 x |>
                   filter_keys(filter) |>
                   dplyr::collect())

    filter <- x |> utils::head(10) |> dplyr::select(vs, am) |> dplyr::distinct()
    expect_equal(x |>
                   dplyr::inner_join(filter, by = c("vs", "am")) |>
                   dplyr::collect(),
                 x |>
                   filter_keys(filter) |>
                   dplyr::collect())

    # Filtering with null means no filtering is done
    m <- mtcars
    row.names(m) <- NULL
    filter <- NULL
    expect_identical(filter_keys(m, filter), m)

    # Filtering by vs = 0
    filter <- data.frame(vs = 0)
    expect_mapequal(filter_keys(m, filter), dplyr::filter(m, vs == 0))

    DBI::dbDisconnect(conn)
  }
})
