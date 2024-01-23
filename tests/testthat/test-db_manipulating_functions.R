test_that("interlace_sql() works", {
  for (conn in get_test_conns()) {

    t1 <- data.frame(key = c("A", "A", "B"),
                     obs_1   = c(1, 2, 2),
                     valid_from  = as.Date(c("2021-01-01", "2021-02-01", "2021-01-01")),
                     valid_until = as.Date(c("2021-02-01", "2021-03-01", NA)))


    t2 <- data.frame(key = c("A", "B"),
                     obs_2 = c("a", "b"),
                     valid_from  = as.Date(c("2021-01-01", "2021-01-01")),
                     valid_until = as.Date(c("2021-04-01", NA)))


    t_ref <- data.frame(key = c("A", "A", "A", "B"),
                        obs_1   = c(1, 2, NA, 2),
                        obs_2   = c("a", "a", "a", "b"),
                        valid_from  = as.Date(c("2021-01-01", "2021-02-01", "2021-03-01", "2021-01-01")),
                        valid_until = as.Date(c("2021-02-01", "2021-03-01", "2021-04-01", NA)))


    # Copy t1, t2 and t_ref to conn (and suppress check_from message)
    t1 <- suppressMessages(
      dplyr::copy_to(conn, t1, name = id("test.SCDB_tmp1", conn), overwrite = TRUE, temporary = FALSE)
    )

    t2 <- suppressMessages(
      dplyr::copy_to(conn, t2, name = id("test.SCDB_tmp2", conn), overwrite = TRUE, temporary = FALSE)
    )

    t_ref <- suppressMessages(
      dplyr::copy_to(conn, t_ref, name = id("test.SCDB_tmp3", conn), overwrite = TRUE, temporary = FALSE)
    )


    expect_identical(interlace_sql(list(t1, t2), by = "key") |> dplyr::collect(),
                     t_ref |> dplyr::collect())

    expect_mapequal(interlace_sql(list(t1, t2), by = "key") |> dplyr::collect(),
                    interlace_sql(list(t2, t1), by = "key") |> dplyr::collect())

    DBI::dbDisconnect(conn)
  }
})


test_that("interlace_sql returns early if length(table) == 1", {
  expect_identical(mtcars$mpg, interlace_sql(mtcars["mpg"], by = "mpg"))
})


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
