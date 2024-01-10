test_that("*_join() works", {
  for (conn in get_test_conns()) {

    # Define two test datasets
    x <- get_table(conn, "__mtcars") |>
      dplyr::select(name, mpg, cyl, hp, vs, am, gear, carb)

    y <- get_table(conn, "__mtcars") |>
      dplyr::select(name, drat, wt, qsec)


    # Test the implemented joins
    q  <- dplyr::left_join(x, y, by = "name") |> dplyr::collect()
    qr <- dplyr::left_join(dplyr::collect(x), dplyr::collect(y), by = "name")
    expect_equal(q, qr)

    q <- dplyr::right_join(x, y, by = "name") |> dplyr::collect()
    qr <- dplyr::right_join(dplyr::collect(x), dplyr::collect(y), by = "name")
    expect_equal(q, qr)

    q <- dplyr::inner_join(x, y, by = "name") |> dplyr::collect()
    qr <- dplyr::inner_join(dplyr::collect(x), dplyr::collect(y), by = "name")
    expect_equal(q, qr)


    # Create two more synthetic test data set with NA data

    # First test case
    x <- data.frame(number = c("1", "2", NA),
                    t = c("strA", NA, "strB")) %>%
      dplyr::copy_to(conn, ., name = id("test.SCDB_tmp1", conn), overwrite =  TRUE, temporary = FALSE)

    y <- data.frame(letter = c("A", "B", "A", "B"),
                    number = c(NA, "2", "1", "1")) %>%
      dplyr::copy_to(conn, ., name = id("test.SCDB_tmp2", conn), overwrite = TRUE, temporary = FALSE)


    q  <- dplyr::left_join(x, y, na_by = "number") |>
      dplyr::collect() |>
      dplyr::arrange(number, t, letter)
    qr <- dplyr::left_join(dplyr::collect(x), dplyr::collect(y),  by = "number", multiple = "all") |>
      dplyr::arrange(number, t, letter)
    expect_mapequal(q, qr)

    q  <- dplyr::right_join(x, y, na_by = "number") |>
      dplyr::collect() |>
      dplyr::arrange(number, t, letter)
    qr <- dplyr::right_join(dplyr::collect(x), dplyr::collect(y), by = "number", multiple = "all") |>
      dplyr::arrange(number, t, letter)
    expect_equal(q, qr)

    q  <- dplyr::inner_join(x, y, na_by = "number") |>
      dplyr::collect() |>
      dplyr::arrange(number, t, letter)
    qr <- dplyr::full_join(dplyr::collect(x), dplyr::collect(y),  by = "number", multiple = "all") |>
      dplyr::arrange(number, t, letter)
    expect_equal(q, qr)


    # Second test case
    x <- data.frame(date = as.Date(c("2022-05-01", "2022-05-01", "2022-05-02", "2022-05-02")),
                    region_id = c("1", NA, NA, "1"),
                    n_start = c(3, NA, NA, NA)) %>%
      dplyr::copy_to(conn, ., name = id("test.SCDB_tmp1", conn), overwrite =  TRUE, temporary = FALSE)
    y <- data.frame(date = as.Date("2022-05-02"),
                    region_id = "1",
                    n_add = 4) %>%
      dplyr::copy_to(conn, ., name = id("test.SCDB_tmp2", conn), overwrite =  TRUE, temporary = FALSE)

    q  <- dplyr::full_join(x, y, by = "date", na_by = "region_id") |>
      dplyr::collect() |>
      dplyr::arrange(date, region_id)
    qr <- dplyr::full_join(dplyr::collect(x), dplyr::collect(y), by = c("date", "region_id")) |>
      dplyr::arrange(date, region_id)
    expect_equal(q, qr)




    # Some other test cases
    x <- get_table(conn, "__mtcars") |>
      dplyr::select(name, mpg, cyl, hp, vs, am, gear, carb)

    y <- get_table(conn, "__mtcars") |>
      dplyr::select(name, drat, wt, qsec)

    xx <- x |> dplyr::mutate(name = dplyr::if_else(dplyr::row_number() == 1, NA, name))
    yy <- y |> dplyr::mutate(name = dplyr::if_else(dplyr::row_number() == 1, NA, name))

    # Using by should give 1 mismatch
    # Using na_by should give no mismatch
    expect_equal(dplyr::left_join(xx, xx, by    = "name") |>
                  dplyr::summarize(n = sum(dplyr::if_else(is.na(cyl.y), 1, 0), na.rm = TRUE)) |>
                  dplyr::pull(n), 1)
    expect_equal(dplyr::left_join(xx, xx, na_by = "name") |>
                  dplyr::summarize(n = sum(dplyr::if_else(is.na(cyl.y), 1, 0), na.rm = TRUE)) |>
                  dplyr::pull(n), 0)

    # And they should be identical with the simple case
    expect_equal(dplyr::left_join(xx, xx, na_by = "name") |>
                  dplyr::select(!"name") |>
                  dplyr::collect(),
                dplyr::left_join(x,  x,  na_by = "name") |>
                  dplyr::select(!"name") |>
                  dplyr::collect())

    DBI::dbDisconnect(conn)
  }
})
