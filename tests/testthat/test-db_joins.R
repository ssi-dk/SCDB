test_that("*_join() works", { for (conn in conns) { # nolint: brace_linter

  # Define two test datasets
  x <- get_table(conn, "__mtcars") |>
    dplyr::select(name, mpg, cyl, hp, vs, am, gear, carb)

  y <- get_table(conn, "__mtcars") |>
    dplyr::select(name, drat, wt, qsec)


  # Test the implemented joins
  q  <- left_join(x, y, by = "name") |> dplyr::collect()
  qr <- dplyr::left_join(dplyr::collect(x), dplyr::collect(y), by = "name")
  expect_equal(q, qr)

  q <- right_join(x, y, by = "name") |> dplyr::collect()
  qr <- dplyr::right_join(dplyr::collect(x), dplyr::collect(y), by = "name")
  expect_equal(q, qr)

  q <- inner_join(x, y, by = "name") |> dplyr::collect()
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


  q  <- left_join(x, y, na_by = "number") |>
    dplyr::collect() |>
    dplyr::arrange(number, t, letter)
  qr <- dplyr::left_join(dplyr::collect(x), dplyr::collect(y),  by = "number", multiple = "all") |>
    dplyr::arrange(number, t, letter)
  expect_mapequal(q, qr)

  q  <- right_join(x, y, na_by = "number") |>
    dplyr::collect() |>
    dplyr::arrange(number, t, letter)
  qr <- dplyr::right_join(dplyr::collect(x), dplyr::collect(y), by = "number", multiple = "all") |>
    dplyr::arrange(number, t, letter)
  expect_equal(q, qr)

  q  <- full_join(x, y, na_by = "number") |>
    dplyr::collect() |>
    dplyr::arrange(number, t, letter)
  qr <- dplyr::full_join(dplyr::collect(x), dplyr::collect(y),  by = "number", multiple = "all") |>
    dplyr::arrange(number, t, letter)
  expect_equal(q, qr)

  if (inherits(conn, "SQLiteConnection")) {
    # If this test fails, it means dbplyr fixed the apparent bug in right_join
    # This also means that our right_join should no longer be an S3 class and can revert to our regular implementation
    sql_on <- join_na_sql(by = NULL, na_by = "number")
    renamer <- select_na_sql(x, y, by = NULL, na_by = "number", left = FALSE)

    expect_false(identical(dplyr::collect(scdb_right_join.tbl_SQLiteConnection(x, y, sql_on, renamer)),
                           dplyr::collect(scdb_right_join.tbl_dbi(x, y, sql_on, renamer))))
  }


  # Second test case
  x <- data.frame(date = as.Date(c("2022-05-01", "2022-05-01", "2022-05-02", "2022-05-02")),
                  region_id = c("1", NA, NA, "1"),
                  n_start = c(3, NA, NA, NA)) %>%
    dplyr::copy_to(conn, ., name = id("test.SCDB_tmp1", conn), overwrite =  TRUE, temporary = FALSE)
  y <- data.frame(date = as.Date("2022-05-02"),
                  region_id = "1",
                  n_add = 4) %>%
    dplyr::copy_to(conn, ., name = id("test.SCDB_tmp2", conn), overwrite =  TRUE, temporary = FALSE)

  q  <- full_join(x, y, by = "date", na_by = "region_id") |>
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
  expect_equal(left_join(xx, xx, by    = "name") |>
                 dplyr::summarize(n = sum(dplyr::if_else(is.na(cyl.y), 1, 0), na.rm = TRUE)) |>
                 dplyr::pull(n), 1)
  expect_equal(left_join(xx, xx, na_by = "name") |>
                 dplyr::summarize(n = sum(dplyr::if_else(is.na(cyl.y), 1, 0), na.rm = TRUE)) |>
                 dplyr::pull(n), 0)

  # And they should be identical with the simple case
  expect_equal(left_join(xx, xx, na_by = "name") |>
                 dplyr::select(!"name") |>
                 dplyr::collect(),
               left_join(x,  x,  na_by = "name") |>
                 dplyr::select(!"name") |>
                 dplyr::collect())


  # left_join should be no slower than dplyr::left_join (otherwise, just deprecate it)
  xx <- get_table(conn, "__mtcars") |>
    dplyr::mutate(vs = dplyr::if_else(vs == 0, NA, vs),
                  am = dplyr::if_else(am == 0, NA, am)) |>
    dplyr::compute()
  xx <- purrr::map(seq(100), ~ xx) |> purrr::reduce(dplyr::union_all) |> dplyr::compute()
  t1 <- system.time(q1 <- dplyr::compute(left_join(xx, xx,
                                                   by = c("gear", "cyl"),
                                                   na_by = c("am", "vs"))))
  t2 <- system.time(q2 <- dplyr::compute(dplyr::left_join(xx, xx,
                                                          by = c("gear", "cyl", "am", "vs"),
                                                          na_matches = "na")))

  expect_equal(nrow(q1), nrow(q2))

  if (!inherits(conn, "SQLiteConnection")) {
    expect_true(t1[["elapsed"]] <= t2[["elapsed"]])
  }
}})
