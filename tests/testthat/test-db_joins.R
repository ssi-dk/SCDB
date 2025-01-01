test_that("*_join() works with character `by` and `na_by`", {
  for (conn in get_test_conns()) {

    # Create two more synthetic test data set with NA data

    # First test case
    x <- data.frame(number = c("1", "2", NA),
                    t = c("strA", NA, "strB"))

    y <- data.frame(letter = c("A", "B", "A", "B"),
                    number = c(NA, "2", "1", "1"))

    # Copy x and y to conn
    x <- dplyr::copy_to(conn, x, name = id("test.SCDB_tmp1", conn), overwrite = TRUE, temporary = FALSE)

    y <- dplyr::copy_to(conn, y, name = id("test.SCDB_tmp2", conn), overwrite = TRUE, temporary = FALSE)


    q  <- dplyr::left_join(x, y, na_by = "number") %>%
      dplyr::collect() %>%
      dplyr::arrange(number, t, letter)
    qr <- dplyr::left_join(dplyr::collect(x), dplyr::collect(y),  by = "number", multiple = "all") %>%
      dplyr::arrange(number, t, letter)
    expect_mapequal(q, qr)

    q  <- dplyr::right_join(x, y, na_by = "number") %>%
      dplyr::collect() %>%
      dplyr::arrange(number, t, letter)
    qr <- dplyr::right_join(dplyr::collect(x), dplyr::collect(y), by = "number", multiple = "all") %>%
      dplyr::arrange(number, t, letter)
    expect_equal(q, qr)

    q  <- dplyr::inner_join(x, y, na_by = "number") %>%
      dplyr::collect() %>%
      dplyr::arrange(number, t, letter)
    qr <- dplyr::full_join(dplyr::collect(x), dplyr::collect(y),  by = "number", multiple = "all") %>%
      dplyr::arrange(number, t, letter)
    expect_equal(q, qr)


    # Second test case
    x <- data.frame(date = as.Date(c("2022-05-01", "2022-05-01", "2022-05-02", "2022-05-02")),
                    region_id = c("1", NA, NA, "1"),
                    n_start = c(3, NA, NA, NA))

    y <- data.frame(date = as.Date("2022-05-02"),
                    region_id = "1",
                    n_add = 4)

    # Copy x and y to conn
    x <- dplyr::copy_to(conn, x, name = id("test.SCDB_tmp1", conn), overwrite = TRUE, temporary = FALSE)

    y <- dplyr::copy_to(conn, y, name = id("test.SCDB_tmp2", conn), overwrite = TRUE, temporary = FALSE)


    q  <- dplyr::full_join(x, y, by = "date", na_by = "region_id") %>%
      dplyr::collect() %>%
      dplyr::arrange(date, region_id)
    qr <- dplyr::full_join(dplyr::collect(x), dplyr::collect(y), by = c("date", "region_id")) %>%
      dplyr::arrange(date, region_id)
    expect_equal(q, qr)




    # Some other test cases
    x <- get_table(conn, "__mtcars") %>%
      dplyr::select(name, mpg, cyl, hp, vs, am, gear, carb)

    y <- get_table(conn, "__mtcars") %>%
      dplyr::select(name, drat, wt, qsec)

    xx <- x %>% dplyr::mutate(name = dplyr::if_else(dplyr::row_number() == 1, NA, name))
    yy <- y %>% dplyr::mutate(name = dplyr::if_else(dplyr::row_number() == 1, NA, name))

    # Using by should give 1 mismatch
    # Using na_by should give no mismatch
    expect_equal(
      dplyr::left_join(xx, xx, by = "name") %>%
        dplyr::summarize(n = sum(dplyr::if_else(is.na(cyl.y), 1, 0), na.rm = TRUE)) %>%                                 # nolint: redundant_ifelse_linter
        dplyr::pull(n),
      1
    )
    expect_equal(
      dplyr::left_join(xx, xx, na_by = "name") %>%
        dplyr::summarize(n = sum(dplyr::if_else(is.na(cyl.y), 1, 0), na.rm = TRUE)) %>%                                 # nolint: redundant_ifelse_linter
        dplyr::pull(n),
      0
    )

    # And they should be identical with the simple case
    expect_equal(dplyr::left_join(xx, xx, na_by = "name") %>%
                   dplyr::select(!"name") %>%
                   dplyr::collect(),
                 dplyr::left_join(x,  x,  na_by = "name") %>%
                   dplyr::select(!"name") %>%
                   dplyr::collect())

    connection_clean_up(conn)
  }
})


test_that("*_join() works with `dplyr::join_by()`", {
  for (conn in get_test_conns()) {

    # Define two test datasets
    x <- get_table(conn, "__mtcars") %>%
      dplyr::select(name, mpg, cyl, hp, vs, am, gear, carb)

    y <- get_table(conn, "__mtcars") %>%
      dplyr::select(name, drat, wt, qsec)


    # Test the implemented joins
    q  <- dplyr::left_join(x, y, by = dplyr::join_by(x$name == y$name)) %>% dplyr::collect()
    qr <- dplyr::left_join(dplyr::collect(x), dplyr::collect(y), by = dplyr::join_by(x$name == y$name))
    expect_equal(q, qr)

    q  <- dplyr::right_join(x, y, by = dplyr::join_by(x$name == y$name)) %>% dplyr::collect()
    qr <- dplyr::right_join(dplyr::collect(x), dplyr::collect(y), by = dplyr::join_by(x$name == y$name))
    expect_equal(q, qr)

    q  <- dplyr::inner_join(x, y, by = dplyr::join_by(x$name == y$name)) %>% dplyr::collect()
    qr <- dplyr::inner_join(dplyr::collect(x), dplyr::collect(y), by = dplyr::join_by(x$name == y$name))
    expect_equal(q, qr)

    connection_clean_up(conn)
  }
})


test_that("*_join() does not break any dplyr joins", {
  for (conn in get_test_conns()) {

    # Define two test datasets
    x <- get_table(conn, "__mtcars") %>%
      dplyr::select(name, mpg, cyl, hp, vs, am, gear, carb)

    y <- get_table(conn, "__mtcars") %>%
      dplyr::select(name, drat, wt, qsec)

    # Test the standard joins
    # left_join
    qr <- dplyr::left_join(dplyr::collect(x), dplyr::collect(y), by = "name")
    q  <- dplyr::left_join(x, y, by = "name") %>% dplyr::collect()
    expect_equal(q, qr)

    q <- dplyr::left_join(x, y, by = dplyr::join_by(x$name == y$name)) %>% dplyr::collect()
    expect_equal(q, qr)

    # right_join
    qr <- dplyr::right_join(dplyr::collect(x), dplyr::collect(y), by = "name")
    q  <- dplyr::right_join(x, y, by = "name") %>% dplyr::collect()
    expect_equal(q, qr)

    q <- dplyr::right_join(x, y, by = dplyr::join_by(x$name == y$name)) %>% dplyr::collect()
    expect_equal(q, qr)

    # inner_join
    qr <- dplyr::inner_join(dplyr::collect(x), dplyr::collect(y), by = "name")
    q  <- dplyr::inner_join(x, y, by = "name") %>% dplyr::collect()
    expect_equal(q, qr)

    q <- dplyr::inner_join(x, y, by = dplyr::join_by(x$name == y$name)) %>% dplyr::collect()
    expect_equal(q, qr)

    # full_join
    qr <- dplyr::full_join(dplyr::collect(x), dplyr::collect(y), by = "name")
    q  <- dplyr::full_join(x, y, by = "name") %>% dplyr::collect()
    expect_equal(q, qr)

    q <- dplyr::full_join(x, y, by = dplyr::join_by(x$name == y$name)) %>% dplyr::collect()
    expect_equal(q, qr)

    # semi_join
    qr <- dplyr::semi_join(dplyr::collect(x), dplyr::collect(y), by = "name")
    q <- dplyr::semi_join(x, y, by = "name") %>% dplyr::collect()
    expect_equal(q, qr)

    q <- dplyr::semi_join(x, y, by = dplyr::join_by(x$name == y$name)) %>% dplyr::collect()
    expect_equal(q, qr)

    # anti_join
    qr <- dplyr::anti_join(dplyr::collect(x), dplyr::collect(y), by = "name")
    q <- dplyr::anti_join(x, y, by = "name") %>% dplyr::collect()
    expect_equal(q, qr)

    q <- dplyr::anti_join(x, y, by = dplyr::join_by(x$name == y$name)) %>% dplyr::collect()
    expect_equal(q, qr)

    connection_clean_up(conn)
  }
})
