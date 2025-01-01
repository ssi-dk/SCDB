test_that("filter_keys() works", {
  for (conn in get_test_conns()) {

    x <- get_table(conn, "__mtcars")

    expect_equal(x,
                 x %>% filter_keys(NULL))

    filter <- x %>% utils::head(10) %>% dplyr::select("name")
    expect_equal(x %>%
                   dplyr::filter(name %in% !!dplyr::pull(filter, "name")) %>%
                   dplyr::collect(),
                 x %>%
                   filter_keys(filter) %>%
                   dplyr::collect())

    filter <- x %>% utils::head(10) %>% dplyr::select("vs", "am") %>% dplyr::distinct()
    expect_equal(x %>%
                   dplyr::inner_join(filter, by = c("vs", "am")) %>%
                   dplyr::collect(),
                 x %>%
                   filter_keys(filter) %>%
                   dplyr::collect())

    # Filtering with null means no filtering is done
    m <- mtcars
    row.names(m) <- NULL
    filter <- NULL
    expect_identical(filter_keys(m, filter), m)

    # Filtering by vs = 0
    filter <- data.frame(vs = 0)
    expect_mapequal(filter_keys(m, filter), dplyr::filter(m, .data$vs == 0))

    connection_clean_up(conn)
  }
})


test_that("filter_keys() works with copy = TRUE", {
  for (conn in get_test_conns()) {

    x <- get_table(conn, "__mtcars")

    filter <- x %>%
      utils::head(10) %>%
      dplyr::select("name") %>%
      dplyr::collect()

    expect_equal(x %>%
                   dplyr::filter(.data$name %in% !!dplyr::pull(filter, "name")) %>%
                   dplyr::collect(),
                 x %>%
                   filter_keys(filter, copy = TRUE) %>%
                   dplyr::collect())

    # The above filter_keys with `copy = TRUE` generates a dbplyr_### table.
    # We manually remove this since we expect it. If more arrise, we will get an error.
    DBI::dbRemoveTable(conn, id(utils::head(get_tables(conn, "dbplyr_"), 1)))

    connection_clean_up(conn)
  }
})
