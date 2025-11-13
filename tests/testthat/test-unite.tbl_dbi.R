test_that("unite.tbl_dbi() works", {
  for (conn in get_test_conns()) {

    q <- get_table(conn, "__mtcars") %>% utils::head(1)
    qu_remove <- tidyr::unite(dplyr::select(q, mpg, hp), "new_column", mpg, hp) %>%
      dplyr::compute(name = unique_table_name())
    qu        <- tidyr::unite(dplyr::select(q, mpg, hp), "new_column", mpg, hp, remove = FALSE) %>%
      dplyr::compute(name = unique_table_name())
    qu_alt    <- tidyr::unite(dplyr::select(q, mpg, hp), "new_column", "mpg", "hp", remove = FALSE) %>%
      dplyr::compute(name = unique_table_name())

    expect_s3_class(qu_remove, "tbl_dbi")
    expect_s3_class(qu,        "tbl_dbi")
    expect_s3_class(qu_alt,    "tbl_dbi")

    expect_identical(colnames(qu_remove), "new_column")
    expect_identical(colnames(qu),     c("new_column", "mpg", "hp"))
    expect_identical(colnames(qu_alt), c("new_column", "mpg", "hp"))

    expect_identical(dplyr::collect(qu), dplyr::collect(qu_alt))

    # tidyr::unite has some quirky (and FUN!!! behavior) that we are forced to match here
    # specifically, the input "col" is converted to a symbol, so we have to do escape-bullshit
    # NOTE: the line "dplyr::mutate(dplyr::across(tidyselect::everything(), as.character)) %>% "
    # is to account for SQLite not having integer data-types. If we do not first convert to character,
    # there will be differences between the objects that are trivial, so we remove these with this operation
    # this way, the test should (hopefully) only fail if there are non-trivial differences
    expect_mapequal(get_table(conn, "__mtcars") %>%
                      tidyr::unite("new_col", mpg, hp) %>%
                      dplyr::mutate(dplyr::across(tidyselect::everything(), as.character)) %>%
                      dplyr::collect(),
                    get_table(conn, "__mtcars") %>%
                      dplyr::mutate(dplyr::across(tidyselect::everything(), as.character)) %>%
                      dplyr::collect() %>%
                      tidyr::unite("new_col", mpg, hp))

    col <- "new_col"
    expect_mapequal(get_table(conn, "__mtcars") %>%
                      tidyr::unite(col, mpg, hp) %>%
                      dplyr::mutate(dplyr::across(tidyselect::everything(), as.character)) %>%
                      dplyr::collect(),
                    get_table(conn, "__mtcars") %>%
                      dplyr::mutate(dplyr::across(tidyselect::everything(), as.character)) %>%
                      dplyr::collect() %>%
                      tidyr::unite(col, mpg, hp))

    expect_mapequal(get_table(conn, "__mtcars") %>%
                      tidyr::unite(!!col, mpg, hp) %>%
                      dplyr::mutate(dplyr::across(tidyselect::everything(), as.character)) %>%
                      dplyr::collect(),
                    get_table(conn, "__mtcars") %>%
                      dplyr::mutate(dplyr::across(tidyselect::everything(), as.character)) %>%
                      dplyr::collect() %>%
                      tidyr::unite(!!col, mpg, hp))

    # Unite places cols in a particular way, lets be sure we match
    qq <- dplyr::mutate(q, dplyr::across(tidyselect::everything(), as.character)) # we convert to character since SQLite
    expect_identical(qq %>% tidyr::unite("test_col", vs, am) %>% dplyr::collect(),
                     qq %>% dplyr::collect() %>% tidyr::unite("test_col", vs, am))

    connection_clean_up(conn)
  }
})
