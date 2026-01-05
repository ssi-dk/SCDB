test_that("`db_timestamp()` produces consistent results", {
  for (conn in get_test_conns()) {
    ts_posix <- Sys.time()
    ts_str <- as.character(ts_posix)

    expect_identical(
      db_timestamp(ts_posix, conn),
      db_timestamp(ts_str, conn),
      info = glue::glue(
        "db_timestamp(ts_posix, conn) not equal to db_timestamp(ts_str, conn) on {class(conn)}"
      )
    )

    expect_identical(
      db_timestamp(ts_posix, conn = NULL),
      db_timestamp(ts_str, conn = NULL)
    )

    # Test default fallback
    expect_identical(
      db_timestamp.default(ts_posix, conn = conn),
      db_timestamp.default(ts_str, conn = conn)
    )

    connection_clean_up(conn)
  }
})


test_that("`db_timestamp()` maps identically for different inputs", {

  # Create list of slice_ts to test
  slice_tss <- list(
    "Date" = Sys.Date(),
    "character Date" = as.character(Sys.Date()),
    "Integer Date" = as.Date(as.integer(Sys.Date()), origin = "1970-01-01"),
    "POSIXct" = lubridate::floor_date(Sys.time(), unit = "day"),
    "character timestamp" = format(lubridate::floor_date(Sys.time(), unit = "day"))
  )

  for (conn in get_test_conns()) {

    # Get all combinations of slice_ts and generate SQL query to check for equivalence
    queries <- tidyr::expand_grid(
      type_1 = slice_tss,
      type_2 = slice_tss
    ) %>%
      purrr::pmap(
        ~ {
          DBI::SQL(
            paste0(
              "SELECT ",
              dbplyr::translate_sql(ifelse(!!db_timestamp(..1, conn) == !!db_timestamp(..2, conn), 1, 0), con = conn)   # nolint: redundant_ifelse_linter. We need to force SQL translation
            )
          )
        }
      )

    # Run queries
    test_results <- purrr::map_lgl(
      queries,
      function(query) {
        DBI::dbGetQuery(conn, query)[[1]]
      }
    )

    # Generate human-readable labels for each query for the test outputs
    labels <- tidyr::expand_grid(
      type_1 = names(slice_tss),
      type_2 = names(slice_tss)
    ) %>%
      purrr::pmap_chr(~ glue::glue("{.x} / {.y}"))

    # All db_timestamps should map to the same value
    expect_identical(
      data.frame(
        classes = labels[!test_results],
        query = as.character(queries[!test_results])
      ),
      data.frame(
        classes = character(0),
        query = character(0)
      )
    )

    connection_clean_up(conn)
  }
})
