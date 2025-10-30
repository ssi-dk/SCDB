test_that("`db_timestamp()` produces consistent results", {
  for (conn in get_test_conns()) {
    ts_posix <- Sys.time()
    ts_str <- format(ts_posix)

    expect_identical(
      db_timestamp(ts_posix, conn),
      db_timestamp(ts_str, conn)
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
    "Integer Date" = as.Date(as.integer(Sys.Date())),
    "POSIXct" = lubridate::floor_date(Sys.time(), unit = "day"),
    "character timestamp" = format(lubridate::floor_date(Sys.time(), unit = "day"))
  )

  for (conn in get_test_conns()) {

    # Get all combinations and generate SQL
    test_results <- tidyr::expand_grid(
      type_1 = slice_tss,
      type_2 = slice_tss
    ) |>
      purrr::pmap_lgl(
        ~ DBI::dbGetQuery(
          conn,
          DBI::SQL(glue::glue("SELECT {db_timestamp(..1, conn)} = {db_timestamp(..2, conn)}"))
        )[[1]]
      )

    labels <- tidyr::expand_grid(
      type_1 = names(slice_tss),
      type_2 = names(slice_tss)
    ) |>
      purrr::pmap_chr(~ glue::glue("{.x} / {.y}"))

    # All db_timestamps should map to the same value
    expect_identical(
      data.frame(
        classes = labels,
        results = test_results
      ),
      data.frame(
        classes = labels,
        results = rep(TRUE, length(labels))
      )
    )

    connection_clean_up(conn)
  }
})