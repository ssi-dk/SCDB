test_that("focus_lineage_map() works", {

  pangolin_data <- get_table(conn, "mg.lineage_info")

  lin_map <- focus_lineage_map(pangolin_data, c("BQ.1.1")) |>
    filter(lineage %in% c("BQ.1.1", "BQ.1.1.1", "BQ.1.10")) |>
    collect() |>
    arrange(lineage)

  ref <- tibble(lineage = c("BQ.1.1", "BQ.1.1.1", "BQ.1.10"),
                focus_lineage = c("BQ.1.1", "BQ.1.1", NA),
                variant = c("omicron", "omicron", "omicron")) |>
    arrange(lineage)

  expect_equal(lin_map, ref)
})
