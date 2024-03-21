withr::local_options("odbc.batch_rows" = 1000)

# Load the connection helper
source("tests/testthat/helper-setup.R")

# Install all needed package versions
for (version in c("CRAN", "main", "branch")) {
  branch <- system("git symbolic-ref --short HEAD", intern = TRUE)
  if (version == "branch" && branch == "main") {
    next
  }

  source <- dplyr::case_when(
    version == "CRAN" ~ "SCDB",
    version == "main" ~ "ssi-dk/SCDB",
    version == "branch" ~ glue::glue("ssi-dk/SCDB@{branch}")
  )

  pak::pkg_install(source, lib = glue::glue("SCDB_installation/{source}"))
}

# Then loop over each and benchmark the update_snapshot function
for (version in c("CRAN", "main", "branch")) {
  branch <- system("git symbolic-ref --short HEAD", intern = TRUE)
  if (version == "branch" && branch == "main") {
    next
  }

  source <- dplyr::case_when(
    version == "CRAN" ~ "SCDB",
    version == "main" ~ "ssi-dk/SCDB",
    version == "branch" ~ glue::glue("ssi-dk/SCDB@{branch}")
  )

  library("SCDB", lib.loc = glue::glue("SCDB_installation/{source}"))

  try({
    # Our benchmark data is the iris data set but repeated to increase the data size
    data_generator <- function(repeats) {
      purrr::map(
        seq(repeats),
        \(it) dplyr::mutate(iris, r = dplyr::row_number() + (it - 1) * nrow(iris))
      ) |>
        purrr::reduce(rbind) |>
        dplyr::rename_with(~ tolower(gsub(".", "_", .x, fixed = TRUE)))
    }

    # Open connection to the database
    conns <- get_test_conns()
    conn <- conns[[1]]

    n <- ifelse(names(conns)[1] == "SQLite", 5, 10)
    data_1 <- data_generator(n)
    data_2 <- data_generator(2 * n) |>
      dplyr::mutate(
        "sepal_length" = dplyr::if_else(
          .data$sepal_length > median(.data$sepal_length),
          .data$sepal_length,
          .data$sepal_length / 2
        )
      )
    data_3 <- data_generator(3 * n) |>
      dplyr::mutate(
        "sepal_length" = dplyr::if_else(
          .data$sepal_length > median(.data$sepal_length),
          .data$sepal_length,
          .data$sepal_length / 2
        ),
        "sepal_width" = dplyr::if_else(
          .data$sepal_width > median(.data$sepal_width),
          .data$sepal_width,
          .data$sepal_width / 2
        )
      )

    # Copy data to the conns
    data_on_conn <- list(
      suppressMessages(
        dplyr::copy_to(conn, data_1, name = id("test.SCDB_data_1", conn), overwrite = TRUE, temporary = FALSE)
      ),
      suppressMessages(
        dplyr::copy_to(conn, data_2, name = id("test.SCDB_data_2", conn), overwrite = TRUE, temporary = FALSE)
      ),
      suppressMessages(
        dplyr::copy_to(conn, data_3, name = id("test.SCDB_data_3", conn), overwrite = TRUE, temporary = FALSE)
      )
    )

    # Define the data to loop over for benchmark
    ts <- list("2021-01-01", "2021-01-02", "2021-01-03")

    # Define the SCDB update functions
    scdb_update_step <- function(conn, data, ts) {
      update_snapshot(data, conn, "SCDB_benchmark", timestamp = ts,
                      logger = Logger$new(output_to_console = FALSE, warn = FALSE))
    }

    scdb_updates <- function(conn, data_on_conn) {
      purrr::walk2(data_on_conn, ts, \(data, ts) scdb_update_step(conn, data, ts))
      DBI::dbRemoveTable(conn, name = "SCDB_benchmark")
    }

    # Construct the list of benchmarks
    update_snapshot_benchmark <- microbenchmark::microbenchmark(scdb_updates(conn, data_on_conn), times = 25) |>
      dplyr::mutate(
        "benchmark_function" = "update_snapshot",
        "database" = names(conns)[[1]],
        "version" = !!ifelse(version == "branch", branch, version)
      )

    dir.create("data", showWarnings = FALSE)
    saveRDS(update_snapshot_benchmark, glue::glue("data/benchmark-update_snapshot_{names(conns)[[1]]}_{version}.rds"))

    # Clean up
    purrr::walk(conns, ~ DBI::dbDisconnect(., shutdown = TRUE))
  })

  detach("package:SCDB", unload = TRUE)
}
