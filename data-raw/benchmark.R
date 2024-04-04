withr::local_options("odbc.batch_rows" = 1000)

# Install extra dependencies
lib_paths_default <- .libPaths()
pak::pkg_install("jsonlite")
pak::pkg_install("microbenchmark")
pak::pkg_install("here")

# Load the connection helper
source("tests/testthat/helper-setup.R")

# Install all needed package versions
for (version in c("CRAN", "main", "branch")) {
  branch <- system("git symbolic-ref --short HEAD", intern = TRUE)
  sha <- system("git rev-parse HEAD", intern = TRUE)
  if (version == "branch" && branch == "main") {
    next
  }

  source <- dplyr::case_when(
    version == "CRAN" ~ "SCDB",
    version == "main" ~ "ssi-dk/SCDB",
    version == "branch" ~ glue::glue("ssi-dk/SCDB@{sha}")
  )

  lib_dir <- dplyr::case_when(
    version == "CRAN" ~ "SCDB",
    version == "main" ~ "ssi-dk-SCDB",
    version == "branch" ~ glue::glue("ssi-dk-SCDB-{sha}")
  )

  lib_path <- here::here("installations", lib_dir)
  dir.create(lib_path, showWarnings = FALSE)

  # Install the missing packages
  .libPaths(c(lib_path, lib_paths_default))
  pak::lockfile_create(source, "SCDB.lock")
  missing <- jsonlite::fromJSON("SCDB.lock")$packages$ref |>
    purrr::discard(rlang::is_installed)
  if (length(missing) > 0) pak::pkg_install(missing, lib = lib_path)
}


# Return early if no back-end is defined
if (identical(Sys.getenv("CI"), "true") && identical(Sys.getenv("BACKEND"), "")) {
  message("No backend defined, skipping benchmark!")
} else {

  # Then loop over each and benchmark the update_snapshot function
  for (version in c("CRAN", "main", "branch")) {
    branch <- system("git symbolic-ref --short HEAD", intern = TRUE)
    sha <- system("git rev-parse HEAD", intern = TRUE)
    if (version == "branch" && branch == "main") {
      next
    }

    source <- dplyr::case_when(
      version == "CRAN" ~ "SCDB",
      version == "main" ~ "ssi-dk/SCDB",
      version == "branch" ~ glue::glue("ssi-dk/SCDB@{sha}")
    )

    lib_dir <- dplyr::case_when(
      version == "CRAN" ~ "SCDB",
      version == "main" ~ "ssi-dk-SCDB",
      version == "branch" ~ glue::glue("ssi-dk-SCDB-{sha}")
    )

    .libPaths(c(here::here("installations", lib_dir), lib_paths_default))
    library("SCDB")

    # Open connection to the database
    conns <- get_test_conns()
    conn <- conns[[1]]


    # Our benchmark data is the iris data set but repeated to increase the data size
    data_generator <- function(repeats) {
      purrr::map(
        seq(repeats),
        \(it) dplyr::mutate(iris, r = dplyr::row_number() + (it - 1) * nrow(iris))
      ) |>
        purrr::reduce(rbind) |>
        dplyr::rename_with(~ tolower(gsub(".", "_", .x, fixed = TRUE)))
    }

    # Benchmark 1, update_snapshot() with consecutive updates
    try({
      n <- 10
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
        dplyr::copy_to(conn, data_1, name = id("test.SCDB_data_1", conn), overwrite = TRUE, temporary = FALSE),
        dplyr::copy_to(conn, data_2, name = id("test.SCDB_data_2", conn), overwrite = TRUE, temporary = FALSE),
        dplyr::copy_to(conn, data_3, name = id("test.SCDB_data_3", conn), overwrite = TRUE, temporary = FALSE)
      )

      # Define the data to loop over for benchmark
      ts <- list("2021-01-01", "2021-01-02", "2021-01-03")

      # Define the SCDB update functions
      scdb_update_step <- function(conn, data, ts) {
        update_snapshot(data, conn, "SCDB_benchmark_1", timestamp = ts,
                        logger = Logger$new(output_to_console = FALSE, warn = FALSE))
      }

      scdb_updates <- function(conn, data_on_conn) {
        purrr::walk2(data_on_conn, ts, \(data, ts) scdb_update_step(conn, data, ts))
        DBI::dbRemoveTable(conn, name = "SCDB_benchmark_1")
      }

      # Construct the list of benchmarks
      update_snapshot_benchmark <- microbenchmark::microbenchmark(scdb_updates(conn, data_on_conn), times = 25) |>
        dplyr::mutate(
          "benchmark_function" = "update_snapshot()",
          "database" = names(conns)[[1]],
          "version" = !!ifelse(version == "branch", substr(sha, 1, 10), version),
          "n" = n
        )

      dir.create("data", showWarnings = FALSE)
      saveRDS(update_snapshot_benchmark, glue::glue("data/benchmark-update_snapshot_{names(conns)[[1]]}_{version}.rds"))
    })

    # Benchmark 2, update_snapshot() with increasing data size
    try({
      for (n in floor(10^(seq(1, 3, length.out = 5)))) {

        data <- data_generator(n) |>
          dplyr::copy_to(conn, df = _, name = id("test.SCDB_data", conn), overwrite = TRUE, temporary = FALSE)

        # Define the SCDB update function
        scdb_updates <- function(conn, data) {
          update_snapshot(data, conn, "SCDB_benchmark_2", timestamp = "2021-01-01",
                          logger = Logger$new(output_to_console = FALSE, warn = FALSE))
          DBI::dbRemoveTable(conn, name = "SCDB_benchmark_2")
        }

        # Construct the list of benchmarks
        update_snapshot_benchmark <- microbenchmark::microbenchmark(scdb_updates(conn, data), times = 5) |>
          dplyr::mutate(
            "benchmark_function" = "update_snapshot() - complexity",
            "database" = names(conns)[[1]],
            "version" = !!ifelse(version == "branch", substr(sha, 1, 10), version),
            "n" = n
          )

        dir.create("data", showWarnings = FALSE)
        saveRDS(
          update_snapshot_benchmark,
          glue::glue("data/benchmark-update_snapshot_complexity_{n}_{names(conns)[[1]]}_{version}.rds")
        )
      }

      # Clean up
      purrr::walk(conns, DBI::dbDisconnect)
    })

    detach("package:SCDB", unload = TRUE)
  }
}
