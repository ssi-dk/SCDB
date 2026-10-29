root <- devtools::package_file()
if (!file.exists(file.path(root, "DESCRIPTION")) ||
      !file.exists(file.path(root, "tests", "testthat", "helper-setup.R"))) {
  stop("Run this script from the package root.", call. = FALSE)
}

# Install packages and dependencies
pak::pkg_install(c("callr", "microbenchmark", "jsonlite"), ask = FALSE)

branch <- system("git symbolic-ref --short HEAD", intern = TRUE)
git_sha <- system("git rev-parse HEAD", intern = TRUE)

# Store the default lib dir
lib_paths_default <- .libPaths()

versions <- c("CRAN", "main", switch(branch != "main", "branch"))

for (version in versions) {

  source <- dplyr::case_when(
    version == "CRAN" ~ "SCDB",
    version == "main" ~ "ssi-dk/SCDB",
    version == "branch" ~ glue::glue("ssi-dk/SCDB@{git_sha}")
  )

  lib_dir <- dplyr::case_when(
    version == "CRAN" ~ "SCDB",
    version == "main" ~ "ssi-dk-SCDB",
    version == "branch" ~ glue::glue("ssi-dk-SCDB-{git_sha}")
  )

  lib_path <- here::here("installations", lib_dir)
  dir.create(lib_path, showWarnings = FALSE)

  # Install SCDB and its required dependencies
  message("Installing ", version, ": ", source)
  pak::pkg_install(source, lib = lib_path, dependencies = NA, ask = FALSE)

}


# Return early on CI if there is no configured backend.
if (identical(Sys.getenv("CI"), "true") && !nzchar(Sys.getenv("BACKEND"))) {
  message("No BACKEND configured in CI; skipping benchmarks.")
} else {
  for (version in versions) {
    source <- dplyr::case_when(
      version == "CRAN" ~ "SCDB",
      version == "main" ~ "ssi-dk/SCDB",
      version == "branch" ~ glue::glue("ssi-dk/SCDB@{git_sha}")
    )

    lib_dir <- dplyr::case_when(
      version == "CRAN" ~ "SCDB",
      version == "main" ~ "ssi-dk-SCDB",
      version == "branch" ~ glue::glue("ssi-dk-SCDB-{git_sha}")
    )

    lib_path <- here::here("installations", lib_dir)

    callr::r(
      args = list(
        version = version,
        root = root,
        lib_path = lib_path,
        git_sha = git_sha
      ),
      libpath = c(lib_path, lib_paths_default),
      user_profile = FALSE,
      system_profile = FALSE,
      show = TRUE,
      func = function(version, root, lib_path, git_sha) {

        # Each call runs in a fresh R process
        options(odbc.batch_rows = 1000)
        loadNamespace("SCDB", lib.loc = lib_path)

        loaded_path <- normalizePath(getNamespaceInfo("SCDB", "path"), winslash = "/")
        expected_path <- normalizePath(file.path(lib_path, "SCDB"), winslash = "/")
        if (!identical(loaded_path, expected_path)) {
          stop("Loaded SCDB from the wrong library: ", loaded_path, call. = FALSE)
        }

        # Open connection to the database
        source(file.path(root, "tests", "testthat", "helper-setup.R"), local = TRUE)
        conns <- suppressMessages(get_test_conns())
        conn <- conns[[1]]
        backend <- names(conns)[[1]]

        # Our benchmark data is the iris data set but repeated to increase the data size
        data_generator <- function(repeats) {
          purrr::map(
            seq(repeats),
            \(it) dplyr::mutate(iris, r = dplyr::row_number() + (it - 1) * nrow(iris))
          ) |>
            purrr::reduce(rbind) |>
            dplyr::rename_with(~ tolower(gsub(".", "_", .x, fixed = TRUE)))
        }

        package_version <- as.character(utils::packageVersion("SCDB", lib.loc = lib_path))

        label <- switch(
          version,
          CRAN = paste0("SCDB v", package_version),
          main = "main",
          branch = git_sha
        )

        # Keep the original fields consumed by the benchmark vignette.
        add_metadata <- function(result, benchmark_function, n) {
          result$benchmark_function <- benchmark_function
          result$n <- n
          result$database <- backend
          result$version <- label
          result$package_version <- package_version
          result$r_version <- as.character(getRversion())
          result$run_at <- Sys.time()
          return(result)
        }

        message("Benchmarking ", label, " on ", backend)

        # Benchmark 1: update_snapshot() with consecutive updates
        n <- 10
        data_1 <- data_generator(n)
        data_2 <- data_generator(2 * n) |>
          dplyr::mutate(
            "sepal_length" = dplyr::if_else(
              .data$sepal_length > stats::median(.data$sepal_length),
              .data$sepal_length,
              .data$sepal_length / 2
            )
          )
        data_3 <- data_generator(3 * n) |>
          dplyr::mutate(
            "sepal_length" = dplyr::if_else(
              .data$sepal_length > stats::median(.data$sepal_length),
              .data$sepal_length,
              .data$sepal_length / 2
            ),
            "sepal_width" = dplyr::if_else(
              .data$sepal_width > stats::median(.data$sepal_width),
              .data$sepal_width,
              .data$sepal_width / 2
            )
          )

        # Copy data to the conns
        data_on_conn <- list(
          dplyr::copy_to(conn, data_1, name = SCDB::id("SCDB_data_1", conn), overwrite = TRUE, temporary = FALSE),
          dplyr::copy_to(conn, data_2, name = SCDB::id("SCDB_data_2", conn), overwrite = TRUE, temporary = FALSE),
          dplyr::copy_to(conn, data_3, name = SCDB::id("SCDB_data_3", conn), overwrite = TRUE, temporary = FALSE)
        )

        remove_if_exists <- function(table) {
          if (DBI::dbExistsTable(conn, table)) DBI::dbRemoveTable(conn, table)
        }

        # Define the data to loop over for benchmark
        timestamps <- c("2021-01-01", "2021-01-02", "2021-01-03")

        run_updates <- function() {
          for (i in seq_along(data_on_conn)) {
            SCDB::update_snapshot(
              .data = data_on_conn[[i]],
              conn = conn,
              db_table = "SCDB_benchmark_1",
              timestamp = timestamps[[i]],
              logger = SCDB::Logger$new(output_to_console = FALSE, warn = FALSE)
            )
          }
          return(invisible(NULL))
        }

        # Construct the list of benchmarks
        update_snapshot_benchmark <- microbenchmark::microbenchmark(
          run_updates(),
          times = 25,
          setup = remove_if_exists("SCDB_benchmark_1") # Don't time the clenaup
        ) |>
          add_metadata(benchmark_function = "update_snapshot()", n = n)
        DBI::dbRemoveTable(conn, name = "SCDB_benchmark_1")


        output_dir <- file.path(root, "inst", "extdata")
        dir.create(output_dir, recursive = TRUE, showWarnings = FALSE)

        saveRDS(
          update_snapshot_benchmark,
          file.path(output_dir, paste0("benchmark-update_snapshot_", backend, "_", version, ".rds"))
        )
        message("  Completed benchmark 1")

        # Benchmark 2: initial snapshot at the original five sizes (5 runs each).
        for (n in floor(10^(seq(1, 3, length.out = 5)))) {
          data <- dplyr::copy_to(
            conn, data_generator(n), name = SCDB::id("SCDB_data", conn),
            overwrite = TRUE, temporary = FALSE
          )

          run_one_update <- function() {
            SCDB::update_snapshot(
              data, conn, "SCDB_benchmark_2",
              timestamp = "2021-01-01",
              logger = SCDB::Logger$new(output_to_console = FALSE, warn = FALSE)
            )
            invisible(NULL)
          }

          update_snapshot_complexity_benchmark <- microbenchmark::microbenchmark(
            run_one_update(),
            times = 5,
            setup = remove_if_exists("SCDB_benchmark_2") # Don't time the clenaup
          ) |>
            add_metadata("update_snapshot() - complexity", n = n)
          DBI::dbRemoveTable(conn, name = "SCDB_benchmark_2")

          saveRDS(
            update_snapshot_complexity_benchmark,
            file.path(
              output_dir,
              paste0("benchmark-update_snapshot_complexity_", n, "_", backend, "_", version, ".rds")
            )
          )
          message("  Completed benchmark 2 for n = ", n)
        }
      }
    )
  }
}
