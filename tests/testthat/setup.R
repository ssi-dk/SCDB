# dplyr has a deprecated function called "id" that in some cases are called over the SCDB implementation if "id"
# We here explicitly prefer our implementation for our tests
conflicted::conflict_prefer("id", "SCDB")

#' Get a list of data base connections to test on
#' @return
#'   If you run your tests locally, it returns a list of connections corresponding to conn_list and conn_args
#'   If you run your tests on GitHub, it return a list of connection corresponding to the environment variables.
#'   i.e. the GitHub workflows will configure the testing back ends
#' @importFrom rlang `:=`
#' @noRd
get_test_conns <- function() {

  # Locally use rlang's (without this, it may not be bound)
  `:=` <- rlang::`:=`

  # Check if we run remotely
  running_locally <- !identical(Sys.getenv("CI"), "true")

  # Define list of connections to check
  if (running_locally) {

    # Define our local connection backends
    conn_list <- list(
      # Backend string = package::function
      "SQLite"              = "RSQLite::SQLite",
      "SQLite - w. schemas" = "RSQLite::SQLite"
    )

    # Define our local connection arguments
    conn_args <- list(
      # Backend string = list(named args)
      "SQLite"              = list(dbname = file.path(tempdir(), "SQLite.SQLite")),
      "SQLite - w. schemas" = list(dbname = file.path(tempdir(), "SQLite_schemas.SQLite"))
    )

    # Define post connection commands to run
    conn_post_connect <- list(
      # Backend string = list(named args)
      "SQLite - w. schemas" = list(
        paste0("ATTACH '", file.path(tempdir(), "SQLite_test.SQLite"), "' AS 'test'"),
        paste0("ATTACH '", file.path(tempdir(), "SQLite_test_one.SQLite"), "' AS 'test.one'")
      )
    )

  } else {

    # Use the connection configured by the remote
    conn_list <- tibble::lst(!!Sys.getenv("BACKEND") := !!Sys.getenv("BACKEND_DRV"))                                    # nolint: object_name_linter

    # Use the connection configured by the remote
    conn_args <- tibble::lst(!!Sys.getenv("BACKEND") := Sys.getenv("BACKEND_ARGS")) |>                                  # nolint: object_name_linter
      purrr::discard(~ identical(., "")) |>
      purrr::map(~ eval(parse(text = .)))

    # Use the connection configured by the remote
    conn_post_connect <- tibble::lst(!!Sys.getenv("BACKEND") := Sys.getenv("BACKEND_POST_CONNECT")) |>                  # nolint: object_name_linter
      purrr::discard(~ identical(., "")) |>
      purrr::map(~ eval(parse(text = .)))

  }

  # Parse any conn_args stored in CONN_ARGS_JSON
  conn_args_json <- jsonlite::fromJSON(Sys.getenv("CONN_ARGS_JSON", unset = "{}"))

  # Combine all arguments
  backends <- unique(c(names(conn_list), names(conn_args), names(conn_args_json)))
  conn_args <- backends |>
    purrr::map(~ c(purrr::pluck(conn_args, .), purrr::pluck(conn_args_json, .))) |>
    stats::setNames(backends)


  get_driver <- function(x = character(), ...) {                                                                        # nolint: object_usage_linter
    if (!grepl(".*::.*", x)) stop("Package must be specified with namespace (e.g. RSQLite::SQLite)!\n",
                                  "Received: ", x)
    parts <- strsplit(x, "::", fixed = TRUE)[[1]]

    # Skip unavailable packages
    if (!requireNamespace(parts[1], quietly = TRUE)) {
      return()
    }

    drv <- getExportedValue(parts[1], parts[2])

    conn <- tryCatch(
      SCDB::get_connection(drv = drv(), ...),
      error = function(e) {
        return(NULL) # Return NULL, if we cannot connect
      }
    )

    # SQLite back end gives an error in SCDB if there are no tables (it assumes a bad configuration)
    # We create a table to suppress this warning
    if (checkmate::test_class(conn, "SQLiteConnection")) {
      DBI::dbWriteTable(conn, "iris", iris, overwrite = TRUE)
    }

    return(conn)
  }

  # Check all conn_args have associated entry in conn_list
  checkmate::assert_subset(names(conn_args), names(conn_list))

  test_conns <- names(conn_list) |>
    purrr::map(~ do.call(get_driver, c(list(x = purrr::pluck(conn_list, .)), purrr::pluck(conn_args, .)))) |>
    stats::setNames(names(conn_list))

  purrr::walk2(test_conns, names(conn_list),
               \(conn, conn_name) purrr::walk(purrr::pluck(conn_post_connect, conn_name), ~ DBI::dbExecute(conn, .)))

  return(test_conns)
}


# Ensure the target connections are empty and configured correctly
coll <- checkmate::makeAssertCollection()
conns <- get_test_conns()
for (conn_id in seq_along(conns)) {

  conn <- conns[[conn_id]]

  # Ensure connections are valid
  if (is.null(conn) || !DBI::dbIsValid(conn)) {
    coll$push(glue::glue("Connection could not be made to backend ({names(conns)[[conn_id]]})."))
  }


  # Check schemas are configured correctly
  if (!schema_exists(conn, "test") && names(conns)[conn_id] != "SQLite") {
    coll$push(glue::glue("Tests require the schema 'test' to exist in connection ({names(conns)[[conn_id]]})."))
  }

  if (!schema_exists(conn, "test.one") && names(conns)[conn_id] != "SQLite") {
    coll$push(glue::glue("Tests require the schema 'test.one' to exist in connection ({names(conns)[[conn_id]]})."))
  }
}
checkmate::reportAssertions(coll)


# Configure the data bases
for (conn in get_test_conns()) {

  # Start with some clean up
  purrr::walk(c("test.mtcars", "__mtcars", "__mtcars_historical", "test.mtcars_modified", "mtcars_modified",
                "test.SCDB_logs", "test.SCDB_tmp1", "test.SCDB_tmp2", "test.SCDB_tmp3",
                "test.SCDB_t0", "test.SCDB_t1", "test.SCDB_t2"),
              ~ if (DBI::dbExistsTable(conn, id(., conn))) DBI::dbRemoveTable(conn, id(., conn)))

  purrr::walk(c(DBI::Id(schema = "test", table = "one.two"), DBI::Id(schema = "test.one", table = "two")),
              ~ if (schema_exists(conn, .@name[["schema"]]) && DBI::dbExistsTable(conn, .)) DBI::dbRemoveTable(conn, .))


  # Copy mtcars to conn (and suppress check_from message)
  tryCatch(
    dplyr::copy_to(conn, mtcars |> dplyr::mutate(name = rownames(mtcars)),
                   name = id("test.mtcars", conn), temporary = FALSE, overwrite = TRUE),
    message = \(m) if (!stringr::str_detect(m$message, "check_from = FALSE")) message(m)
  )

  dplyr::copy_to(conn, mtcars |> dplyr::mutate(name = rownames(mtcars)),
                 name = id("__mtcars", conn), temporary = FALSE, overwrite = TRUE)

  dplyr::copy_to(conn,
                 mtcars |>
                   dplyr::mutate(name = rownames(mtcars)) |>
                   digest_to_checksum() |>
                   dplyr::mutate(from_ts = as.POSIXct("2020-01-01 09:00:00"),
                                 until_ts = as.POSIXct(NA)),
                 name = id("__mtcars_historical", conn), temporary = FALSE, overwrite = TRUE)


  # Disconnect
  DBI::dbDisconnect(conn)
}


# Report testing environment to the user
message(
  "#####\n\n",
  "Following backends will be tested:",
  sep = ""
)

conns <- get_test_conns()
message(sprintf("  %s\n", names(conns)))
message("#####")

# Disconnect
purrr::walk(conns, DBI::dbDisconnect)
