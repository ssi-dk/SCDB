#' Get a list of data base connections to test on
#' @param skip_backends (`character()`)\cr
#'   List of connection types to not return connections for.
#' @return
#'   If you run your tests locally, it returns a list of connections corresponding to conn_list and conn_args
#'   If you run your tests on GitHub, it return a list of connection corresponding to the environment variables.
#'   i.e. the GitHub workflows will configure the testing back ends
#' @importFrom rlang `:=`
#' @noRd
get_test_conns <- function(skip_backends = NULL) {

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
    conn_list <- tibble::lst(!!Sys.getenv("BACKEND") := !!Sys.getenv("BACKEND_DRV"))

    # Use the connection configured by the remote
    conn_args <- tibble::lst(!!Sys.getenv("BACKEND") := Sys.getenv("BACKEND_ARGS"))
    conn_args <- purrr::discard(conn_args, ~ identical(., ""))
    conn_args <- purrr::map(conn_args, ~ eval(parse(text = .)))

    # Use the connection configured by the remote
    conn_post_connect <- tibble::lst(!!Sys.getenv("BACKEND") := Sys.getenv("BACKEND_POST_CONNECT"))
    conn_post_connect <- purrr::discard(conn_post_connect, ~ identical(., ""))
    conn_post_connect <- purrr::map(conn_post_connect, ~ eval(parse(text = .)))

  }

  # Early return if no connections are defined
  if (length(conn_list) == 0) {
    return(list())
  }

  # Parse any conn_args stored in CONN_ARGS_JSON
  conn_args_json <- jsonlite::fromJSON(Sys.getenv("CONN_ARGS_JSON", unset = "{}"))

  # Combine all arguments
  backends <- unique(c(names(conn_list), names(conn_args), names(conn_args_json)))
  conn_args <- purrr::map(backends, ~ c(purrr::pluck(conn_args, .), purrr::pluck(conn_args_json, .)))
  names(conn_args) <- backends


  get_driver <- function(x = character(), ...) {                                                                        # nolint: object_usage_linter
    if (!grepl(".*::.*", x)) {
      stop(
        "Package must be specified with namespace (e.g. RSQLite::SQLite)!\n",
        "Received: ",
        x,
        call. = FALSE
      )
    }
    parts <- strsplit(x, "::", fixed = TRUE)[[1]]

    # Skip unavailable packages
    if (!rlang::is_installed(parts[1])) {
      message("Library ", parts[1], " not available!")
      return(NULL)
    }

    return(getExportedValue(parts[1], parts[2])())
  }

  # Check all conn_args have associated entry in conn_list
  checkmate::assert_subset(names(conn_args), names(conn_list))

  # Open connections
  drivers <- purrr::map(names(conn_list), ~ do.call(get_driver, list(x = purrr::pluck(conn_list, .))))
  names(drivers) <- names(conn_list)
  drivers <- purrr::discard(drivers, is.null)

  test_conn_args <- purrr::map(
    names(drivers),
    ~ c(list("drv" = purrr::pluck(drivers, .)), purrr::pluck(conn_args, .))
  )

  test_conns <- purrr::map(
    test_conn_args,
    ~ do.call(SCDB::get_connection, args = .)
  )
  names(test_conns) <- names(drivers)
  test_conns <- purrr::discard(test_conns, is.null)

  # Skip backends if given
  test_conns <- purrr::walk(
    test_conns,
    ~ {
      if (checkmate::test_multi_class(., purrr::pluck(skip_backends, .default = ""))) {
        DBI::dbDisconnect(.)
      }
    }
  )
  test_conns <- purrr::discard(
    test_conns,
    ~ checkmate::test_multi_class(., purrr::pluck(skip_backends, .default = ""))
  )

  # Run post_connect commands on the connections
  purrr::iwalk(
    test_conns,
    function(conn, conn_name) {
      purrr::walk(purrr::pluck(conn_post_connect, conn_name), ~ DBI::dbExecute(conn, .))
    }
  )

  # Inform the user about the tested back ends:
  msg <- paste(sep = "\n",
    "#####",
    "Following backends will be tested:",
    paste("  ", names(test_conns), collapse = "\n"),
    "####"
  )

  # Message the user only once within this session
  rlang::inform(
    message = msg,
    .frequency = "once",
    .frequency_id = msg
  )

  return(test_conns)
}


#' Parse checkmate assertions for testthat compatibility
#' @description
#'   The error messages generated by `checkmate` are formatted to look nicely in the console by the
#'   addition of `*` and `\n` characters.
#'
#'   This means that checking these errors with `testthat::expect_error()` will often fail or will be harder to read
#'   in the test since we need to manually insert `*` and `\n` to the comparison pattern to match the error message.
#'
#'   This helper function intercepts the `checkmate` error message and removes the `*` and `\n` characters to allow for
#'   human readable error checking.
#' @return
#'   The checkmate error without `*` and `\n` characters.
#' @noRd
checkmate_err_msg <- function(expr) {
  tryCatch(
    expr,
    error = function(e) {
      msg <- e$message
      msg <- stringr::str_remove_all(msg, stringr::fixed("\n *"))
      msg <- stringr::str_remove_all(msg, stringr::fixed("* "))

      stop(simpleError(message = msg))                                                                                  # nolint: condition_call_linter
    }
  )
}
