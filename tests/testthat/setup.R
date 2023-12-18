# dplyr has a deprecated function called "id" that in some cases are called over the SCDB implementation if "id"
# We here explicitly prefer our implementation for our tests
conflicted::conflict_prefer("id", "SCDB")

# Define list of connections to check
conn_list <- list(
  # Backend string = package::function
  "SQLite"     = "RSQLite::SQLite",
  "PostgreSQL" = "RPostgres::Postgres",
  "MSSQL"      = "odbc::odbc"
)

get_driver <- function(x = character(), ...) {
  if (!grepl(".*::.*", x)) stop("Package must be specified with namespace (e.g. RSQLite::SQLite)!\n",
                                "Received: ", x)
  parts <- strsplit(x, "::")[[1]]

  # Skip unavailable packages
  if (!requireNamespace(parts[1], quietly = TRUE)) {
    return()
  }

  drv <- getExportedValue(parts[1], parts[2])

  tryCatch(suppressWarnings(get_connection(drv = drv(), ...)),  # We expect a warning if no tables are found
           error = function(e) {
             NULL # Return NULL, if we cannot connect
           })
}

conns <- lapply(conn_list, get_driver) |>
  unlist()

# Access any arguments to
odbc_json <- Sys.getenv("SCDB_ODBC_JSON")
if (odbc_json != "" && rlang::is_installed("jsonlite")) {
  conns <- jsonlite::fromJSON(odbc_json) |>
    lapply(\(.x) {
      .x$drv = odbc::odbc()

      return(do.call(DBI::dbConnect, args = .x))
    }) |>
    append(conns, x = _)
}

if (length(conns[names(conns) != "SQLite"]) == 0) {
  message("No useful drivers (other than SQLite) were found!")
}

message("#####\n",
        "Following drivers will be tested:\n",
        paste0(sprintf("  %s (%s)",
                       conn_list[names(conns)],
                       names(conns)), collapse = "\n"))

unavailable_drv <-
  conn_list[which(!names(conn_list) %in% names(conns))]
if (length(unavailable_drv) > 0) {
  message(
    "\n",
    "Following drivers were not found and will NOT be tested:\n",
    paste0(sprintf(
      "  %s (%s)",
      conn_list[names(unavailable_drv)],
      names(unavailable_drv)
    ), collapse = "\n")
  )
}
message("#####")

# Attach tempfile as schema for SQLite
for (conn in conns) {
  if (!inherits(conn, "SQLiteConnection")) next
  DBI::dbExecute(conn, paste0("ATTACH '", tempfile(), "' AS 'test'"))
}

# Start with some clean up
for (conn in conns) {
  if (!schema_exists(conn, "test")) {
    stop("Tests require the schema 'test' to exist in all available connections")
  }

  purrr::walk(c("test.mtcars", "__mtcars",
                "test.scdb_logs", "test.scdb_tmp1", "test.scdb_tmp2", "test.SCDB_tmp3",
                "test.SCDB_t0", "test.SCDB_t1", "test.SCDB_t2"),
              ~ if (DBI::dbExistsTable(conn, id(., conn))) DBI::dbRemoveTable(conn, id(., conn)))


  # Copy mtcars to conn
  dplyr::copy_to(conn, mtcars |> dplyr::mutate(name = rownames(mtcars)),
                 name = id("test.mtcars", conn), temporary = FALSE, overwrite = TRUE)
  dplyr::copy_to(conn, mtcars |> dplyr::mutate(name = rownames(mtcars)),
                 name = id("__mtcars", conn),    temporary = FALSE, overwrite = TRUE)

  dplyr::copy_to(conn,
                 mtcars |>
                   dplyr::mutate(name = rownames(mtcars)) |>
                   digest_to_checksum() |>
                   dplyr::mutate(from_ts = as.POSIXct("2020-01-01 09:00:00"),
                                 until_ts = as.POSIXct(NA)),
                 name = id("__mtcars_historical", conn),    temporary = FALSE, overwrite = TRUE)
}
