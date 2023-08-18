# Define list of connections to check
conn_list <- list(
  # Backend string = package::function
  "SQLite"     = "RSQLite::SQLite",
  "PostgreSQL" = "RPostgres::Postgres"
)

get_driver <- function(x) {
  if (!grepl(".*::.*", x)) stop("Package must be specified with namespace (e.g. RSQLite::SQLite)!\n",
                                "Received: ", x)
  parts <- strsplit(x, "::")[[1]]

  if (!exists("pkgs")) pkgs <<- installed.packages()[,"Package"]

  # Skip unavailable packages
  if (!parts[1] %in% pkgs) {
    return(NULL)
  }

  drv <- getExportedValue(parts[1], parts[2])

  tryCatch(suppressWarnings(  # We expect a warning if no tables are found
            get_connection(drv = drv())),
           error = function(e) {
             NULL # Return NULL, if we cannot connect
           })
}

conns <- lapply(conn_list, get_driver) |>
  unlist()

if (length(conns[names(conns) != "SQLite"]) == 0) {
  message("No useful drivers (other than SQLite) were found!")
}

{
  cat("#####\n",
      "Following drivers will be tested:\n",
      sprintf("  %s (%s)\n", conn_list[names(conns)], names(conns)),
      sep = ""
  )
  unavailable_drv <- conn_list[which(!names(conn_list) %in% names(conns))]
  if (length(unavailable_drv) > 0) {
    cat("\nFollowing drivers were not found and will NOT be tested:\n",
      sprintf("  %s (%s)\n", conn_list[names(unavailable_drv)], names(unavailable_drv)),
      sep = ""
  )
  }
  cat("#####\n")
}

# Start with some clean up
for (conn in conns) {
if (!inherits(conn, "SQLiteConnection") && !schema_exists(conn, "test")){
  stop("Tests require the schema 'test' to exist in all available connections (except SQLite)")
}

purrr::walk(c("test.mtcars", "__mtcars",
              "test.mg_logs", "test.mg_tmp1", "test.mg_tmp2", "test.mg_tmp3",
              "test.mg_t0", "test.mg_t1", "test.mg_t2"),
            ~ if (DBI::dbExistsTable(conn, id(., conn))) DBI::dbRemoveTable(conn, id(., conn)))


# Copy mtcars to conn
dplyr::copy_to(conn, mtcars |> dplyr::mutate(name = rownames(mtcars)),
               name = id("test.mtcars", conn), temporary = FALSE, overwrite = TRUE)
dplyr::copy_to(conn, mtcars |> dplyr::mutate(name = rownames(mtcars)),
               name = id("__mtcars", conn),    temporary = FALSE, overwrite = TRUE)
}
