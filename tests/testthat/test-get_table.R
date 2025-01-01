test_that("get_table() returns list of tables if no table is requested", {
  for (conn in get_test_conns()) {

    expect_message(
      get_table(conn),
      regexp = "Select one of the following tables:"
    )

    connection_clean_up(conn)
  }
})


test_that("get_table() works when tables/view exist", {
  for (conn in get_test_conns()) {

    mtcars_t <- tibble::tibble(mtcars %>% dplyr::mutate(name = rownames(mtcars)))

    # Lets try different ways to read __mtcars (added during setup)
    expect_mapequal(get_table(conn, "__mtcars")  %>% dplyr::collect(), mtcars_t)
    expect_equal(get_table(conn, id("__mtcars")) %>% dplyr::collect(), mtcars_t)
    t <- "__mtcars"
    expect_equal(get_table(conn, t) %>% dplyr::collect(), mtcars_t)
    t <- id("__mtcars")
    expect_equal(get_table(conn, t) %>% dplyr::collect(), mtcars_t)

    # And test.mtcars (added during setup)
    expect_equal(get_table(conn, "test.mtcars") %>% dplyr::collect(), mtcars_t)
    expect_equal(get_table(conn, id("test.mtcars", conn)) %>% dplyr::collect(), mtcars_t)
    t <- "test.mtcars"
    expect_equal(get_table(conn, t) %>% dplyr::collect(), mtcars_t)
    t <- id("test.mtcars", conn)
    expect_equal(get_table(conn, t) %>% dplyr::collect(), mtcars_t)


    # Check for the existence of views on backends that support it (added here)
    if (checkmate::test_multi_class(conn, c("PqConnection", "Microsoft SQL Server"))) {

      if (inherits(conn, "PqConnection")) {
        DBI::dbExecute(conn, "CREATE VIEW __mtcars_view AS SELECT * FROM __mtcars LIMIT 10")
      } else if (inherits(conn, "Microsoft SQL Server")) {
        DBI::dbExecute(conn, "CREATE VIEW __mtcars_view AS SELECT TOP 10 * FROM __mtcars")
      }

      view_1 <- paste(c(get_schema(conn), "__mtcars_view"), collapse = ".")

      expect_identical(nrow(get_table(conn, view_1)), 10)
      expect_identical(
        dplyr::collect(get_table(conn, view_1)),
        dplyr::collect(utils::head(get_table(conn, "__mtcars"), 10))
      )

      DBI::dbExecute(conn, glue::glue("DROP VIEW {view_1}"))
    }


    connection_clean_up(conn)
  }
})


test_that("get_table() works when table does not exist in default schema", {
  for (conn in get_test_conns()) {

    # Generate table in default schema that does not exist
    k <- 0
    while (k < 100) {
      invalid_table_name <- paste(sample(letters, size = 16, replace = TRUE), collapse = "")
      k <- k + 1
      if (DBI::dbExistsTable(conn, id(invalid_table_name, conn))) next
      break
    }

    if (k < 100) {

      expect_error(
        get_table(conn, invalid_table_name),
        regexp = glue::glue("Table {as.character(id(invalid_table_name, conn))} could not be found!")
      )
      expect_error(
        get_table(conn, id(invalid_table_name, conn)),
        regexp = glue::glue("Table {as.character(id(invalid_table_name, conn))} could not be found!")
      )
      expect_error(
        get_table(conn, id(invalid_table_name)),
        regexp = glue::glue("Table {as.character(id(invalid_table_name, conn))} could not be found!")
      )

    } else {
      warning("Non-existing table in default schema could not be generated!")
    }

    connection_clean_up(conn)
  }
})


test_that("get_table() works when table does not exist in non-existing schema", {
  for (conn in get_test_conns()) {

    # Generate schema that does not exist
    k <- 0
    while (k < 100) {
      invalid_schema_name <- paste(sample(letters, size = 16, replace = TRUE), collapse = "")
      k <- k + 1
      if (schema_exists(conn, invalid_schema_name)) next
      break
    }

    if (k < 100) {

      # Test some malformed inputs
      invalid_table_name <- paste(invalid_schema_name, "mtcars", sep = ".")

      expect_error(
        get_table(conn, invalid_table_name),
        regexp = glue::glue("Table {as.character(id(invalid_table_name, conn))} could not be found!")
      )
      expect_error(
        get_table(conn, id(invalid_table_name, conn)),
        regexp = glue::glue("Table {as.character(id(invalid_table_name, conn))} could not be found!")
      )

    } else {
      warning("Non-existing schema could not be generated!")
    }

    connection_clean_up(conn)
  }
})
