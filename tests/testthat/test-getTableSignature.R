# Generate test datasets with different data types

# One that follows the structure in update_snapshot()
data_update_snapsnot <- data.frame(
  "Date"      = Sys.Date(),
  "POSIXct"   = Sys.time(),
  "character" = "test",
  "integer"   = as.integer(1),
  "numeric"   = as.numeric(1),
  "logical"   = TRUE,
  # .. and our special columns
  "checksum"  = "test",
  "from_ts"   = Sys.time(),
  "until_ts"  = Sys.time()
)

# One that has the special columns of update_snapshot(), but not at the end
data_random <- data.frame(
  "Date"      = Sys.Date(),
  "POSIXct"   = Sys.time(),
  "character" = "test",
  # .. Our special columns, but not at the end
  "checksum"  = "test",
  "from_ts"   = Sys.time(),
  "until_ts"  = Sys.time(),
  # ..
  "integer"   = as.integer(1),
  "numeric"   = as.numeric(1),
  "logical"   = TRUE
)

for (conn in c(list(NULL), get_test_conns())) {                                                                            # nolint: cyclocomp_linter

  if (is.null(conn)) {
    test_that("getTableSignature() generates signature for update_snapshot() (conn == NULL)", {
      expect_identical(
        getTableSignature(data_update_snapsnot, conn),
        c(
          "Date"      = "Date",
          "POSIXct"   = "POSIXct",
          "character" = "character",
          "integer"   = "integer",
          "numeric"   = "numeric",
          "logical"   = "logical",
          # ..
          "checksum"  = "character",
          "from_ts"   = "POSIXct",
          "until_ts"  = "POSIXct"
        )
      )
    })
  }

  if (inherits(conn, "SQLiteConnection")) {
    test_that("getTableSignature() generates signature for update_snapshot() (SQLiteConnection)", {
      expect_identical(
        getTableSignature(data_update_snapsnot, conn),
        c(
          "Date"      = "DATE",
          "POSIXct"   = "TIMESTAMP",
          "character" = "TEXT",
          "integer"   = "INT",
          "numeric"   = "DOUBLE",
          "logical"   = "SMALLINT",
          # ..
          "checksum"  = "TEXT",
          "from_ts"   = "TIMESTAMP",
          "until_ts"  = "TIMESTAMP"
        )
      )
    })
  }

  if (inherits(conn, "PqConnection")) {
    test_that("getTableSignature() generates signature for update_snapshot() (PqConnection)", {
      expect_identical(
        getTableSignature(data_update_snapsnot, conn),
        c(
          "Date"      = "DATE",
          "POSIXct"   = "TIMESTAMPTZ",
          "character" = "TEXT",
          "integer"   = "INTEGER",
          "numeric"   = "DOUBLE PRECISION",
          "logical"   = "BOOLEAN",
          # ..
          "checksum"  = "VARCHAR(32)",
          "from_ts"   = "TIMESTAMP",
          "until_ts"  = "TIMESTAMP"
        )
      )
    })
  }

  if (inherits(conn, "Microsoft SQL Server")) {
    test_that("getTableSignature() generates signature for update_snapshot() (Microsoft SQL Server)", {
      expect_identical(
        getTableSignature(data_update_snapsnot, conn),
        c(
          "Date"      = "DATE",
          "POSIXct"   = "DATETIME",
          "character" = "varchar(255)",
          "integer"   = "INT",
          "numeric"   = "FLOAT",
          "logical"   = "BIT",
          # ..
          "checksum"  = "varchar(40)",
          "from_ts"   = "DATETIME",
          "until_ts"  = "DATETIME"
        )
      )
    })
  }


  if (is.null(conn)) {
    test_that("getTableSignature() generates signature for random data (conn == NULL)", {
      expect_identical(
        getTableSignature(data_random, conn),
        c(
          "Date"      = "Date",
          "POSIXct"   = "POSIXct",
          "character" = "character",
          # ..
          "checksum"  = "character",
          "from_ts"   = "POSIXct",
          "until_ts"  = "POSIXct",
          # ..
          "integer"   = "integer",
          "numeric"   = "numeric",
          "logical"   = "logical"
        )
      )
    })
  }

  if (inherits(conn, "SQLiteConnection")) {
    test_that("getTableSignature() generates signature for random data (SQLiteConnection)", {
      expect_identical(
        getTableSignature(data_random, conn),
        c(
          "Date"      = "DATE",
          "POSIXct"   = "TIMESTAMP",
          "character" = "TEXT",
          # ..
          "checksum"  = "TEXT",
          "from_ts"   = "TIMESTAMP",
          "until_ts"  = "TIMESTAMP",
          # ..
          "integer"   = "INT",
          "numeric"   = "DOUBLE",
          "logical"   = "SMALLINT"
        )
      )
    })
  }

  if (inherits(conn, "PqConnection")) {
    test_that("getTableSignature() generates signature for random data (PqConnection)", {
      expect_identical(
        getTableSignature(data_random, conn),
        c(
          "Date"      = "DATE",
          "POSIXct"   = "TIMESTAMPTZ",
          "character" = "TEXT",
          # ..
          "checksum"  = "TEXT",
          "from_ts"   = "TIMESTAMPTZ",
          "until_ts"  = "TIMESTAMPTZ",
          # ..
          "integer"   = "INTEGER",
          "numeric"   = "DOUBLE PRECISION",
          "logical"   = "BOOLEAN"
        )
      )
    })
  }

  if (inherits(conn, "Microsoft SQL Server")) {
    test_that("getTableSignature() generates signature for random data (Microsoft SQL Server)", {
      expect_identical(
        getTableSignature(data_random, conn),
        c(
          "Date"      = "DATE",
          "POSIXct"   = "DATETIME",
          "character" = "varchar(255)",
          # ..
          "checksum"  = "varchar(255)",
          "from_ts"   = "DATETIME",
          "until_ts"  = "DATETIME",
          # ..
          "integer"   = "INT",
          "numeric"   = "FLOAT",
          "logical"   = "BIT"
        )
      )
    })
  }


  if (inherits(conn, "SQLiteConnection")) {
    test_that("getTableSignature() generates signature for random data on remote (SQLiteConnection)", {
      expect_identical(
        getTableSignature(dplyr::copy_to(conn, data_random), conn),
        c(
          "Date"      = "DOUBLE",  # By copying to SQLite and back, information is changed by
          "POSIXct"   = "DOUBLE",  # dbplyr / DBI so data types are now similar, but different.
          "character" = "TEXT",    # Dates and timestamps which are normally stored in SQLite
          # ..                     # as internally TEXT are now converted to DOUBLE
          "checksum"  = "TEXT",    # Logical, which have the "SMALLINT" type are now "INT"
          "from_ts"   = "DOUBLE",  # In the next test, we check that this conversion is consistent
          "until_ts"  = "DOUBLE",  # for the user on the local R side.
          # ..
          "integer"   = "INT",
          "numeric"   = "DOUBLE",
          "logical"   = "INT"
        )
      )
    })
  }

  if (inherits(conn, "PqConnection")) {
    test_that("getTableSignature() generates signature for random data on remote (PqConnection)", {
      expect_identical(
        getTableSignature(dplyr::copy_to(conn, data_random), conn),
        c(
          "Date"      = "DATE",
          "POSIXct"   = "TIMESTAMPTZ",
          "character" = "TEXT",
          # ..
          "checksum"  = "TEXT",
          "from_ts"   = "TIMESTAMPTZ",
          "until_ts"  = "TIMESTAMPTZ",
          # ..
          "integer"   = "INTEGER",
          "numeric"   = "DOUBLE PRECISION",
          "logical"   = "BOOLEAN"
        )
      )
    })
  }

  if (inherits(conn, "Microsoft SQL Server")) {
    test_that("getTableSignature() generates signature for random data on remote (Microsoft SQL Server)", {
      expect_identical(
        getTableSignature(dplyr::copy_to(conn, data_random), conn),
        c(
          "Date"      = "DATE",
          "POSIXct"   = "DATETIME",
          "character" = "varchar(255)",
          # ..
          "checksum"  = "varchar(255)",
          "from_ts"   = "DATETIME",
          "until_ts"  = "DATETIME",
          # ..
          "integer"   = "INT",
          "numeric"   = "FLOAT",
          "logical"   = "BIT"
        )
      )
    })
  }


  if (!is.null(conn)) {
    test_that(glue::glue("getTableSignature() generates consistent data types ({class(conn)})"), {
      # This tests that the data types are consistent when copying to a remote table with getTableSignature().
      # We first copy the data to a remote table, then copy that table to another remote table on the same connection.
      # The
      remote_data_1 <- dplyr::copy_to(
        conn,
        data_random,
        name = "remote_data_1",
        types = getTableSignature(data_random, conn)
      )
      remote_data_2 <- dplyr::copy_to(
        conn,
        remote_data_1,
        name = "remote_data_2",
        types = getTableSignature(remote_data_1, conn)
      )

      # The table signatures are not always the same (eg. SQLiteConnection).
      if (inherits(conn, "SQLiteConnection")) {
        expect_false(identical( # In leui of expect_not_identical
          getTableSignature(data_random, conn),
          getTableSignature(remote_data_1, conn)
        ))
        expect_identical(
          names(getTableSignature(data_random, conn)),
          names(getTableSignature(remote_data_1, conn))
        )
      } else {
        expect_identical(
          getTableSignature(data_random, conn),
          getTableSignature(remote_data_1, conn)
        )
      }

      # But the data, when transfered locally, should be the same
      expect_identical(dplyr::collect(remote_data_2), dplyr::collect(remote_data_1))
    })
  }

  if (!is.null(conn)) connection_clean_up(conn)
}
