#' Generate sql_on statement for na joins
#' @details
#'   This function generates a much faster SQL statement for NA join compared to dbplyr's _join with na_matches = "na".
#' @inheritParams left_join
#' @return A sql_on statement to join by such that "NA" are matched with "NA"
#'   given the columns listed in "by" and "na_by"
#' @noRd
join_na_sql <- function(x, by, na_by) {
  UseMethod("join_na_sql")
}

join_na_not_distinct <- function(by, na_by = NULL) {
  sql_on <- ""
  if (!missing(by)) {
    for (i in seq_along(by)) {
      sql_on <- paste0(sql_on, '"LHS"."', by[i], '" = "RHS"."', by[i], '"')
      if (i < length(by) || !is.null(na_by)) {
        sql_on <- paste(sql_on, "\nAND ")
      }
    }
  }

  if (!missing(na_by)) {
    for (i in seq_along(na_by)) {
      sql_on <- paste0(sql_on, '"LHS"."', na_by[i], '" IS NOT DISTINCT FROM "RHS"."', na_by[i], '"')
      if (i < length(na_by)) {
        sql_on <- paste(sql_on, "\nAND ")
      }
    }
  }

  return(sql_on)
}

join_na_not_null <- function(by, na_by = NULL) {
  sql_on <- ""
  if (!missing(by)) {
    for (i in seq_along(by)) {
      sql_on <- paste0(sql_on, '"LHS"."', by[i], '" = "RHS"."', by[i], '"')
      if (i < length(by) || !is.null(na_by)) {
        sql_on <- paste(sql_on, "\nAND ")
      }
    }
  }

  if (!missing(na_by)) {
    for (i in seq_along(na_by)) {
      sql_on <- paste0(sql_on,
                       '("LHS"."', na_by[i], '" IS NULL AND "RHS"."', na_by[i], '" IS NULL ',
                       'OR "LHS"."', na_by[i], '" = "RHS"."', na_by[i], '")')
      if (i < length(na_by)) {
        sql_on <- paste(sql_on, "\nAND ")
      }
    }
  }

  return(sql_on)
}

join_na_sql.tbl_dbi <- function(x, by, na_by) {
  return(join_na_not_distinct(by = by, na_by = na_by))
}

`join_na_sql.tbl_Microsoft SQL Server` <- function(x, by, na_by) {
  return(join_na_not_null(by = by, na_by = na_by))
}

#' Get colnames to select
#' @inheritParams left_join
#' @param left Boolean that control if joins is left (alternatively right) join
#' @return A named character vector indicating which columns to select from x and y
#' @noRd
select_na_sql <- function(x, y, by, na_by, left = TRUE) {

  all_by <- c(by, na_by) # Variables to be common after join
  cx  <- dplyr::setdiff(colnames(x), colnames(y)) # Variables only in x
  cy  <- dplyr::setdiff(colnames(y), colnames(x)) # Variables only in y

  sql_select <-
    c(paste0(colnames(x), ifelse(!colnames(x) %in% cx, ".x", "")),
      paste0(colnames(y), ifelse(!colnames(y) %in% cy, ".y", ""))[!colnames(y) %in% all_by]) |>
    stats::setNames(c(colnames(x),
                      paste0(colnames(y), ifelse(colnames(y) %in% colnames(x), ".y", ""))[!colnames(y) %in% all_by]))

  return(sql_select)
}


#' A warning to users that SQL does not match on NA by default
#' @return A warning that *_joins on SQL backends does not match NA by default
#' @noRd
join_warn <- function() {
  if (testthat::is_testing() || !interactive()) return()
  if (identical(parent.frame(n = 2), globalenv())) {
    rlang::warn(paste("*_joins in database-backend does not match NA by default.\n",
                      "If your data contains NA, the columns with NA values must be supplied to \"na_by\",",
                      "or you must specifiy na_matches = \"na\""),
                .frequency = "once", .frequency_id = "*_join NA warning")
  }
}


#' A warning to users that SQL does not match on NA by default
#' @return A warning that *_joins are still experimental
#' @noRd
join_warn_experimental <- function() {
  if (testthat::is_testing() || !interactive()) return()
  if (identical(parent.frame(n = 2), globalenv())) {
    rlang::warn("*_joins with na_by is stil experimental. Please report issues to rassky",
                .frequency = "once", .frequency_id = "*_join NA warning")
  }
}


#' SQL Joins
#'
#' @name joins
#'
#' @description Overloads the dplyr *_join to accept an na_by argument.
#' By default, joining using SQL does not match on NA / NULL.
#' dbplyr has the option "na_matches = na" to match on NA / NULL but this is very inefficient
#' This function does the matching more efficiently.
#' If a column contains NA / NULL, give the argument to na_by to match during the join
#' If no na_by is given, the function defaults to using dplyr::*_join
#' @inheritParams dbplyr::join.tbl_sql
#' @inherit dbplyr::join.tbl_sql return
#' @examples
#' library(dplyr, warn.conflicts = FALSE)
#' library(dbplyr, warn.conflicts = FALSE)
#' band_db <- tbl_memdb(dplyr::band_members)
#' instrument_db <- tbl_memdb(dplyr::band_instruments)
#' band_db %>% left_join(instrument_db) %>% show_query()
#'
#' # Can join with local data frames by setting copy = TRUE
#' band_db %>%
#'   left_join(dplyr::band_instruments, copy = TRUE)
#'
#' # Unlike R, joins in SQL don't usually match NAs (NULLs)
#' db <- memdb_frame(x = c(1, 2, NA))
#' label <- memdb_frame(x = c(1, NA), label = c("one", "missing"))
#' db %>% left_join(label, by = "x")
#' # But you can activate R's usual behaviour with the na_matches argument
#' db %>% left_join(label, by = "x", na_matches = "na")
#'
#' # By default, joins are equijoins, but you can use `sql_on` to
#' # express richer relationships
#' db1 <- memdb_frame(x = 1:5)
#' db2 <- memdb_frame(x = 1:3, y = letters[1:3])
#' db1 %>% left_join(db2) %>% show_query()
#' db1 %>% left_join(db2, sql_on = "LHS.x < RHS.x") %>% show_query()
#' @param na_by columns that should match on NA
#' @seealso [dplyr::mutate-joins] which this function wraps.
#' @seealso [dbplyr::join.tbl_sql] which this function wraps.
#' @exportS3Method dplyr::inner_join
inner_join.tbl_sql <- function(x, y, by = NULL, ...) {

  # Check arguments
  assert_data_like(x)
  assert_data_like(y)
  checkmate::assert_character(by, null.ok = TRUE)

  .dots <- list(...)

  if (!"na_by" %in% names(.dots)) {
    if (inherits(x, "tbl_dbi") || inherits(y, "tbl_dbi")) join_warn()

    return(NextMethod("inner_join"))
  }

  join_warn_experimental()

  # Modify arguments for new dplyr call
  args <- as.list(rlang::current_env())
  args$sql_on <- join_na_sql(x, by, .dots$na_by)
  .renamer <- select_na_sql(x, y, by, .dots$na_by)

  join_result <- do.call(dplyr::inner_join, args = args) |>
    dplyr::rename(!!.renamer) |>
    dplyr::select(tidyselect::all_of(names(.renamer)))

  return(join_result)
}

#' @rdname joins
#' @exportS3Method dplyr::left_join
left_join.tbl_sql <- function(x, y, by = NULL, ...) {

  # Check arguments
  assert_data_like(x)
  assert_data_like(y)
  checkmate::assert_character(by, null.ok = TRUE)

  .dots <- list(...)

  if (!"na_by" %in% names(.dots)) {
    if (inherits(x, "tbl_dbi") || inherits(y, "tbl_dbi")) join_warn()
    return(NextMethod("left_join"))
  }
  join_warn_experimental()
  args <- as.list(rlang::current_env())
  args$sql_on <- join_na_sql(x, by, .dots$na_by)
  .renamer <- select_na_sql(x, y, by, .dots$na_by)

  join_result <- do.call(dplyr::left_join, args = args) |>
    dplyr::rename(!!.renamer) |>
    dplyr::select(tidyselect::all_of(names(.renamer)))

  return(join_result)
}

#' @exportS3Method dplyr::right_join
#' @rdname joins
right_join.tbl_sql <- function(x, y, by = NULL, ...) {

  # Check arguments
  assert_data_like(x)
  assert_data_like(y)
  checkmate::assert_character(by, null.ok = TRUE)

  .dots <- list(...)
  args <- as.list(rlang::current_env())

  if (!"na_by" %in% names(.dots)) {
    if (inherits(x, "tbl_dbi") || inherits(y, "tbl_dbi")) join_warn()

    .renamer <- select_na_sql(x, y, by, na_by = NULL, left = FALSE)
  } else {
    join_warn_experimental()
    args$sql_on <- join_na_sql(x, by, .dots$na_by)
    .renamer <- select_na_sql(x, y, by, .dots$na_by, left = FALSE)
  }

  # Swap x and y and proceed with left_join
  args$x <- y
  args$y <- x
  args$keep <- TRUE

  join_result <- do.call(dplyr::left_join, args = args) |>
    dplyr::rename(!!.renamer) |>
    dplyr::select(tidyselect::all_of(names(.renamer)))

  return(join_result)
}

#' dbplyr and SQLite does not work right now for right_joins it seems
#' so we "fix" it by doing a left join on SQLiteConnections
#' @inheritParams dbplyr::join.tbl_sql
#' @inherit dbplyr::join.tbl_sql return
#' @param renamer named list generated by select_na_sql
#' @noRd
scdb_right_join <- function(x, y, sql_on, suffix = c(".x", ".y"), renamer, ...) {
  UseMethod("scdb_right_join")
}

#' @inheritParams scdb_right_join
scdb_right_join.tbl_dbi <- function(x, y, sql_on, renamer, ...) {
  dplyr::right_join(x, y, suffix = c(".y", ".x"), sql_on = sql_on, ...) |>
    dplyr::rename(!!renamer) |>
    dplyr::select(tidyselect::all_of(names(renamer)))
}

#' @inheritParams scdb_right_join
scdb_right_join.tbl_SQLiteConnection <- function(x, y, sql_on, renamer, ...) {
  dplyr::left_join(y, x, suffix = c(".y", ".x"), sql_on = sql_on, ...) |>
    dplyr::rename(!!renamer) |>
    dplyr::select(tidyselect::all_of(names(renamer)))
}


#' @exportS3Method dplyr::full_join
#' @rdname joins
full_join.tbl_sql <- function(x, y, by = NULL, ...) {

  # Check arguments
  assert_data_like(x)
  assert_data_like(y)
  checkmate::assert_character(by, null.ok = TRUE)

  .dots <- list(...)

  if (!"na_by" %in% names(.dots)) {
    if (inherits(x, "tbl_dbi") || inherits(y, "tbl_dbi")) join_warn()
    return(dplyr::full_join(x, y, by = by, ...))
  } else {
    join_warn_experimental()
    # Full joins are hard...
    out <- dplyr::union(dplyr::left_join(x, y, by = by, na_by = .dots$na_by),
                        dplyr::right_join(x, y, by = by, na_by = .dots$na_by))
    return(out)
  }
}


#' @exportS3Method dplyr::semi_join
#' @rdname joins
semi_join.tbl_sql <- function(x, y, by = NULL, ...) {

  # Check arguments
  assert_data_like(x)
  assert_data_like(y)
  checkmate::assert_character(by, null.ok = TRUE)

  .dots <- list(...)

  if (!"na_by" %in% names(.dots)) {
    if (inherits(x, "tbl_dbi") || inherits(y, "tbl_dbi")) join_warn()
    return(dplyr::semi_join(x, y, by = by, ...))
  } else {
    stop("Not implemented")
  }
}

#' @exportS3Method dplyr::anti_join
#' @rdname joins
anti_join.tbl_sql <- function(x, y, by = NULL, ...) {

  # Check arguments
  assert_data_like(x)
  assert_data_like(y)
  checkmate::assert_character(by, null.ok = TRUE)

  .dots <- list(...)

  if (!"na_by" %in% names(.dots)) {
    if (inherits(x, "tbl_dbi") || inherits(y, "tbl_dbi")) join_warn()
    return(dplyr::anti_join(x, y, by = by, ...))
  } else {
    stop("Not implemented")
  }
}
