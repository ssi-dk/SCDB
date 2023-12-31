#' Generate sql_on statement for na joins
#' @details
#'   This function generates a much faster SQL statement for NA join compared to dbplyr's _join with na_matches = "na".
#' @inheritParams left_join
#' @return A sql_on statement to join by such that "NA" are matched with "NA"
#'   given the columns listed in "by" and "na_by"
#' @noRd
join_na_sql <- function(by, na_by) {
  sql_on <- ""
  if (!missing(by)) {
    for (i in seq_along(by)) {
      sql_on <- paste0(sql_on, '"LHS"."', by[i], '" = "RHS"."', by[i], '"')
      if (i < length(by) || !missing(na_by)) {
        sql_on <- paste(sql_on, "\nAND ")
      }
    }
  }

  if (!missing(na_by)) {
    for (i in seq_along(na_by)) {
      sql_on <- paste0(sql_on, '("LHS"."', na_by[i], '" IS NOT DISTINCT FROM "RHS"."', na_by[i], '")')
      if (i < length(na_by)) {
        sql_on <- paste(sql_on, "\nAND ")
      }
    }
  }

  return(sql_on)
}


#' Get colnames to select
#' @inheritParams left_join
#' @param left Boolean that control if joins is left (alternatively right) join
#' @return A named character vector indicating which columns to select from x and y
#' @noRd
select_na_sql <- function(x, y, by, na_by, left = TRUE) {

  all_by <- c(by, na_by) # Variables to be common after join
  cxy <- dplyr::setdiff(dplyr::intersect(colnames(x), colnames(y)), all_by)   # Duplicate columns after join
  cx  <- dplyr::setdiff(colnames(x), colnames(y)) # Variables only in x
  cy  <- dplyr::setdiff(colnames(y), colnames(x)) # Variables only in y

  vars <- list(all_by, cx, cy, cxy, cxy)

  renamer <- \(suffix) suffix |> purrr::map(~ purrr::partial(\(x, suffix) paste0(x, suffix), suffix = .))

  sql_select <- vars |>
    purrr::map2(renamer(list(ifelse(left, ".x", ".y"), "", "", ".x", ".y")), ~ purrr::map(.x, .y)) |>
    purrr::map(~ purrr::reduce(., c, .init = character(0))) |>
    purrr::reduce(c)

  sql_names <- vars |>
    purrr::map2(renamer(list("", "", "", ".x", ".y")), ~ purrr::map(.x, .y)) |>
    purrr::map(~ purrr::reduce(., c, .init = character(0))) |>
    purrr::reduce(c)

  names(sql_select) <- sql_names

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
#' @export
inner_join <- function(x, y, by = NULL, na_by = NULL, ...) {

  # Check arguments
  assert_data_like(x)
  assert_data_like(y)
  checkmate::assert_character(by, null.ok = TRUE)
  checkmate::assert_character(na_by, null.ok = TRUE)

  if (is.null(na_by)) {
    if (inherits(x, "tbl_dbi") || inherits(y, "tbl_dbi")) join_warn()
    return(dplyr::inner_join(x, y, by = by, ...))
  } else {
    join_warn_experimental()
    sql_on <- join_na_sql(by, na_by)
    renamer <- select_na_sql(x, y, by, na_by)
    return(dplyr::inner_join(x, y, suffix = c(".x", ".y"), sql_on = sql_on, ...) |>
             dplyr::rename(!!renamer) |>
             dplyr::select(tidyselect::all_of(names(renamer))))
  }
}


#' @export
#' @rdname joins
left_join <- function(x, y, by = NULL, na_by = NULL, ...) {

  # Check arguments
  assert_data_like(x)
  assert_data_like(y)
  checkmate::assert_character(by, null.ok = TRUE)
  checkmate::assert_character(na_by, null.ok = TRUE)

  if (is.null(na_by)) {
    if (inherits(x, "tbl_dbi") || inherits(y, "tbl_dbi")) join_warn()
    return(dplyr::left_join(x, y, by = by, ...))
  } else {
    join_warn_experimental()
    sql_on <- join_na_sql(by, na_by)
    renamer <- select_na_sql(x, y, by, na_by)
    return(dplyr::left_join(x, y, suffix = c(".x", ".y"), sql_on = sql_on, ...) |>
             dplyr::rename(!!renamer) |>
             dplyr::select(tidyselect::all_of(names(renamer))))


  }
}


#' @export
#' @rdname joins
right_join <- function(x, y, by = NULL, na_by = NULL, ...) {

  # Check arguments
  assert_data_like(x)
  assert_data_like(y)
  checkmate::assert_character(by, null.ok = TRUE)
  checkmate::assert_character(na_by, null.ok = TRUE)

  if (is.null(na_by)) {
    if (inherits(x, "tbl_dbi") || inherits(y, "tbl_dbi")) join_warn()
    return(dplyr::right_join(x, y, by = by, ...))
  } else {
    join_warn_experimental()
    sql_on <- join_na_sql(by, na_by)
    renamer <- select_na_sql(x, y, by, na_by, left = FALSE)

    # Seems like right_join does not work for SQLite, so we'll do a left join for now
    return(scdb_right_join(x, y, sql_on, renamer, ...))
  }
}


#' dbplyr and SQLite does not work right now for right_joins it seems
#' so we "fix" it by doing a left join on SQLiteConnections
#' @inheritParams dbplyr::join.tbl_sql
#' @inherit dbplyr::join.tbl_sql return
#' @param renamer named list generated by select_na_sql
#' @noRd
scdb_right_join <- function(x, y, sql_on, renamer, ...) {
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


#' @export
#' @rdname joins
full_join <- function(x, y, by = NULL, na_by = NULL, ...) {

  # Check arguments
  assert_data_like(x)
  assert_data_like(y)
  checkmate::assert_character(by, null.ok = TRUE)
  checkmate::assert_character(na_by, null.ok = TRUE)

  if (is.null(na_by)) {
    if (inherits(x, "tbl_dbi") || inherits(y, "tbl_dbi")) join_warn()
    return(dplyr::full_join(x, y, by = by, ...))
  } else {
    join_warn_experimental()
    # Full joins are hard...
    out <- dplyr::union(left_join(x, y, by = by, na_by = na_by),
                        right_join(x, y, by = by, na_by = na_by))
    return(out)
  }
}


#' @export
#' @rdname joins
semi_join <- function(x, y, by = NULL, na_by = NULL, ...) {

  # Check arguments
  assert_data_like(x)
  assert_data_like(y)
  checkmate::assert_character(by, null.ok = TRUE)
  checkmate::assert_character(na_by, null.ok = TRUE)

  if (is.null(na_by)) {
    if (inherits(x, "tbl_dbi") || inherits(y, "tbl_dbi")) join_warn()
    return(dplyr::semi_join(x, y, by = by, ...))
  } else {
    stop("Not implemented")
  }
}

#' @export
#' @rdname joins
anti_join <- function(x, y, by = NULL, na_by = NULL, ...) {

  # Check arguments
  assert_data_like(x)
  assert_data_like(y)
  checkmate::assert_character(by, null.ok = TRUE)
  checkmate::assert_character(na_by, null.ok = TRUE)

  if (is.null(na_by)) {
    if (inherits(x, "tbl_dbi") || inherits(y, "tbl_dbi")) join_warn()
    return(dplyr::anti_join(x, y, by = by, ...))
  } else {
    stop("Not implemented")
  }
}
