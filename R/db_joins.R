#' Generate sql_on statement for na joins
#' @details
#'   This function generates a much faster sql statement for NA join compared to dbplyr's _join with na_matches = "na".
#' @inheritParams left_join
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
join_warn <- function() {
  if (testthat::is_testing() || !interactive()) return()
  if (identical(parent.frame(n = 2), globalenv())) { # nocov start
    rlang::warn(paste("*_joins in database-backend does not match NA by default.\n",
                      "If your data contains NA, the columns with NA values must be supplied to \"na_by\",",
                      "or you must specifiy na_matches = \"na\""),
                .frequency = "once", .frequency_id = "*_join NA warning")
  } # nocov end
}


#' A warning to users that SQL does not match on NA by default
join_warn_experimental <- function() {
  if (testthat::is_testing() || !interactive()) return()
  if (identical(parent.frame(n = 2), globalenv())) { # nocov start
    rlang::warn("*_joins with na_by is stil experimental. Please report issues to rassky",
                .frequency = "once", .frequency_id = "*_join NA warning")
  } # nocov end
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
    mg_right_join(x, y, sql_on, renamer, ...)
  }
}


#' dbplyr and SQLite does not work right now for right_joins it seems
#' so we "fix" it by doing a left join on SQLiteConnections
#' @inheritParams dbplyr::join.tbl_sql
#' @param renamer named list generated by select_na_sql
mg_right_join <- function(x, y, sql_on, renamer, ...) {
  UseMethod("mg_right_join")
}

#' @inheritParams mg_right_join
mg_right_join.tbl_dbi <- function(x, y, sql_on, renamer, ...) {
  dplyr::right_join(x, y, suffix = c(".y", ".x"), sql_on = sql_on, ...) |>
    dplyr::rename(!!renamer) |>
    dplyr::select(tidyselect::all_of(names(renamer)))
}

#' @inheritParams mg_right_join
mg_right_join.tbl_SQLiteConnection <- function(x, y, sql_on, renamer, ...) {
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
semi_join <- function(x, y, by = NULL, na_by = NULL, ...) { # nocov start

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
} # nocov end

#' @export
#' @rdname joins
anti_join <- function(x, y, by = NULL, na_by = NULL, ...) { # nocov start

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
} # nocov end
