#' SQL Joins
#'
#' @name joins
#'
#' @description
#'  `r lifecycle::badge("experimental")`
#'
#'   Overloads the dplyr `*_join` to accept an `na_by` argument.
#'   By default, joining using SQL does not match on `NA` / `NULL`.
#'   dbplyr `*_join`s has the option "na_matches = na" to match on `NA` / `NULL` but this operation is substantially
#'   slower since it turns all equality comparisons to identical comparisons.
#'
#'   This function does the matching more efficiently by allowing the user to specify which column contains
#'   `NA` / `NULL` values and which does not:
#'   If a column contains `NA` / `NULL`, the names of these columns can be passed via the `na_by` argument and
#'   efficiently match as if `na_matches = "na"`.
#'   Columns without `NA` / `NULL` values is passed via the `by` argument and will be matched `na_matches = "never"`.
#'
#'   If no `na_by` argument is given, the function defaults to using `dplyr::*_join` without modification.
#'
#' @inheritParams dbplyr::join.tbl_sql
#' @return Another \code{tbl_lazy}. Use \code{\link[dplyr:show_query]{show_query()}} to see the generated
#' query, and use \code{\link[dbplyr:collect.tbl_sql]{collect()}} to execute the query
#' and return data to R.
#' @examplesIf requireNamespace("RSQLite", quietly = TRUE)
#'   library(dplyr, warn.conflicts = FALSE)
#'   library(dbplyr, warn.conflicts = FALSE)
#'
#'   band_db <- tbl_memdb(dplyr::band_members)
#'   instrument_db <- tbl_memdb(dplyr::band_instruments)
#'
#'   left_join(band_db, instrument_db) |>
#'     show_query()
#'
#'   # Can join with local data frames by setting copy = TRUE
#'   left_join(band_db, dplyr::band_instruments, copy = TRUE)
#'
#'   # Unlike R, joins in SQL don't usually match NAs (NULLs)
#'   db <- memdb_frame(x = c(1, 2, NA))
#'   label <- memdb_frame(x = c(1, NA), label = c("one", "missing"))
#'   left_join(db, label, by = "x")
#'
#'   # But you can activate R's usual behaviour with the na_matches argument
#'   left_join(db, label, by = "x", na_matches = "na")
#'
#'   # By default, joins are equijoins, but you can use `dplyr::join_by()` to
#'   # express richer relationships
#'   db1 <- memdb_frame(id = 1:5)
#'   db2 <- memdb_frame(id = 1:3, y = letters[1:3])
#'
#'   left_join(db1, db2) |> show_query()
#'   left_join(db1, db2, by = join_by(x$id < y$id)) |> show_query()
#' @seealso [dplyr::mutate-joins] which this function wraps.
#' @seealso [dbplyr::join.tbl_sql] which this function wraps.
#' @seealso [dplyr::show_query]
#' @exportS3Method dplyr::inner_join
inner_join.tbl_sql <- function(x, y, by = NULL, ...) {
  .dots <- list(...)

  if (!"na_by" %in% names(.dots)) {
    join_warn()
    return(NextMethod("inner_join"))
  }

  # Prepare the combined join
  out <- do.call(dplyr::inner_join, args = join_args(x, y, by, .dots))
  out$lazy_query$vars <- join_na_select_fix(out$lazy_query$vars, .dots$na_by)

  return(out)
}

#' @rdname joins
#' @exportS3Method dplyr::left_join
left_join.tbl_sql <- function(x, y, by = NULL, ...) {
  .dots <- list(...)

  if (!"na_by" %in% names(.dots)) {
    join_warn()
    return(NextMethod("left_join"))
  }

  out <- do.call(dplyr::left_join, args = join_args(x, y, by, .dots))
  out$lazy_query$vars <- join_na_select_fix(out$lazy_query$vars, .dots$na_by)

  return(out)
}

#' @rdname joins
#' @exportS3Method dplyr::right_join
right_join.tbl_sql <- function(x, y, by = NULL, ...) {
  .dots <- list(...)

  if (!"na_by" %in% names(.dots)) {
    join_warn()
    return(NextMethod("right_join"))
  }

  out <- do.call(dplyr::right_join, args = join_args(x, y, by, .dots))
  out$lazy_query$vars <- join_na_select_fix(out$lazy_query$vars, .dots$na_by, right = TRUE)

  return(out)
}


#' @rdname joins
#' @exportS3Method dplyr::full_join
full_join.tbl_sql <- function(x, y, by = NULL, ...) {
  .dots <- list(...)

  if (!"na_by" %in% names(.dots)) {
    join_warn()
    return(NextMethod("full_join"))
  }

  out <- do.call(dplyr::full_join, args = join_args(x, y, by, .dots))
  out$lazy_query$vars <- join_na_select_fix(out$lazy_query$vars, .dots$na_by)

  return(out)
}


#' @rdname joins
#' @exportS3Method dplyr::semi_join
semi_join.tbl_sql <- function(x, y, by = NULL, ...) {
  .dots <- list(...)

  if (!"na_by" %in% names(.dots)) {
    join_warn()
    return(NextMethod("semi_join"))
  }

  out <- do.call(dplyr::semi_join, args = join_args(x, y, by, .dots))

  return(out)
}


#' @rdname joins
#' @exportS3Method dplyr::anti_join
anti_join.tbl_sql <- function(x, y, by = NULL, ...) {
  .dots <- list(...)

  if (!"na_by" %in% names(.dots)) {
    join_warn()
    return(NextMethod("anti_join"))
  }

  out <- do.call(dplyr::anti_join, args = join_args(x, y, by, .dots))

  return(out)
}


#' Warn users that SQL does not match on NA by default
#'
#' @return
#'   A warning that *_joins on SQL backends does not match NA by default.
#' @noRd
join_warn <- function() {
  if (interactive() && identical(parent.frame(n = 2), globalenv())) {
    rlang::warn(
      paste(
        "*_joins in database-backend does not match NA by default.\n",
        "If your data contains NA, the columns with NA values must be supplied to \"na_by\",",
        "or you must specify na_matches = \"na\""
      ),
      .frequency = "once",
      .frequency_id = "*_join NA warning"
    )
  }
}


#' Warn users that SQL joins by NA is experimental
#'
#' @return
#'   A warning that *_joins are still experimental.
#' @noRd
join_warn_experimental <- function() {
  if (interactive() && identical(parent.frame(n = 2), globalenv())) {
    rlang::warn(
      "*_joins with na_by is still experimental. Please report issues.",
      .frequency = "once",
      .frequency_id = "*_join NA warning"
    )
  }
}


#' Construct the arguments to `*_join` that accounts for the na matching
#' @param x (`tbl_sql`) \cr
#'   The left table to join.
#' @param y (`tbl_sql`) \cr
#'   The right table to join.
#' @param by (`dbplyr_join_by` or `character`) \cr
#'   The columns to match on without NA values.
#' @param .dots (`list`) \cr
#'   Arguments passed to the `*_join` function.
#' @noRd
join_args <- function(x, y, by, .dots) {

  # Remove the na matching args, and let join_na_sql combine the `by` and `na_by` statements
  by <- join_na_sql(x, y, by, .dots)
  args <- append(list(x = x, y = y, by = by), purrr::discard_at(.dots, c("na_by", "na_matches")))

  return(args)
}


#' Merge two `dplyr_join_by` objects
#' @param by (`dplyr_join_by` or `character`) \cr
#'   The columns to match on without NA values.
#' @param na_by (`dplyr_join_by` or `character`) \cr
#'   The columns to match on NA.
#' @noRd
join_merger <- function(by, na_by) {

  # Early return if only one by statement is given
  if (is.null(by) && is.null(na_by)) {
    stop("Both by and na_by cannot be NULL")
  } else if (is.null(by)) {
    return(na_by)
  } else if (is.null(na_by)) {
    return(by)
  }

  # Combine the by and na_by statements by unclassing, merging and reclassing
  combined_join <- list(
    "exprs" = c(purrr::pluck(by, "exprs"), purrr::pluck(na_by, "exprs"))
  ) |>
    utils::modifyList(
      purrr::map2(purrr::discard_at(by, "exprs"), purrr::discard_at(na_by, "exprs"), ~ c(.x, .y))
    )
  class(combined_join) <- "dplyr_join_by"

  return(combined_join)
}


#' Generate `dplyr_join_by` statement for na joins
#'
#' @description
#'   This function creates a `dplyr_join_by` object to join by where the statements supplied in `by` are treated as not
#'   having NA values while the columns listed in `na_by` are treated as having NA values.
#'   This latter translation corresponds to using `dplyr::*_join` with `na_matches = "na"`.
#' @inheritParams left_join
#' @param na_by (`character`)\cr
#'   The columns to match on NA. If a column contains NA, the names of these columns can be passed via the `na_by`
#'   argument. These will then be matched as if with the `na_matches = "na"` argument.
#' @return
#'   A `dplyr_join_by` object to join by such that "NA" are matched with "NA" given the columns listed in `by` and
#'   `na_by`.
#' @noRd
join_na_sql <- function(x, y, by = NULL, .dots = NULL) {

  # Early return if no na_by statement is given
  if (is.null(.dots$na_by)) {
    return(by)
  } else {
    na_by <- .dots$na_by
  }

  # Check arguments
  checkmate::assert(
    checkmate::check_character(by, null.ok = TRUE),
    checkmate::check_class(by, "dplyr_join_by", null.ok = TRUE)
  )
  checkmate::assert(
    checkmate::check_character(na_by, null.ok = TRUE),
    checkmate::check_class(na_by, "dplyr_join_by", null.ok = TRUE)
  )

  join_warn_experimental()

  # Convert to dplyr_join_by if not already
  if (!is.null(by) && !inherits(by, "dplyr_join_by")) {
    by <- dplyr::join_by(!!!by)
  }

  if (!is.null(na_by) && !inherits(na_by, "dplyr_join_by")) {
    na_by <- dplyr::join_by(!!!na_by)
  }

  # Get the translation for matching the na_by component of the join
  subquery_args <- purrr::discard_at(.dots, "na_by") |>
    utils::modifyList(
      list(
        x = x,
        y = y,
        by = join_merger(by, na_by),
        na_matches = "na"
      )
    )
  na_subquery <- dbplyr::remote_query(do.call(dplyr::inner_join, args = subquery_args))

  # Determine the NA matching statement by extracting from the translated query.
  # E.g. on RSQlite, the keyword "IS" checks if arguments are identical
  # and on PostgreSQL, the keyword "IS NOT DISTINCT FROM" checks if arguments are identical.
  na_matching <- na_subquery |>
    stringr::str_remove_all(stringr::fixed("\n")) |> # Remove newlines from the formatted query
    stringr::str_replace_all(r"{\s{2,}}", " ") |> # Remove multiple spaces from the formatted query
    stringr::str_extract(r"{(?<=ON \().*(?=\))}") |> # Extract the contents of the ON statement
    stringr::str_extract(pattern = r"{(?:["'`]\s)([\w\s]+)(?:\s["'`])}", group = 1) # First non quoted word(s)

  # Replace NA equals with NA matching statement
  na_by$condition[na_by$condition == "=="] <- na_matching

  return(join_merger(by, na_by))
}


#' Manually fixes the select component of the `lazy_query` after overwriting the `by` statement.
#'
#' @description
#'   After overwriting the `by` statement in the `lazy_query`, the `vars` component of the `lazy_query` is not
#'   consistent with the new non-overwritten `by` statement.
#'   As a result, columns which are matched in the join are included as both `<col>.x` and `<col.y>`, instead of just
#'   as `<col>`.
#'   This function fixes the `vars` component of the `lazy_query` to remove the doubly selected columns and rename
#'   to the expected name.
#' @param vars (`tibble`)\cr
#'   The `vars` component of the `lazy_query`.
#' @param na_by (`dplyr_join_by`)\cr
#'   The `na_by` statement used in the join.
#' @param right (`logical`)\cr
#'   If the join is a right join.
#' @return
#'   A `tibble` with the `vars` component of the `lazy_query` fixed to remove doubly selected columns.
#' @noRd
join_na_select_fix <- function(vars, na_by, right = FALSE) {
  if (is.null(na_by)) return(vars)

  if (!inherits(na_by, "dplyr_join_by")) na_by <- dplyr::join_by(!!!na_by)

  # All equality joins in `na_by` are incorrectly translated
  doubly_selected_columns <- na_by |>
    purrr::discard_at("exprs") |>
    tibble::as_tibble() |>
    dplyr::filter(.data$condition == "==", .data$x == .data$y) |>
    dplyr::pull("x")

  if (length(doubly_selected_columns) == 0) {
    updated_vars <- vars # no doubly selected columns
  } else {

    # The vars table structure is not consistent between dplyr join types
    # There are two formats which we needs to manage independently.
    if (checkmate::test_names(names(vars), identical.to = c("name", "x", "y"))) {
      updated_vars <- rbind(
        tibble::tibble(
          "name" = doubly_selected_columns,
          "x" = ifelse(right, NA, doubly_selected_columns),
          "y" = doubly_selected_columns
        ),
        dplyr::filter(vars, .data$x %in% !!doubly_selected_columns | .data$y %in% !!doubly_selected_columns)
      ) |>
        dplyr::symdiff(vars)

      # Reorder our updated columns to match the original order
      updated_vars <- updated_vars[
        order(match(updated_vars$name, unique(purrr::pmap_chr(vars, ~ dplyr::coalesce(..2, ..3))))),
      ]

    } else if (checkmate::test_names(names(vars), identical.to = c("name", "table", "var"))) {
      updated_vars <- rbind(
        tibble::tibble(
          "name" = doubly_selected_columns,
          "table" = 1,
          "var" = doubly_selected_columns
        ),
        dplyr::filter(vars, .data$var %in% !!doubly_selected_columns)
      ) |>
        dplyr::symdiff(vars)

      # Reorder our updated columns to match the original order
      updated_vars <- updated_vars[order(match(updated_vars$name, unique(vars$var))), ]

    }
  }

  return(updated_vars)
}
