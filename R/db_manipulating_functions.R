#' Filters .data according to all records in the filter
#'
#' @description
#' If filter = NULL, not filtering is done
#' If filter is different from NULL, the .data is filtered by a inner_join using all columns of the filter:
#' \code{inner_join(.data, filter, by = colnames(filter))}
#'
#' by and na_by can overwrite the inner_join columns used in the filtering
#'
#' @template .data
#' @template filters
#' @param by      passed to inner_join if different from NULL
#' @param na_by   passed to inner_join if different from NULL
#' @template .data_return
#' @examples
#' # Filtering with null means no filtering is done
#' filter <- NULL
#' identical(filter_keys(mtcars, filter), mtcars) # TRUE
#'
#' # Filtering by vs = 0
#' filter <- data.frame(vs = 0)
#' identical(filter_keys(mtcars, filter), dplyr::filter(mtcars, vs == 0)) # TRUE
#'
#' # Filtering by the specific combinations of vs = 0 and am = 1
#' filter <- dplyr::distinct(mtcars, vs, am)
#' filter_keys(mtcars, filter)
#'
#' @importFrom rlang .data
#' @export
filter_keys <- function(.data, filters, by = NULL, na_by = NULL) {
  if (is.null(filters)) {
    return(.data)
  }

  UseMethod("filter_keys")
}

#' @export
filter_keys.tbl_sql <- function(.data, filters, by = NULL, na_by = NULL) {

  if (is.null(by) && is.null(na_by)) {
    # Determine key types
    key_types <- filters |>
      dplyr::ungroup() |>
      dplyr::summarise(dplyr::across(
        .cols = tidyselect::everything(),
        .fns = ~ sum(ifelse(is.na(.), 0, 1), na.rm = TRUE)
      )) |>
      tidyr::pivot_longer(tidyselect::everything(), names_to = "column_name", values_to = "is_na")

    by    <- key_types |> dplyr::filter(.data$is_na > 0) |> dplyr::pull("column_name")
    na_by <- key_types |> dplyr::filter(.data$is_na == 0)  |> dplyr::pull("column_name")

    if (length(by) == 0)    by    <- NULL
    if (length(na_by) == 0) na_by <- NULL
  }
  return(dplyr::inner_join(.data, filters, by = by, na_by = na_by))
}

#' @export
filter_keys.data.frame <- function(.data, filters, by = NULL, na_by = NULL) {
  if (is.null(by) && is.null(na_by)) by <- colnames(filters)
  return(dplyr::inner_join(.data, filters, by = c(by, na_by)))
}

#' tidyr::unite for tbl_dbi
#'
#' @inheritParams tidyr::unite
#' @examples
#' library(tidyr, warn.conflicts = FALSE)
#' df <- expand_grid(x = c("a", NA), y = c("b", NA))
#' df

#' df %>% unite("z", x:y, remove = FALSE)
#' # To remove missing values:
#* df %>% unite("z", x:y, na.rm = TRUE, remove = FALSE)
#'
#' # Separate is almost the complement of unite
#' df %>%
#'   unite("xy", x:y) %>%
#'   separate(xy, c("x", "y"))
# (but note `x` and `y` contain now "NA" not NA)
#' @return A tbl_dbi with the specified columns united into a new column named according to "col"
#' @importFrom rlang `:=`
#' @exportS3Method tidyr::unite tbl_dbi
unite.tbl_dbi <- function(data, col, ..., sep = "_", remove = TRUE, na.rm = FALSE) { # nolint: object_name_linter

  # Modified from
  # https://stackoverflow.com/questions/48536983/how-to-concatenate-strings-of-multiple-  -- continued below
  # columns-from-table-in-sql-server-using-dp

  # Check arguments
  checkmate::assert_class(data, "tbl_dbi")
  checkmate::assert_character(sep)
  checkmate::assert_logical(remove)
  checkmate::assert_logical(na.rm)

  # Code below is adapted from tidyr::unite.data.frame
  rlang::check_dots_unnamed()

  if (rlang::dots_n(...) == 0) {
    from_vars <- colnames(data)
  } else {
    from_vars <- colnames(dplyr::select(data, ...))
  }

  # We need add some support for how tidyr::unite accepts input of "col"
  col <- rlang::as_string(rlang::ensym(col))

  col_symbols <- purrr::map(from_vars, as.symbol)

  # We need to determine where col should be placed
  first_from <- which(colnames(data) %in% from_vars)[1]

  # CONCAT_WS does not exist in SQLite
  if (inherits(data, "tbl_SQLiteConnection")) {
    out <- data |>
      dplyr::mutate({{col}} := NULLIF(paste(!!!col_symbols, sep = sep), ""), .before = !!first_from)
  } else {
    out <- data |>
      dplyr::mutate({{col}} := NULLIF(CONCAT_WS(sep, !!!col_symbols), ""), .before = !!first_from)
  }

  if (remove) out <- out |> dplyr::select(!tidyselect::all_of(from_vars))

  return(out)
}


#' Combine any number of SQL queries, where each has their own time axis of
#' validity (valid_from and valid_until)
#'
#' @description
#' The function "interlaces" the queries and combines their validity time axes
#' onto a single time axis
#'
#' @param tables    A list(!) of tables you want to combine is supplied here as
#'                  lazy_queries.
#' @param by        The (group) variable to merge by
#' @param colnames  If the time axes of validity is not called "valid_to" and
#'                  "valid_until" inside each lazy_query, you can specify their
#'                  names by supplying the arguments as a list
#'                  (e.g. c(t1.from = "\<colname\>", t2.until = "\<colname\>").
#'                  colnames must be named in same order as as given in tables
#'                  (i.e. t1, t2, t3, ...).
#' @examples
#' conn <- get_connection(drv = RSQLite::SQLite())
#'
#'
#' if (schema_exists(conn, "test")) {
#'   t1 <- data.frame(key = c("A", "A", "B"),
#'                   obs_1   = c(1, 2, 2),
#'                   valid_from  = as.Date(c("2021-01-01", "2021-02-01", "2021-01-01")),
#'                   valid_until = as.Date(c("2021-02-01", "2021-03-01", NA)))
#'   t1 <- dplyr::copy_to(conn, t1, id("test.SCDB_tmp1", conn), overwrite = TRUE, temporary = FALSE)
#'
#'   t2 <- data.frame(key = c("A", "B"),
#'                   obs_2 = c("a", "b"),
#'                   valid_from  = as.Date(c("2021-01-01", "2021-01-01")),
#'                   valid_until = as.Date(c("2021-04-01", NA)))
#'   t2 <- dplyr::copy_to(conn, t2, id("test.SCDB_tmp2", conn), overwrite = TRUE, temporary = FALSE)
#'
#'   interlace_sql(list(t1, t2), by = "key")
#' }
#'
#' close_connection(conn)
#' @return          The combination of input queries with a single, interlaced
#'                  valid_from / valid_until time axis
#' @importFrom rlang .data
#' @export
interlace_sql <- function(tables, by = NULL, colnames = NULL) {

  # Check arguments
  checkmate::assert_character(by)
  # TODO: how to checkmate tables and colnames?

  # Check edgecase
  if (length(tables) == 1) return(purrr::pluck(tables, 1))

  # Parse inputs for colnames .from / .to columns
  from_cols <- seq_along(tables) |>
    purrr::map_chr(\(t) paste0("t", t, ".from")) |>
    purrr::map_chr(\(col) ifelse(col %in% names(colnames), colnames[col], "valid_from"))

  until_cols <- seq_along(tables) |>
    purrr::map_chr(\(t) paste0("t", t, ".until")) |>
    purrr::map_chr(\(col) ifelse(col %in% names(colnames), colnames[col], "valid_until"))


  # Rename valid_from / valid_until columns
  tables <- purrr::map2(tables, from_cols,
                        \(table, from_col)  table |> dplyr::rename(valid_from  = !!from_col))
  tables <- purrr::map2(tables, until_cols,
                        \(table, until_col) table |> dplyr::rename(valid_until = !!until_col))


  # Get all changes to valid_from / valid_until
  q1 <- tables |> purrr::map(\(table) {
    table |>
      dplyr::select(tidyselect::all_of(by), "valid_from")
  })
  q2 <- tables |>
    purrr::map(\(table) {
      table |>
        dplyr::select(tidyselect::all_of(by), "valid_until") |>
        dplyr::rename(valid_from = "valid_until")
    })
  t <- union(q1, q2) |> purrr::reduce(union)

  # Sort and find valid_until in the combined validities
  t <- t |>
    dplyr::group_by(dplyr::across(tidyselect::all_of(by))) |>
    dbplyr::window_order(.data$valid_from) |>
    dplyr::mutate(.row = dplyr::if_else(is.na(.data$valid_from),  # Some DB backends considers NULL to be the
                                        dplyr::n(),               # smallest, so we need to adjust for that
                                        dplyr::row_number() - ifelse(is.na(dplyr::first(.data$valid_from)), 1, 0)))

  t <- dplyr::left_join(t |>
                          dplyr::filter(.data$.row < dplyr::n()),
                        t |>
                          dplyr::filter(.data$.row > 1) |>
                          dplyr::mutate(.row = .data$.row - 1) |>
                          dplyr::rename("valid_until" = "valid_from"),
                        by = c(by, ".row")) |>
    dplyr::select(!".row") |>
    dplyr::ungroup() |>
    dplyr::compute()


  # Merge data onto the new validities using non-equi joins
  joiner <- \(.data, table) {
    .data |>
      dplyr::left_join(table,
                       suffix = c("", ".tmp"),
                       sql_on = paste0('"LHS"."', by, '" = "RHS"."', by, '" AND
                                      "LHS"."valid_from"  >= "RHS"."valid_from" AND
                                     ("LHS"."valid_until" <= "RHS"."valid_until" OR "RHS"."valid_until" IS NULL)')) |>
      dplyr::select(!tidyselect::ends_with(".tmp")) |>
      dplyr::relocate(tidyselect::starts_with("valid_"), .after = tidyselect::everything())
  }

  return(purrr::reduce(tables, joiner, .init = t))
}
