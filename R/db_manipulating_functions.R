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
#' conn <- get_connection()
#'
#' cpr3_t_person <- get_table(conn, "prod.cpr3_t_person") |> head(100) |> compute()
#'
#' # Filtering with null means no filtering is done
#' filter <- NULL
#' nrow(filter_keys(cpr3_t_person, filter)) == 100
#'
#' # Lets select those who have a birthdate of "2000-01-01" and have cprnr that
#' # starts with 'c'
#' filter <- cpr3_t_person |> filter(d_foddato == "2000-01-01", grepl("c%", v_pnr))
#' nrow(filter_keys(cpr3_t_person, filter)) # < 100
#'
#' close_connection(conn)
#' @importFrom rlang .data
#' @export
filter_keys <- function(.data, filters, by = NULL, na_by = NULL) {

  # Check arguments
  assert_data_like(.data)
  assert_data_like(filters, null.ok = TRUE)
  checkmate::assert_character(by, null.ok = TRUE)
  checkmate::assert_character(na_by, null.ok = TRUE)

  if (is.null(filters)) {
    return(.data)
  } else {
    if (is.null(by) && is.null(na_by)) {
      # Determine key types
      key_types <- filters |>
        dplyr::ungroup() |>
        dplyr::summarise(dplyr::across(.fns = ~ any(is.na(.), na.rm = TRUE))) |>
        tidyr::pivot_longer(tidyselect::everything(), names_to = "column_name", values_to = "is_na")

      by    <- key_types |> dplyr::filter(!.data$is_na) |> dplyr::pull("column_name")
      na_by <- key_types |> dplyr::filter(.data$is_na)  |> dplyr::pull("column_name")

      if (length(by) == 0)    by    <- NULL
      if (length(na_by) == 0) na_by <- NULL
    }
    return(inner_join(.data, filters, by = by, na_by = na_by))
  }
}


#' tidyr::unite for tbl_dbi
#'
#' @inheritParams tidyr::unite
#' @importFrom rlang `:=`
#' @exportS3Method tidyr::unite tbl_dbi
unite.tbl_dbi <- function(data, col, ..., sep = "_", remove = TRUE, na.rm = FALSE) { # nolint: object_name_linter

  # Modified from
  # https://stackoverflow.com/questions/48536983/how-to-concatenate-strings-of-multiple-  -- continued below
  # columns-from-table-in-sql-server-using-dp

  # Check arguments
  checkmate::assert_class(data, "tbl_dbi")
  #checkmate::assert_character(col) # I am not sure exactly what inputs tidyr::unite takes, so I cannot write a check
  checkmate::assert_character(sep)
  checkmate::assert_logical(remove)
  checkmate::assert_logical(na.rm)

  # Code below is adapted from tidyr::unite.data.frame
  rlang::check_dots_unnamed()

  if (rlang::dots_n(...) == 0) {
    from_vars <- rlang::set_names(seq_along(data), names(data))
  } else {
    from_vars <- colnames(dplyr::select(data, ...))
  }

  # We need add some support for how tidyr::unite accepts input of "col"
  col <- rlang::as_string(rlang::ensym(col))

  col_symbols <- purrr::map(from_vars, as.symbol)

  # We need to determine where col should be placed
  first_from <- which(colnames(data) %in% from_vars)[1]

  out <- data |>
    dplyr::mutate({{col}} := NULLIF(paste(!!!col_symbols, sep = sep), ""), .before = !!first_from)

  if (remove) out <- out |> dplyr::select(!tidyselect::all_of(from_vars))

  return(out)
}


#' Combine any number of sql queries, where each has their own time axis of
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
#' @return          The combination of input queries with a single, interlaced
#'                  valid_from / valid_until time axis
#' @examples
#' conn <- get_connection()
#'
#' x <- head(get_table(conn, "prod.cpr3_t_adresse"), 100)
#' y <- head(get_table(conn, "prod.cpr3_t_person"), 100)
#'
#' q <- interlace_sql(list(x, y),
#'                    by = "v_pnr",
#'                    colnames = c(t1.from = "d_tilflyt_kom_dato",
#'                                 t1.until = "d_tilflyt_dato", # columns of x
#'                                 t2.from = "d_foddato",       # columns of y
#'                                 t2.until = "d_status_hen_start"))
#'
#' close_connection(conn)
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
  tables <- tables |>
    purrr::map2(from_cols,  \(table, from_col)  table |> dplyr::rename(valid_from  = !!from_col)) |>
    purrr::map2(until_cols, \(table, until_col) table |> dplyr::rename(valid_until = !!until_col))


  # Get all changes to valid_from / valid_until
  q1 <- tables |> purrr::map(\(table) table |>
                               dplyr::select(tidyselect::all_of(by), "valid_from"))
  q2 <- tables |> purrr::map(\(table) table |>
                               dplyr::select(tidyselect::all_of(by), "valid_until") |>
                               dplyr::rename(valid_from = "valid_until"))
  t <- union(q1, q2) |> purrr::reduce(union)

  # Sort and find valid_until in the combined validities
  t <- t |>
    dplyr::group_by(dplyr::across(tidyselect::all_of(by))) |>
    dbplyr::window_order(.data$valid_from) |>
    dplyr::mutate(
      .row_number_id = dplyr::if_else(is.na(.data$valid_from),  # Some DB backends considers NULL to be the
                                      dplyr::n(),               # smallest, so we need to adjust for that
                                      dplyr::row_number() - ifelse(is.na(dplyr::first(.data$valid_from)), 1, 0)))

  t <- dplyr::left_join(t |>
                          dplyr::filter(.data$.row_number_id < dplyr::n()),
                        t |>
                          dplyr::filter(.data$.row_number_id > 1) |>
                          dplyr::mutate(.row_number_id = .data$.row_number_id - 1) |>
                          dplyr::rename("valid_until" = "valid_from"),
                        by = c(by, ".row_number_id")) |>
    dplyr::select(!".row_number_id") |>
    dplyr::ungroup() |>
    dplyr::compute()


  # Merge data onto the new validities using non-equi joins
  joiner <- \(.data, table) .data |>
    dplyr::left_join(table,
                     suffix = c("", ".tmp"),
                     sql_on = paste0(
                       '"LHS"."', by, '" = "RHS"."', by, '" AND
                        "LHS"."valid_from"  >= "RHS"."valid_from" AND
                       ("LHS"."valid_until" <= "RHS"."valid_until" OR "RHS"."valid_until" IS NULL)')) |>
    dplyr::select(!tidyselect::ends_with(".tmp")) |>
    dplyr::relocate(tidyselect::starts_with("valid_"), .after = tidyselect::everything())

  return(purrr::reduce(tables, joiner, .init = t))
}


#' Add a custom age group to data
#'
#' @param .data
#'   lazy_query to parse (must contain birthdate, valid_from and valid_until columns)
#' @param age_cuts
#'   The lower bound of the groups (0 is implicitly included)
#' @param age_group
#'   Name of the column to place the new age_groups in (default "age_group")
#' @param key
#'   Name of the column with personal identification number
#' @param birth
#'   Name of the column with birthdates
#' @param valid_from
#'   Name of the column with valid_from dates (default "valid_from")
#' @param valid_until
#'   Name of the column with valid_until dates (default "valid_until")
#' @return
#'   A copy of .data with the age_group added (interlaced) such that new valid_from and valid_until columns are added
#' @examples
#' conn <- get_connection()
#'
#' birth <- as.Date(c("1961-01-01", "1991-01-01"))
#'
#' data <- rbind(
#'    data.frame(key = c("A", "B"), birth,
#'               valid_from = birth, valid_until = birth + years(25), age_gr = "00-24"),
#'    data.frame(key = c("A", "B"), birth,
#'               valid_from = birth + years(25), valid_until = NA_Date_, age_gr = "25+")) %>%
#'   dplyr::copy_to(conn, .)
#'
#' data |>
#'      add_age_group(age_cuts = c(18, 60), key = "key")
#'
#' data |>
#'      add_age_group(age_cuts = c(18, 60), key = "key", age_group = "age_group_3")
#'
#' close_connection(conn)
#' @importFrom rlang `:=`
#' @export
add_age_group <- function(.data, age_cuts, age_group = "age_group", key = NULL, birth = "birth",
                          valid_from = "valid_from", valid_until = "valid_until") {

  # Check columns exist
  for (col in c(birth, valid_from, valid_until)) {
    if (!(col %in% colnames(.data))) stop("Could not find column '", col, "' in data")
  }

  # Determine the valid_to / valid_from times for the new age_group
  tmp <- .data |> dplyr::select(!!key, !!birth) |> dplyr::distinct() |> dplyr::compute()

  new_age_group <- tmp  |>
    dplyr::select(!!key, !!birth) |>
    dplyr::mutate(age = 0, !!valid_from := dbplyr::sql(!!birth))  |>
    dplyr::mutate(!!valid_until := as.Date(dbplyr::sql(!!valid_from) + lubridate::years(!!age_cuts[1])))

  for (i in seq_along(age_cuts)) {
    tt <- tmp  |>
      dplyr::select(!!key, !!birth)  |>
      dplyr::mutate(age = !!age_cuts[i],
                    !!valid_from := as.Date(dbplyr::sql(!!birth) + lubridate::years(!!age_cuts[i])))
    if (i < length(age_cuts)) {
      tt <- tt |> dplyr::mutate(!!valid_until := as.Date(dbplyr::sql(!!birth) + lubridate::years(!!age_cuts[i + 1])))
    }
    new_age_group <- dplyr::union_all(new_age_group, tt)
  }

  # Compute the new age_group
  new_age_group <- new_age_group  |>
    dplyr::mutate(!!age_group := !!aggregate_age_sql("age", age_cuts = age_cuts))  |>
    dplyr::select(!"age")


  # We use existing valid_from and valid_until timestamps to determine periods where new age_group should be inserted
  max_time <- .data  |>
    dplyr::ungroup() |>
    dplyr::summarize(max_time = max(
      pmax(dbplyr::sql(!!valid_from), dbplyr::sql(!!valid_until), na.rm = TRUE), na.rm = TRUE)) |>
    dplyr::pull(max_time)

  out <- interlace_sql(list(.data, new_age_group),
                       by = key,
                       colnames = c(t1.from = {{valid_from}}, t1.until = {{valid_until}},
                                    t2.from = {{valid_from}}, t2.until = {{valid_until}}))  |>
    dplyr::filter(valid_from <= max_time)  |>
    dplyr::mutate(valid_until = ifelse(max_time < valid_until, NA, valid_until))

  return(out)
}
