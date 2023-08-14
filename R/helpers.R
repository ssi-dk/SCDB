#' @import tidyverse dbplyr lubridate
NULL


#' Groups age (or other numerics) into a bins set by age_cuts
#' @param age      The vector of numbers to group
#' @template age_cuts
#' @return A factor of same length as age, but containing the name of the bin
#' @export
aggregate_age <- function(age, age_cuts) {
  checkmate::assert_numeric(age, any.missing = FALSE, lower = 0)

  age_cuts <- age_cuts[age_cuts > 0 & is.finite(age_cuts)]
  age_labels <- age_labels(age_cuts)

  age_group <- cut(age, breaks = c(0, age_cuts, Inf), include.lowest = TRUE, right = FALSE, labels = age_labels)
  return(age_group)
}


#' Groups age (or other numerics) into a bins set by age_cuts (builds query for database table object)
#' @param age_column The name of the column with the age group
#' @template age_cuts
#' @return A query to bin the age_column. Use in combination with "mutate" and "!!"
#' @examples
#' conn <- get_connection()
#'
#' get_table(conn, "prod.covid_19_maalgruppe") %>%
#'      mutate(age_group = !!aggregate_age_sql(vaccineage, age_cuts = c(30, 60)))
#'
#' close_connection(conn)
#' @export
aggregate_age_sql <- function(age_column, age_cuts) {

  age_column <- as.character(substitute(age_column))
  age_cuts <- age_cuts[age_cuts > 0 & is.finite(age_cuts)]
  age_labels <-  age_labels(age_cuts)

  q <- paste0("CASE\n")
  q <- paste0(q, "WHEN (", age_column, " < 0) THEN (NULL)\n")
  for (i in seq_along(age_cuts)) {
    q <- paste0(q, "WHEN (", age_column, " < ", age_cuts[i], ") THEN ('", age_labels[i], "')\n")
  }
  q <- paste0(q, "WHEN (", age_column, " >= ", age_cuts[i], ") THEN ('", age_labels[i + 1], "')\n")
  q <- paste0(q, "ELSE (NULL) END")

  return(dplyr::sql(q))
}


#' Provides age_labels that follows the mg standard
#' @template age_cuts
#' @return A vector of labels with zero-padded numerics so they can be sorted easily
#' @examples
#' age_labels(c(5, 12, 20, 30))
#' @export
age_labels <- function(age_cuts) {
  checkmate::assert_numeric(age_cuts, any.missing = FALSE, lower = 0, unique = TRUE, sorted = TRUE)

  age_cuts <- age_cuts[age_cuts > 0 & is.finite(age_cuts)]
  width <- nchar(as.character(max(c(0, age_cuts))))
  stringr::str_c(
    stringr::str_pad(c(0, age_cuts), width, pad = "0"),
    c(rep("-", length(age_cuts)), "+"),
    c(stringr::str_pad(age_cuts - 1, width, pad = "0"), ""))
}


#' nrow() but also works on remote tables
#'
#' @param .data lazy_query to parse
#' @return The number of records in the object
#' @examples
#' conn <- get_connection()
#'
#' get_table(conn, "mg.epicpr") %>%
#'      nrow()
#'
#' close_connection(conn)
#' @export
nrow <- function(.data) {
  if (inherits(.data, "tbl_dbi")) {
    return(dplyr::pull(dplyr::count(dplyr::ungroup(.data))))
  } else {
    return(base::nrow(.data))
  }
}


#' Checks if table contains historical data
#'
#' @template .data
#' @return TRUE if .data contains the columns: "checksum", "from_ts", and "until_ts". FALSE otherwise
#' @examples
#' conn <- get_connection()
#'
#' is.historical(get_table(conn, "prod.basis_samples")) # TRUE
#' is.historical(get_table(conn, "prod.cpr_status")) # FALSE
#'
#' close_connection(conn)
#' @export
is.historical <- function(.data) { # nolint: object_name_linter

  # Check arguments
  assert_data_like(.data)

  return(all(c("checksum", "from_ts", "until_ts") %in% colnames(.data)))
}



#' not-in operator
#' @inheritParams base::match
#' @export
`%notin%` <- function(x, table) {
  return(!(x %in% table))
}


#' checkmate helper: Assert "generic" data.table/data.frame/tbl/tibble type
#' @param .data Object to test if is data.table, data.frame, tbl or tibble
#' @param ...   Parameters passed to checkmate::check_*
#' @param add   `AssertCollection` to add assertions to
#' @export
assert_data_like <- function(.data, ..., add = NULL) {
  checkmate::assert(
    checkmate::check_class(.data, "tbl_dbi", ...),
    checkmate::check_data_frame(.data, ...),
    checkmate::check_data_table(.data, ...),
    checkmate::check_tibble(.data, ...),
    add = add)
}


#' checkmate helper: Assert "generic" timestamp type
#' @param timestamp Object to test if is POSIX or character
#' @param ...       parameters passed to checkmate::check_*
#' @param add       `AssertCollection` to add assertions to
#' @export
assert_timestamp_like <- function(timestamp, ..., add = NULL) {
  checkmate::assert(
    checkmate::check_posixct(timestamp, ...),
    checkmate::check_character(timestamp, ...),
    checkmate::check_date(timestamp, ...),
    add = add)
}


#' checkmate helper: Assert for "generic" db_table type
#' @param db_table Object to test if is of class "tbl_dbi" or character on form "schema.table"
#' @param ...      Parameters passed to checkmate::check_*
#' @param add      `AssertCollection` to add assertions to
#' @export
assert_dbtable_like <- function(db_table, ..., add = NULL) {
  checkmate::assert(
    checkmate::check_character(db_table, pattern = r"{^\w*.\w*$}", ...),
    checkmate::check_class(db_table, "Id", ...),
    checkmate::check_class(db_table, "tbl_dbi", ...),
    add = add)
}


#' checkmate helper: Assert for "generic" id structure
#' @param id   Object to test if is of class "Id" or character on form "schema.table"
#' @param ...  Parameters passed to checkmate::check_*
#' @param add `AssertCollection` to add assertions to
#' @export
assert_id_like <- function(id, ..., add = NULL) {
  checkmate::assert(
    checkmate::check_character(id, ...),
    checkmate::check_class(id, "Id", ...),
    add = add)
}
