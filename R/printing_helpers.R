#' cat printing with default new line
#'
#' @param ...  The normal input to cat
#' @param file Path of an output file to append the output to
#' @param sep  The separator given to cat
#' @export
printr <- function(..., file = "/dev/null", sep = "") {
  sink(file = file, split = TRUE, append = TRUE, type = "output")
  cat(..., "\n", sep = sep)
  sink()
}


#' print numbers with Danish thousands separator and decimal marks
#'
#' @param n   Number to format
#' @param fmt A printf-like format specifier
#' @export
format_danish <- function(n, fmt = "f") {
  return(formattable::comma(n, format = fmt, big.mark = ".", decimal.mark = ","))
}


#' formats timestamps as 2022-13-09 09:30,213
#'
#' @param ts  Timestamp to format
#' @export
format_timestamp <- function(ts) {
  return(stringr::str_replace(format(ts, "%F %H:%M:%OS3", locale = "en"), "\\.", ","))
}
