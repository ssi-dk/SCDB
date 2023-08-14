# Yes, "zzz.R" is ACTUALLY what this file is typically named


### Adapted from the patch r-package github ###
# https://github.com/r-rudra/patch/blob/main/inst/embedded/usecases.R


# credit goes to creators of RStudio
# Check this
# https://github.com/rstudio/rstudio/blob/efc873ed38a738be9dc0612d70f21f59bfd58410/src -- continued below
# /cpp/session/modules/SessionRCompletions.R
# Above is AGPL-3.0

# Check this also https://support.rstudio.com/hc/en-us/articles/205273297-Code-Completion
# this code is having the same license as main package {patch}


globalVariables(c(".rs.addJsonRpcHandler",
                  ".rs.rpc.get_completions",
                  ".rs.addFunction",
                  ".rs.getCompletionsArgument",
                  ".auto_complete_db_table_names"))
.onAttach <- function(libname, pkgname) { # nocov start
  if (exists(".rs.addFunction")) {

    # Open connection and get db_tables
    conn <- tryCatch(get_connection(),
                     error = function(cond) {
                       packageStartupMessage("DB credentials is not correctly configured")
                       packageStartupMessage("Error message:")
                       stop(cond)},
                     warning = function(cond) {
                       packageStartupMessage("DB credentials is not correctly configured")
                       packageStartupMessage("Warning message:")
                       warning(cond)})

    # Determine available db_tables
    db_tables <- get_tables(conn) |>
      tidyr::unite("db_table", "schema", "table", sep = ".") |>
      dplyr::pull("db_table")

    # Close connection
    close_connection(conn)


    eval(parse(text = paste0('.auto_complete_db_table_names <- function() {

      .rs.addJsonRpcHandler(
        "get_completions",
        patch::patch_function(.rs.rpc.get_completions,
                              ".rs.getCompletionsEnvironmentVariables",

                              # addition portion
                              if (length(string) &&
                                  ("mg" %in% .packages()) &&
                                  string[[1]] == "get_table" &&
                                  numCommas[[1]] == 1) {

                                # Determine available db_tables
                                candidates <- c("', paste(db_tables, collapse = '", "'), '")
                                results <- .rs.selectFuzzyMatches(candidates, token)

                                return(.rs.makeCompletions(
                                  token = token,
                                  results = results,
                                  quote = TRUE,
                                  type = .rs.acCompletionTypes$VECTOR
                                ))
                              },
                              chop_locator_to = 1,
                              safely = TRUE
        )
      )
    }')))

    options(store_function_before_patch = TRUE)
    .auto_complete_db_table_names()
    options(store_function_before_patch = NULL)
  }
} # nocov end


.onDetach <- function(libpath) { # nocov start
  if (exists(".rs.addFunction")) {
    # reset auto-completion
    .rs.addJsonRpcHandler(
      "get_completions",
      patch::patch_function(.rs.rpc.get_completions)
    )
  }
} # nocov end

# Patch licence
# MIT License
#
# Copyright (c) 2020 Indranil Gayen
#
# Permission is hereby granted, free of charge, to any person obtaining a copy of this software and associated
# documentation files (the "Software"), to deal in the Software without restriction, including without limitation
# the rights to use, copy, modify, merge, publish, distribute, sublicense, and/or sell copies of the Software, and to
# permit persons to whom the Software is furnished to do so, subject to the following conditions:
#
# The above copyright notice and this permission notice shall be included in all copies or substantial portions of
# the Software.
#
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE
# WARRANTIES OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR
# COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR
# OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.
