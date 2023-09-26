## R CMD check results

0 errors | 0 warnings | 0 notes

This is a resubmission release where a failing test during CRAN incoming checks has been fixed.

Regarding non-standard file README.tex found at top level, we expect this to be caused by the failing test
preventing Rmarkdown from removing the .tex file after build (which should be default behavior).
