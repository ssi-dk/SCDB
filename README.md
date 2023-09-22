
<!-- README.md is generated from README.Rmd. Please edit that file. -->

# SCDB <a href="https://ssi-dk.github.io/SCDB/"><img src="man/figures/logo.png" alt="SCDB website" align="right" height="138"/></a>

<!-- badges: start -->

[![CRAN
status](https://www.r-pkg.org/badges/version/SCDB)](https://CRAN.R-project.org/package=SCDB)
[![R-CMD-check](https://github.com/ssi-dk/SCDB/actions/workflows/R-CMD-check.yaml/badge.svg)](https://github.com/ssi-dk/SCDB/actions/workflows/R-CMD-check.yaml)
[![Codecov test
coverage](https://codecov.io/gh/ssi-dk/SCDB/branch/main/graph/badge.svg)](https://app.codecov.io/gh/ssi-dk/SCDB?branch=main)

<!-- badges: end -->

## Overview

`SCDB` is a package for easily maintaining and updating data with a
slowly changing dimension. More specifically, the package facilitates
type-2 history for data warehouses and provide a number of
quality-of-life improvements for working with SQL databases within R.

To better understand what a slowly changing dimension is and how and
this packages provides it, see `vignette("basic_principles")`.

## Installation

``` r
# Install SCDB from CRAN:
install.packages("SCDB")

# Alternatively, install the development version from github:
# install.packages("devtools")
devtools::install_github("ssi-dk/SCDB")
```

## Usage

For basic usage examples, see `vignette("basic_principles")`.
