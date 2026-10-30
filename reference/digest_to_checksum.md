# Computes an checksum from columns

Computes an checksum from columns

## Usage

``` r
digest_to_checksum(.data, col = "checksum", exclude = NULL, warn = TRUE)
```

## Arguments

- .data:

  (`data.frame(1)`, `tibble(1)`, `data.table(1)`, or `tbl_dbi(1)`)\
  Data object.

- col:

  (`character(1)`)\
  Name of the column to put the checksums in. Will be generated if
  missing.

- exclude:

  ([`character()`](https://rdrr.io/r/base/character.html))\
  Columns to exclude from the checksum generation.

- warn:

  (`logical(1)`)\
  Warn if `col` exists in the input data?

## Value

.data with a checksum column added.

## Details

In most cases, the md5 algorithm is used to compute the checksums. For
Microsoft SQL Server, the SHA-256 algorithm is used.

## Examples

``` r
  digest_to_checksum(mtcars)
#>                      mpg cyl  disp  hp drat    wt  qsec vs am gear carb
#> Mazda RX4           21.0   6 160.0 110 3.90 2.620 16.46  0  1    4    4
#> Mazda RX4 Wag       21.0   6 160.0 110 3.90 2.875 17.02  0  1    4    4
#> Datsun 710          22.8   4 108.0  93 3.85 2.320 18.61  1  1    4    1
#> Hornet 4 Drive      21.4   6 258.0 110 3.08 3.215 19.44  1  0    3    1
#> Hornet Sportabout   18.7   8 360.0 175 3.15 3.440 17.02  0  0    3    2
#> Valiant             18.1   6 225.0 105 2.76 3.460 20.22  1  0    3    1
#> Duster 360          14.3   8 360.0 245 3.21 3.570 15.84  0  0    3    4
#> Merc 240D           24.4   4 146.7  62 3.69 3.190 20.00  1  0    4    2
#> Merc 230            22.8   4 140.8  95 3.92 3.150 22.90  1  0    4    2
#> Merc 280            19.2   6 167.6 123 3.92 3.440 18.30  1  0    4    4
#> Merc 280C           17.8   6 167.6 123 3.92 3.440 18.90  1  0    4    4
#> Merc 450SE          16.4   8 275.8 180 3.07 4.070 17.40  0  0    3    3
#> Merc 450SL          17.3   8 275.8 180 3.07 3.730 17.60  0  0    3    3
#> Merc 450SLC         15.2   8 275.8 180 3.07 3.780 18.00  0  0    3    3
#> Cadillac Fleetwood  10.4   8 472.0 205 2.93 5.250 17.98  0  0    3    4
#> Lincoln Continental 10.4   8 460.0 215 3.00 5.424 17.82  0  0    3    4
#> Chrysler Imperial   14.7   8 440.0 230 3.23 5.345 17.42  0  0    3    4
#> Fiat 128            32.4   4  78.7  66 4.08 2.200 19.47  1  1    4    1
#> Honda Civic         30.4   4  75.7  52 4.93 1.615 18.52  1  1    4    2
#> Toyota Corolla      33.9   4  71.1  65 4.22 1.835 19.90  1  1    4    1
#> Toyota Corona       21.5   4 120.1  97 3.70 2.465 20.01  1  0    3    1
#> Dodge Challenger    15.5   8 318.0 150 2.76 3.520 16.87  0  0    3    2
#> AMC Javelin         15.2   8 304.0 150 3.15 3.435 17.30  0  0    3    2
#> Camaro Z28          13.3   8 350.0 245 3.73 3.840 15.41  0  0    3    4
#> Pontiac Firebird    19.2   8 400.0 175 3.08 3.845 17.05  0  0    3    2
#> Fiat X1-9           27.3   4  79.0  66 4.08 1.935 18.90  1  1    4    1
#> Porsche 914-2       26.0   4 120.3  91 4.43 2.140 16.70  0  1    5    2
#> Lotus Europa        30.4   4  95.1 113 3.77 1.513 16.90  1  1    5    2
#> Ford Pantera L      15.8   8 351.0 264 4.22 3.170 14.50  0  1    5    4
#> Ferrari Dino        19.7   6 145.0 175 3.62 2.770 15.50  0  1    5    6
#> Maserati Bora       15.0   8 301.0 335 3.54 3.570 14.60  0  1    5    8
#> Volvo 142E          21.4   4 121.0 109 4.11 2.780 18.60  1  1    4    2
#>                                             checksum
#> Mazda RX4           cc15ed979f975a3146e36b47b2ab0ebf
#> Mazda RX4 Wag       9738c8ae1037fa9f686c4de092f2cb4b
#> Datsun 710          01524a6ad903dccc72597f9dec5b925f
#> Hornet 4 Drive      096336924b8ef0abb69ce48fa8e967b7
#> Hornet Sportabout   156d66b5a25c6432856113300fafb9e1
#> Valiant             07b710a4eb565552a9368910b582a9ca
#> Duster 360          1855f1a3577b214fc997ee8f8d6fa4b0
#> Merc 240D           476a317a1bfc11bc14cb733b0c7ec648
#> Merc 230            f6801ecb87a9ac46e52167436073ff7a
#> Merc 280            d0d16567f6feb2749fbd851cf5933611
#> Merc 280C           96e1ac29420f4b2bdf0d44d213523f3a
#> Merc 450SE          0d6f19a3b4e6d345856e25721541d50e
#> Merc 450SL          8a22bd4e61cede746156a197c9167b35
#> Merc 450SLC         7cde159b5d54d8456510287d592e177c
#> Cadillac Fleetwood  c7c23741b3b507260200a7f0dd460c9c
#> Lincoln Continental 8ca072e4dead5168faf744a64158a7fc
#> Chrysler Imperial   f51de5b8dcb7c9cb42d241a5a88aa627
#> Fiat 128            438ba093f9a6b70c6e299f441e6b1a78
#> Honda Civic         f290c1c19224018b951fae74e54aeae9
#> Toyota Corolla      2c7a947184089c1e417840ddd01f4d76
#> Toyota Corona       37fc848c004b4e1108097213cf3254d9
#> Dodge Challenger    766486bd531e78bff5cda183321b38b1
#> AMC Javelin         2b3d01c07a9797c8eda0e41d730e9cf1
#> Camaro Z28          20e47bc65cfbfac2f39e76b8317228c0
#> Pontiac Firebird    6a823af356328cddf9113d76d0f29354
#> Fiat X1-9           3f0d3eb97097b28139b69c744ffd74b7
#> Porsche 914-2       403acdf33454004fad2aba60d863e273
#> Lotus Europa        0f5e497657ffd117f2354d39c4164ecd
#> Ford Pantera L      7232cf3cf178c45559cf020a7c8bdfb7
#> Ferrari Dino        96739c21cfe5d9939591c7c257a6fe77
#> Maserati Bora       6f492f2bc98fddc742d8b1f277861980
#> Volvo 142E          f390bc2b5608f4d23ad34b39d0b1d636
```
