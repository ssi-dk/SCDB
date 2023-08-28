## R CMD check results

0 errors | 0 warnings | 1 note

* This is a resubmission release where the following issues have been adressed:

  1) Exported functions are documented with @return statements (\value generated in .Rd files)
We have added a test to our testing to prevent this issue going forward
Likewise, we now ensure .Rd files have examples

  2) Printing to console in get_table replaced with a message in the usecase where information needs to be passed to the user

  3) We now use requireNamespace instead of checking installed.packages in testing

  4) We have added a reference to the description for the methods used

* Additionally, we have made minor changes to the package.

  1) Added a vignette to describe functionality

  2) Expanded package Description to be more verbose

  3) Changed some Depends to Imports
