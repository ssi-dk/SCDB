## R CMD check results

0 errors | 0 warnings | 1 note

* This is a resubmission release where the following issues have been adressed:

1) Exported functions are documented with @return statements (\Value generated in .Rd files)
We have added a test to our testing to prevent this issue going forward

2) Printing to console in get_table replaced with a message in the usecase where information needs to be passed to the user

3) We now use requireNamespace intead of checking installed.packages in testing