FROM rocker/tidyverse
ENV TZ=Australia/Sydney
RUN date

# System dependencies (apt-get since r-base is debian based)
RUN apt-get update
RUN apt-get install -y libcurl4-gnutls-dev curl # curl files
RUN apt-get install -y docker.io # docker interface
RUN apt-get install -y gnupg2 # required to install odbc

# Move pak.lock to the container
COPY . /app/
WORKDIR /app

# tex packages are installed in /root/bin so we have to make sure those
# packages accessible by adding that directory to the PATH variable.
ENV PATH="/snap/bin:/home/runner/.local/bin:/opt/pipx_bin:/home/runner/.cargo/bin:/home/runner/.config/composer/vendor/bin:/usr/local/.ghcup/bin:/home/runner/.dotnet/tools:/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin:/usr/games:/usr/local/games:/snap/bin:/root/bin"

RUN R -e 'install.packages("pak")'

RUN R -e 'getwd(); dir()'

RUN R -e 'pak::pak("jsonlite"); d <- jsonlite::read_json("pak.lock"); d$packages <- Filter(\(x) x$package != "SCDB", d$packages); print(unlist(Map(\(x) x$package, d$packages))); jsonlite::write_json(d, "pak.lock", auto_unbox = TRUE)'

RUN R -e 'pak::lockfile_install(lockfile = "pak.lock")'

RUN R -e 'pak::pak(c("jsonlite", "rcmdcheck", "devtools", "lintr", "covr", "roxygen2", "pkgdown", "rmarkdown", "styler"))'

RUN R -e 'devtools::install()'

