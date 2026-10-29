FROM rocker/tidyverse
ENV TZ=Australia/Sydney
RUN date

# System and development dependencies
RUN apt-get update && apt-get install -y --no-install-recommends \
    ca-certificates \
    curl \
    docker.io \
    git \
    gnupg2 \
    libcurl4-gnutls-dev \
    openssh-client \
    sudo \
    xz-utils \
    && rm -rf /var/lib/apt/lists/*

# Install the latest stable arf release
ARG ARF_VERSION=0.6.0
ARG ARF_INSTALLER_SHA256=2bc9a643dc3a09022cac83d3340e0128f896886573af0ca3f871a9b73d3a24d6
RUN curl --proto '=https' --tlsv1.2 -LsSf \
      "https://github.com/eitsupi/arf/releases/download/v${ARF_VERSION}/arf-console-installer.sh" \
      -o /tmp/arf-installer.sh \
    && echo "${ARF_INSTALLER_SHA256}  /tmp/arf-installer.sh" | sha256sum -c - \
    && CARGO_DIST_FORCE_INSTALL_DIR=/usr/local sh /tmp/arf-installer.sh \
    && rm /tmp/arf-installer.sh \
    && arf --version

# Move pak.lock to the container
COPY . /app/
WORKDIR /app

# tex packages are installed in /root/bin so we have to make sure those
# packages accessible by adding that directory to the PATH variable.
ENV PATH="${PATH}:/root/bin"

RUN R -e 'install.packages("pak")'

RUN R -e 'getwd(); dir()'

# Remove dev package from pak.lock
RUN R -e 'pak::pak("jsonlite"); d <- jsonlite::read_json("pak.lock"); d$packages <- Filter(\(x) x$package != "SCDB", d$packages); print(unlist(Map(\(x) x$package, d$packages))); jsonlite::write_json(d, "pak.lock", auto_unbox = TRUE)'

# Install package dependencies
RUN R -e 'pkgs <- jsonlite::fromJSON("pak.lock")$packages; pak::pak(paste0(pkgs$package, "@" ,pkgs$version))'

# Install workflow and VS Code development dependencies
RUN R -e 'pak::pak(c("jsonlite", "rcmdcheck", "devtools", "lintr", "covr", "roxygen2", "pkgdown", "rmarkdown", "styler", "languageserver", "httpgd"))'

# Install the dev package
RUN R -e 'devtools::install()'

# Give the non-root Rocker user a writable personal package library
# for packages installed interactively while using the dev container.
RUN mkdir -p /home/rstudio/R/library \
    && chown -R rstudio:rstudio /home/rstudio/R
ENV R_LIBS_USER=/home/rstudio/R/library
