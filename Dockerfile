FROM rocker/r-ver:4.4.2 AS builder

ENV DEBIAN_FRONTEND=noninteractive
ENV R_LIBS_SITE=/opt/R/site-library

RUN apt-get update && apt-get install -y --no-install-recommends \
    ca-certificates \
    curl \
    git \
    make \
    g++ \
    gfortran \
    pkg-config \
    libcurl4-openssl-dev \
    libssl-dev \
    libxml2-dev \
    libcairo2-dev \
    libfontconfig1-dev \
    libfreetype6-dev \
    libfribidi-dev \
    libharfbuzz-dev \
    libjpeg-dev \
    libpng-dev \
    libtiff5-dev \
    libbz2-dev \
    liblzma-dev \
    zlib1g-dev \
    libpcre2-dev \
    libicu-dev \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

COPY DESCRIPTION NAMESPACE ./

# Runtime + app dependencies (включая те, что используются в коде, но не перечислены в DESCRIPTION)
RUN mkdir -p /opt/R/site-library && \
    Rscript -e "install.packages(c( \
      'shiny','jsonlite','curl','mongolite','gh','zip','archive','utils', \
      'dplyr','purrr','httr','stringr','openssl','digest','DBI','markdown','commonmark' \
    ), repos='https://cloud.r-project.org')" && \
    Rscript -e "install.packages('.', repos=NULL, type='source')"

COPY . .

FROM gcr.io/distroless/cc-debian12:nonroot

ENV LANG=C.UTF-8
ENV LC_ALL=C.UTF-8
ENV R_HOME=/usr/local/lib/R
ENV R_LIBS_SITE=/opt/R/site-library
ENV PATH=/usr/local/bin:/usr/bin:/bin
ENV PORT=3838

WORKDIR /app

# R runtime
COPY --from=builder /usr/local/bin/R /usr/local/bin/R
COPY --from=builder /usr/local/bin/Rscript /usr/local/bin/Rscript
COPY --from=builder /usr/local/lib/R /usr/local/lib/R
COPY --from=builder /opt/R/site-library /opt/R/site-library

# App
COPY --from=builder /app /app

EXPOSE 3838

CMD ["/usr/local/bin/Rscript","-e","shiny::runApp('shiny_app', host='0.0.0.0', port=as.integer(Sys.getenv('PORT','3838')), launch.browser=FALSE)"]

