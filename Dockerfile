FROM rocker/r-ver:4.4.2 AS builder

ARG SYFT_VERSION=v1.43.0

ENV DEBIAN_FRONTEND=noninteractive
ENV R_LIBS_SITE=/opt/R/site-library:/usr/local/lib/R/site-library:/usr/lib/R/site-library
ENV R_LIBS=/opt/R/site-library
ENV RENV_CONFIG_SANDBOX_ENABLED=false

RUN apt-get update && apt-get install -y --no-install-recommends \
    ca-certificates \
    curl \
    build-essential \
    gfortran \
    git \
    make \
    pkg-config \
    libarchive-dev \
    libbz2-dev \
    libcairo2-dev \
    libcurl4-openssl-dev \
    libfontconfig1-dev \
    libfreetype6-dev \
    libfribidi-dev \
    libharfbuzz-dev \
    libicu-dev \
    libjpeg-dev \
    liblzma-dev \
    libpcre2-dev \
    libpng-dev \
    libsasl2-dev \
    libssl-dev \
    libtiff5-dev \
    libxml2-dev \
    zlib1g-dev \
    fonts-liberation \
    fonts-dejavu-core \
    && r_apt_packages=" \
      r-cran-jsonlite r-cran-curl r-cran-mongolite r-cran-gh r-cran-shiny \
      r-cran-httr r-cran-dplyr r-cran-purrr r-cran-stringr r-cran-zip \
      r-cran-archive r-cran-dbi r-cran-openssl r-cran-digest r-cran-commonmark \
      r-cran-markdown r-cran-png r-cran-later r-cran-base64enc \
    " \
    && available_r_apt_packages="" \
    && for pkg in ${r_apt_packages}; do \
      if apt-cache show "${pkg}" >/dev/null 2>&1; then \
        available_r_apt_packages="${available_r_apt_packages} ${pkg}"; \
      else \
        echo "R apt package is not available, will try CRAN later: ${pkg}"; \
      fi; \
    done \
    && if [ -n "${available_r_apt_packages}" ]; then \
      apt-get install -y --no-install-recommends ${available_r_apt_packages}; \
    fi \
    && rm -rf /var/lib/apt/lists/*

COPY vendor/ /tmp/vendor/

RUN mkdir -p /opt/R/site-library && \
    Rscript -e "target_lib <- Sys.getenv('R_LIBS'); .libPaths(unique(c(target_lib, .libPaths()))); vendor_dir <- '/tmp/vendor'; required <- c('jsonlite','curl','mongolite','gh','shiny','httr','dplyr','purrr','stringr','zip','DBI','openssl','digest','commonmark','markdown','png','later','base64enc'); if (!requireNamespace('mongolite', quietly = TRUE)) { local_tarballs <- Sys.glob(file.path(vendor_dir, 'mongolite_*.tar.gz')); if (!length(local_tarballs)) stop('mongolite is missing and no vendor/mongolite_*.tar.gz was provided'); local_tarball <- sort(local_tarballs)[length(local_tarballs)]; message('Installing mongolite from vendored tarball: ', local_tarball); install.packages(local_tarball, lib = target_lib, repos = NULL, type = 'source') }; missing_required <- required[!vapply(required, requireNamespace, logical(1), quietly = TRUE)]; if (length(missing_required)) stop('Missing required R packages after apt/vendor install: ', paste(missing_required, collapse = ', '), '. Install them via apt or add vendored tarballs.'); cat('mongolite path:', find.package('mongolite'), '\n'); cat('shiny path:', find.package('shiny'), '\n'); cat('R library paths:', paste(.libPaths(), collapse = ' | '), '\n')"

RUN Rscript -e "target <- Sys.getenv('R_LIBS'); target_norm <- normalizePath(target, mustWork = TRUE); .libPaths(unique(c(target, .libPaths()))); site_paths <- setdiff(normalizePath(.libPaths(), mustWork = FALSE), normalizePath(R.home('library'), mustWork = FALSE)); ip <- installed.packages(lib.loc = site_paths); for (i in seq_len(nrow(ip))) { pkg <- ip[i, 'Package']; src <- file.path(ip[i, 'LibPath'], pkg); dst <- file.path(target, pkg); if (!file.exists(src) || normalizePath(dirname(src), mustWork = FALSE) == target_norm) next; if (!file.exists(dst)) { ok <- file.copy(src, target, recursive = TRUE, copy.date = TRUE); if (!ok) stop('Failed to mirror R package ', pkg, ' from ', src, ' to ', target) } }; .libPaths(target); required <- c('jsonlite','curl','gh','shiny','httr','dplyr','purrr','stringr','zip','DBI','openssl','digest','commonmark','markdown','png','later','base64enc'); missing <- required[!vapply(required, requireNamespace, logical(1), quietly = TRUE)]; if (length(missing)) stop('Missing mirrored R packages: ', paste(missing, collapse = ', ')); cat('Mirrored shiny to:', find.package('shiny'), '\n')"

RUN set -eux; \
    syft_arch="$(dpkg --print-architecture)"; \
    case "${syft_arch}" in \
      amd64) syft_arch="amd64" ;; \
      arm64) syft_arch="arm64" ;; \
      *) echo "Unsupported Syft architecture: ${syft_arch}" >&2; exit 1 ;; \
    esac; \
    curl --fail --location --show-error --retry 5 --retry-all-errors --connect-timeout 20 --max-time 180 \
      "https://github.com/anchore/syft/releases/download/${SYFT_VERSION}/syft_${SYFT_VERSION#v}_linux_${syft_arch}.tar.gz" \
      --output /tmp/syft.tar.gz; \
    tar -xzf /tmp/syft.tar.gz -C /usr/local/bin syft; \
    chmod 0755 /usr/local/bin/syft; \
    rm -f /tmp/syft.tar.gz; \
    syft version

WORKDIR /app

COPY . .

RUN mkdir -p /app/tmp /app/R/analysis_output

RUN sed -i 's/^Package: .*/Package: GitaRiki.OSAryans/' DESCRIPTION && \
    sed -i '/^[[:space:]]*archive,/d' DESCRIPTION && \
    Rscript -e "desc <- read.dcf('DESCRIPTION'); ar <- desc[1, 'Authors@R']; people <- eval(parse(text = ar)); cre_seen <- FALSE; people <- lapply(people, function(p) { r <- p$role; if ('cre' %in% r) { if (cre_seen) p$role <- setdiff(r, 'cre') else cre_seen <<- TRUE }; p }); lines <- readLines('DESCRIPTION', warn = FALSE); start <- grep('^Authors@R:', lines)[1]; end <- start; while (end < length(lines) && grepl('^[[:space:]]', lines[end + 1])) end <- end + 1; replacement <- c('Authors@R: c(', vapply(seq_along(people), function(i) paste0('    ', paste(deparse(people[[i]]), collapse = '\\n    '), if (i < length(people)) ',' else ''), character(1)), '    )'); prefix <- if (start > 1) lines[seq_len(start - 1)] else character(); suffix <- if (end < length(lines)) lines[(end + 1):length(lines)] else character(); writeLines(c(prefix, replacement, suffix), 'DESCRIPTION')" && \
    Rscript -e ".libPaths(unique(c(Sys.getenv('R_LIBS'), .libPaths()))); pkgs <- c('jsonlite','curl','mongolite','gh','shiny','httr','dplyr','purrr','zip','DBI'); missing <- pkgs[!vapply(pkgs, requireNamespace, logical(1), quietly = TRUE)]; if (length(missing)) stop('Missing R packages: ', paste(missing, collapse = ', ')); cat('R library paths:', paste(.libPaths(), collapse = ' | '), '\n')"

RUN R CMD INSTALL --library="${R_LIBS}" --no-byte-compile .

FROM gcr.io/distroless/cc-debian12:nonroot

ENV LANG=C.UTF-8
ENV LC_ALL=C.UTF-8
ENV R_HOME=/usr/local/lib/R
ENV R_LIBS_SITE=/opt/R/site-library:/usr/lib/R/site-library:/usr/local/lib/R/site-library
ENV R_LIBS=/opt/R/site-library
ENV LD_LIBRARY_PATH=/usr/local/lib/R/lib:/usr/local/lib:/usr/lib/x86_64-linux-gnu:/usr/lib/x86_64-linux-gnu/blas:/usr/lib/x86_64-linux-gnu/lapack:/lib/x86_64-linux-gnu
ENV PATH=/usr/local/bin:/usr/bin:/bin
ENV PORT=3838
ENV HOME=/app
ENV TMPDIR=/app/tmp

WORKDIR /app

# Runtime shared libraries and data needed by R packages built in the builder.
COPY --from=builder /lib/x86_64-linux-gnu /lib/x86_64-linux-gnu
COPY --from=builder /lib64 /lib64
COPY --from=builder /usr/lib/x86_64-linux-gnu /usr/lib/x86_64-linux-gnu
COPY --from=builder /usr/share/zoneinfo /usr/share/zoneinfo
COPY --from=builder /usr/share/ca-certificates /usr/share/ca-certificates
COPY --from=builder /etc/ssl/certs /etc/ssl/certs
COPY --from=builder /etc/alternatives /etc/alternatives
COPY --from=builder /etc/fonts /etc/fonts
COPY --from=builder /usr/share/fonts /usr/share/fonts
COPY --from=builder /var/cache/fontconfig /var/cache/fontconfig
COPY --from=builder /usr/lib/locale /usr/lib/locale

# A small shell is needed by several R runtime paths, including parallel workers.
COPY --from=builder /bin/dash /bin/dash
COPY --from=builder /bin/sh /bin/sh
COPY --from=builder /bin/bash /bin/bash

# R's base packages call a few POSIX utilities during startup and cleanup.
COPY --from=builder /usr/bin/env /usr/bin/env
COPY --from=builder /usr/bin/rm /usr/bin/rm
COPY --from=builder /usr/bin/sed /usr/bin/sed
COPY --from=builder /usr/bin/uname /usr/bin/uname
COPY --from=builder /usr/bin/which /usr/bin/which

# R runtime, installed packages, and the Linux x64 Syft executable.
COPY --from=builder /usr/local/bin/R /usr/local/bin/R
COPY --from=builder /usr/local/bin/Rscript /usr/local/bin/Rscript
COPY --from=builder /usr/local/bin/syft /usr/local/bin/syft
COPY --from=builder /usr/local/lib/R /usr/local/lib/R
COPY --from=builder /opt/R/site-library /opt/R/site-library
COPY --from=builder /usr/lib/R/site-library /usr/lib/R/site-library

# Application sources are writable because the app creates reports under R/analysis_output.
COPY --chown=65532:65532 --from=builder /app /app

RUN ["/usr/local/lib/R/bin/exec/R", "--slave", "--no-restore", "-e", ".libPaths(unique(c('/opt/R/site-library', '/usr/lib/R/site-library', '/usr/local/lib/R/site-library', .libPaths()))); library(utils); library(stats); stopifnot(requireNamespace('shiny', quietly = TRUE)); stopifnot(requireNamespace('mongolite', quietly = TRUE)); cat('Runtime lib paths:', paste(.libPaths(), collapse = ' | '), '\n'); cat('Runtime shiny path:', find.package('shiny'), '\n'); cat('Runtime mongolite path:', find.package('mongolite'), '\n')"]

EXPOSE 3838

CMD ["/usr/local/lib/R/bin/exec/R", "--slave", "--no-restore", "-e", ".libPaths(unique(c('/opt/R/site-library', '/usr/lib/R/site-library', '/usr/local/lib/R/site-library', .libPaths()))); shiny::runApp('/app/shiny_app', host = '0.0.0.0', port = as.integer(Sys.getenv('PORT', '3838')), launch.browser = FALSE)"]
