#' Download the official OSV database archive.
#'
#' @param url Source URL for the OSV archive.
#' @param destfile Destination path for the downloaded archive.
#' @param overwrite Whether to overwrite an existing file.
#'
#' @return Absolute path to the downloaded archive.
#' @export
download_osv_database <- function(
    url = "https://www.googleapis.com/download/storage/v1/b/osv-vulnerabilities/o/all.zip?alt=media",
    destfile = file.path("osv_db", "all.zip"),
    overwrite = FALSE
) {
  stopifnot(is.character(url), length(url) == 1L, nzchar(url))
  stopifnot(is.character(destfile), length(destfile) == 1L, nzchar(destfile))
  stopifnot(is.logical(overwrite), length(overwrite) == 1L)

  if (!requireNamespace("curl", quietly = TRUE)) {
    stop("Package 'curl' is required but not installed.", call. = FALSE)
  }

  dest_dir <- dirname(destfile)
  if (!dir.exists(dest_dir)) dir.create(dest_dir, recursive = TRUE, showWarnings = FALSE)

  if (file.exists(destfile) && !isTRUE(overwrite)) {
    message("OSV archive already exists: ", normalizePath(destfile, winslash = "/", mustWork = FALSE))
    return(normalizePath(destfile, winslash = "/", mustWork = FALSE))
  }

  message("Downloading OSV archive from: ", url)
  curl::curl_download(url = url, destfile = destfile, mode = "wb")

  normalizePath(destfile, winslash = "/", mustWork = FALSE)
}

#' Extract OSV archive to a local directory.
#'
#' @param zip_path Path to OSV `all.zip`.
#' @param output_dir Directory for extracted JSON files.
#' @param overwrite Whether to clear output dir before extraction.
#'
#' @return Absolute path to extraction directory.
#' @export
extract_osv_database <- function(zip_path, output_dir = file.path("osv_db", "raw"), overwrite = FALSE) {
  stopifnot(is.character(zip_path), length(zip_path) == 1L, nzchar(zip_path))
  stopifnot(is.character(output_dir), length(output_dir) == 1L, nzchar(output_dir))
  stopifnot(is.logical(overwrite), length(overwrite) == 1L)

  if (!file.exists(zip_path)) {
    stop("OSV archive does not exist: ", zip_path, call. = FALSE)
  }

  if (dir.exists(output_dir) && isTRUE(overwrite)) {
    unlink(output_dir, recursive = TRUE, force = TRUE)
  }
  if (!dir.exists(output_dir)) {
    dir.create(output_dir, recursive = TRUE, showWarnings = FALSE)
  }

  message("Extracting OSV archive to: ", normalizePath(output_dir, winslash = "/", mustWork = FALSE))
  if (requireNamespace("archive", quietly = TRUE)) {
    archive::archive_extract(zip_path, dir = output_dir)
  } else {
    utils::unzip(zip_path, exdir = output_dir)
  }

  normalizePath(output_dir, winslash = "/", mustWork = FALSE)
}

.osv_make_index_rows <- function(entry, json_path_rel) {
  affected <- entry$affected
  if (is.null(affected) || length(affected) == 0L) return(list())

  out <- vector("list", length(affected))
  idx <- 0L

  for (a in affected) {
    pkg <- .osv_null(a$package, list())
    ecosystem <- .osv_to_scalar(pkg$ecosystem)
    package_name <- .osv_to_scalar(pkg$name)
    package_purl <- .osv_to_scalar(pkg$purl)

    package_key <- .osv_normalize_package_key(ecosystem, package_name)
    purl_norm <- if (.osv_non_empty(package_purl)) {
      tryCatch(.osv_normalize_purl(package_purl, drop_version = TRUE), error = function(e) NA_character_)
    } else {
      NA_character_
    }

    idx <- idx + 1L
    out[[idx]] <- data.frame(
      json_path = json_path_rel,
      osv_id = .osv_to_scalar(entry$id),
      published = .osv_to_scalar(entry$published),
      modified = .osv_to_scalar(entry$modified),
      ecosystem = ecosystem,
      package_name = package_name,
      package_purl = package_purl,
      package_key = package_key,
      package_purl_norm = purl_norm,
      index_mode = "full",
      stringsAsFactors = FALSE
    )
  }

  out[seq_len(idx)]
}

.osv_rel_path <- function(file_path, raw_root) {
  file_norm <- normalizePath(file_path, winslash = "/", mustWork = FALSE)
  prefix <- paste0(raw_root, "/")
  if (startsWith(file_norm, prefix)) {
    substr(file_norm, nchar(prefix) + 1L, nchar(file_norm))
  } else {
    basename(file_norm)
  }
}

.osv_parse_file_to_rows <- function(file_path, raw_root) {
  entry <- tryCatch(
    jsonlite::fromJSON(file_path, simplifyVector = FALSE),
    error = function(e) NULL
  )
  if (is.null(entry)) return(list())

  json_rel <- .osv_rel_path(file_path, raw_root)
  .osv_make_index_rows(entry, json_rel)
}

.osv_parse_chunk_to_df <- function(file_chunk, raw_root) {
  rows <- list()
  k <- 0L

  for (file_path in file_chunk) {
    parsed <- .osv_parse_file_to_rows(file_path, raw_root)
    if (length(parsed) == 0L) next
    rows[(k + 1L):(k + length(parsed))] <- parsed
    k <- k + length(parsed)
  }

  if (k == 0L) return(NULL)
  do.call(rbind, rows)
}

.osv_path_chunk_to_df <- function(file_chunk, raw_root) {
  if (length(file_chunk) == 0L) return(NULL)

  rel_paths <- vapply(file_chunk, .osv_rel_path, character(1), raw_root = raw_root)
  path_parts <- strsplit(rel_paths, "/", fixed = TRUE)

  ecosystem <- vapply(path_parts, function(parts) {
    parts <- parts[nzchar(parts)]
    if (length(parts) >= 1L) parts[[1]] else NA_character_
  }, character(1))

  package_name <- vapply(path_parts, function(parts) {
    parts <- parts[nzchar(parts)]
    if (length(parts) >= 3L) {
      paste(parts[2:(length(parts) - 1L)], collapse = "/")
    } else {
      NA_character_
    }
  }, character(1))

  osv_id <- sub("\\.json$", "", basename(rel_paths), ignore.case = TRUE)
  package_key <- vapply(
    seq_along(package_name),
    function(i) .osv_normalize_package_key(ecosystem[[i]], package_name[[i]]),
    character(1)
  )

  data.frame(
    json_path = rel_paths,
    osv_id = osv_id,
    published = NA_character_,
    modified = NA_character_,
    ecosystem = ecosystem,
    package_name = package_name,
    package_purl = NA_character_,
    package_key = package_key,
    package_purl_norm = NA_character_,
    index_mode = "path",
    stringsAsFactors = FALSE
  )
}

.osv_write_index_chunk <- function(chunk_df, index_file, append = TRUE, writer = c("auto", "fwrite", "write.table")) {
  writer <- match.arg(writer)
  if (is.null(chunk_df) || nrow(chunk_df) == 0L) return(invisible(FALSE))

  use_fwrite <- FALSE
  if (identical(writer, "fwrite")) {
    use_fwrite <- requireNamespace("data.table", quietly = TRUE)
  } else if (identical(writer, "auto")) {
    use_fwrite <- requireNamespace("data.table", quietly = TRUE)
  }

  if (use_fwrite) {
    data.table::fwrite(
      x = chunk_df,
      file = index_file,
      append = isTRUE(append),
      col.names = !isTRUE(append),
      bom = TRUE
    )
  } else {
    utils::write.table(
      x = chunk_df,
      file = index_file,
      sep = ",",
      row.names = FALSE,
      col.names = !isTRUE(append),
      append = isTRUE(append),
      quote = TRUE,
      qmethod = "double",
      fileEncoding = "UTF-8"
    )
  }

  invisible(TRUE)
}

.osv_split_into_chunks <- function(x, chunk_size) {
  n <- length(x)
  if (n == 0L) return(list())
  idx <- ceiling(seq_len(n) / as.integer(chunk_size))
  split(x, idx)
}

.osv_is_absolute_path <- function(path) {
  grepl("^(?:[A-Za-z]:[\\\\/]|/)", path, perl = TRUE)
}

.osv_discover_json_files <- function(raw_dir, discovery = c("auto", "rg", "base")) {
  discovery <- match.arg(discovery)

  try_rg <- discovery %in% c("auto", "rg")
  if (try_rg) {
    rg_path <- Sys.which("rg")
    if (nzchar(rg_path)) {
      rg_out <- tryCatch(
        system2(
          rg_path,
          args = c("--files", "-g", "*.json", normalizePath(raw_dir, winslash = "/", mustWork = TRUE)),
          stdout = TRUE,
          stderr = TRUE
        ),
        error = function(e) structure(character(0), status = 1L)
      )
      rg_status <- attr(rg_out, "status")

      if (is.null(rg_status) || rg_status == 0L) {
        paths <- rg_out[nzchar(trimws(rg_out))]
        if (length(paths) > 0L) {
          paths <- gsub("\\\\", "/", paths)
          files <- vapply(paths, function(p) {
            candidate <- if (.osv_is_absolute_path(p)) p else file.path(raw_dir, p)
            normalizePath(candidate, winslash = "/", mustWork = FALSE)
          }, character(1))
          files <- files[grepl("\\.json$", files, ignore.case = TRUE)]
          return(list(files = unname(files), method = "rg"))
        }
      } else if (identical(discovery, "rg")) {
        warning("`rg` file discovery failed; falling back to base recursive listing.", call. = FALSE)
      }
    } else if (identical(discovery, "rg")) {
      warning("`rg` is not available; falling back to base recursive listing.", call. = FALSE)
    }
  }

  files <- list.files(raw_dir, pattern = "\\.json$", recursive = TRUE, full.names = TRUE)
  list(files = files, method = "base")
}

.osv_format_duration <- function(seconds) {
  seconds <- as.numeric(seconds)
  if (!is.finite(seconds) || is.na(seconds) || seconds < 0) return("n/a")
  if (seconds < 60) return(sprintf("%.0fs", seconds))
  mins <- floor(seconds / 60)
  secs <- round(seconds %% 60)
  if (mins < 60) return(sprintf("%dm %02ds", mins, secs))
  hrs <- floor(mins / 60)
  rem_mins <- mins %% 60
  sprintf("%dh %02dm %02ds", hrs, rem_mins, secs)
}

.osv_progress_bar <- function(ratio, width = 24L) {
  ratio <- max(0, min(1, as.numeric(ratio)))
  width <- as.integer(width)
  done <- floor(ratio * width)
  paste0(strrep("#", done), strrep("-", max(0L, width - done)))
}

#' Build a searchable OSV index from extracted JSON files.
#'
#' @param raw_dir Directory with extracted OSV JSON files.
#' @param index_file Target CSV path for the index.
#' @param rebuild Whether to rebuild an existing index.
#' @param files_per_chunk Number of JSON files per processing chunk.
#' @param workers Number of parallel workers for chunk processing.
#' @param writer CSV writer: `auto`, `fwrite` (if available), or `write.table`.
#' @param discovery File discovery mode: `auto`, `rg`, or `base`.
#' @param index_mode Index strategy: `full` parses JSON files, `path` builds a fast path-based index.
#' @param progress_every Progress message interval in processed files.
#'
#' @return Absolute path to the index CSV file.
#' @export
build_osv_index <- function(
    raw_dir = file.path("osv_db", "raw"),
    index_file = file.path("osv_db", "osv_index.csv"),
    rebuild = FALSE,
    files_per_chunk = 5000L,
    workers = 1L,
    writer = c("auto", "fwrite", "write.table"),
    discovery = c("auto", "rg", "base"),
    index_mode = c("path", "full"),
    progress_every = 5000L
) {
  stopifnot(is.character(raw_dir), length(raw_dir) == 1L, nzchar(raw_dir))
  stopifnot(is.character(index_file), length(index_file) == 1L, nzchar(index_file))
  stopifnot(is.logical(rebuild), length(rebuild) == 1L)
  stopifnot(is.numeric(files_per_chunk), files_per_chunk > 0)
  stopifnot(is.numeric(workers), workers > 0)
  stopifnot(is.numeric(progress_every), progress_every > 0)
  writer <- match.arg(writer)
  discovery <- match.arg(discovery)
  index_mode <- match.arg(index_mode)

  if (!dir.exists(raw_dir)) {
    stop("OSV raw directory does not exist: ", raw_dir, call. = FALSE)
  }

  index_dir <- dirname(index_file)
  if (!dir.exists(index_dir)) dir.create(index_dir, recursive = TRUE, showWarnings = FALSE)

  if (file.exists(index_file) && !isTRUE(rebuild)) {
    message("OSV index already exists: ", normalizePath(index_file, winslash = "/", mustWork = FALSE))
    return(normalizePath(index_file, winslash = "/", mustWork = FALSE))
  }
  if (file.exists(index_file) && isTRUE(rebuild)) {
    file.remove(index_file)
  }

  setup_started <- Sys.time()
  message("[setup] discovering JSON files (mode=", discovery, ")...")
  discovered <- .osv_discover_json_files(raw_dir = raw_dir, discovery = discovery)
  json_files <- discovered$files
  if (length(json_files) == 0L) {
    stop("No JSON files found in: ", raw_dir, call. = FALSE)
  }
  discover_elapsed <- as.numeric(difftime(Sys.time(), setup_started, units = "secs"))
  message("[setup] discovery done: ", length(json_files), " files in ", .osv_format_duration(discover_elapsed),
          " via ", discovered$method)

  raw_root <- normalizePath(raw_dir, winslash = "/", mustWork = TRUE)
  message("[setup] chunking file list...")
  chunks <- .osv_split_into_chunks(json_files, chunk_size = as.integer(files_per_chunk))
  chunk_sizes <- vapply(chunks, length, integer(1))
  message("[setup] chunks ready: ", length(chunks), " chunks, up to ", as.integer(files_per_chunk), " files/chunk")
  index_exists <- FALSE
  processed_files <- 0L
  total_files <- length(json_files)
  start_time <- Sys.time()
  next_progress_mark <- as.integer(progress_every)

  progress_tick <- function(force = FALSE, stage = "indexing") {
    if (!force && processed_files < next_progress_mark) return(invisible(NULL))

    elapsed <- as.numeric(difftime(Sys.time(), start_time, units = "secs"))
    rate <- if (elapsed > 0) processed_files / elapsed else NA_real_
    remaining <- max(0L, total_files - processed_files)
    eta <- if (is.finite(rate) && !is.na(rate) && rate > 0) remaining / rate else NA_real_
    ratio <- if (total_files > 0) processed_files / total_files else 1

    message(
      sprintf(
        "[%s] %5.1f%% [%s] %d/%d | %.1f files/s | elapsed %s | ETA %s",
        stage,
        ratio * 100,
        .osv_progress_bar(ratio),
        processed_files,
        total_files,
        if (is.finite(rate) && !is.na(rate)) rate else 0,
        .osv_format_duration(elapsed),
        .osv_format_duration(eta)
      )
    )

    while (next_progress_mark <= processed_files) {
      next_progress_mark <<- next_progress_mark + as.integer(progress_every)
    }

    invisible(NULL)
  }

  message("Building OSV index from ", total_files, " JSON files...")
  message("Index mode: ", if (as.integer(workers) > 1L) "parallel" else "single-thread", ", workers=", as.integer(workers),
          ", files_per_chunk=", as.integer(files_per_chunk), ", writer=", writer, ", strategy=", index_mode)
  message("[setup] full setup finished in ", .osv_format_duration(as.numeric(difftime(Sys.time(), setup_started, units = "secs"))), ".")

  if (identical(index_mode, "path")) {
    message("[path-index] building fast path-based index (without JSON parsing)...")
    for (chunk_idx in seq_along(chunks)) {
      res_df <- .osv_path_chunk_to_df(chunks[[chunk_idx]], raw_root)
      wrote <- .osv_write_index_chunk(
        chunk_df = res_df,
        index_file = index_file,
        append = index_exists,
        writer = writer
      )
      if (isTRUE(wrote)) index_exists <- TRUE

      processed_files <- processed_files + chunk_sizes[[chunk_idx]]
      processed_files <- min(total_files, processed_files)
      progress_tick(stage = "path")
    }

    progress_tick(force = TRUE, stage = "final")
    total_elapsed <- as.numeric(difftime(Sys.time(), start_time, units = "secs"))
    message("[done] OSV index created in ", .osv_format_duration(total_elapsed), ".")
    return(normalizePath(index_file, winslash = "/", mustWork = FALSE))
  }

  if (as.integer(workers) > 1L && requireNamespace("parallel", quietly = TRUE)) {
    worker_setup_started <- Sys.time()
    message("[setup] starting ", as.integer(workers), " parallel workers...")
    cl <- parallel::makeCluster(as.integer(workers))
    on.exit(parallel::stopCluster(cl), add = TRUE)
    message("[setup] workers started in ", .osv_format_duration(as.numeric(difftime(Sys.time(), worker_setup_started, units = "secs"))), ".")
    message("[setup] exporting helper functions to workers...")
    parallel::clusterExport(
      cl = cl,
      varlist = c(
        ".osv_make_index_rows",
        ".osv_rel_path",
        ".osv_parse_file_to_rows",
        ".osv_parse_chunk_to_df",
        ".osv_to_scalar",
        ".osv_non_empty",
        ".osv_normalize_package_key",
        ".osv_normalize_purl",
        ".osv_parse_purl",
        ".osv_null"
      ),
      envir = environment()
    )
    message("[setup] worker export complete.")

    total_chunks <- length(chunks)
    step <- as.integer(workers)
    for (start_idx in seq.int(1L, total_chunks, by = step)) {
      end_idx <- min(start_idx + step - 1L, total_chunks)
      chunk_batch <- chunks[start_idx:end_idx]

      batch_results <- parallel::parLapply(
        cl = cl,
        X = chunk_batch,
        fun = function(file_chunk, root) .osv_parse_chunk_to_df(file_chunk, root),
        root = raw_root
      )

      for (res_df in batch_results) {
        wrote <- .osv_write_index_chunk(
          chunk_df = res_df,
          index_file = index_file,
          append = index_exists,
          writer = writer
        )
        if (isTRUE(wrote)) index_exists <- TRUE
      }

      processed_files <- processed_files + sum(chunk_sizes[start_idx:end_idx])
      processed_files <- min(total_files, processed_files)
      progress_tick(stage = "parallel")
    }
  } else {
    for (chunk_idx in seq_along(chunks)) {
      res_df <- .osv_parse_chunk_to_df(chunks[[chunk_idx]], raw_root)
      wrote <- .osv_write_index_chunk(
        chunk_df = res_df,
        index_file = index_file,
        append = index_exists,
        writer = writer
      )
      if (isTRUE(wrote)) index_exists <- TRUE

      processed_files <- processed_files + chunk_sizes[[chunk_idx]]
      processed_files <- min(total_files, processed_files)
      progress_tick(stage = "single")
    }
  }

  progress_tick(force = TRUE, stage = "final")
  total_elapsed <- as.numeric(difftime(Sys.time(), start_time, units = "secs"))
  message("[done] OSV index created in ", .osv_format_duration(total_elapsed), ".")

  normalizePath(index_file, winslash = "/", mustWork = FALSE)
}

#' Load prepared OSV database metadata.
#'
#' @param db_dir Root directory of the prepared OSV database.
#' @param load_index Whether to load CSV index into memory.
#'
#' @return An `osv_database` object.
#' @export
load_osv_database <- function(db_dir = "osv_db", load_index = TRUE) {
  stopifnot(is.character(db_dir), length(db_dir) == 1L, nzchar(db_dir))
  stopifnot(is.logical(load_index), length(load_index) == 1L)

  index_path <- file.path(db_dir, "osv_index.csv")
  raw_dir <- file.path(db_dir, "raw")

  if (!dir.exists(raw_dir)) {
    stop("OSV raw directory not found: ", raw_dir, call. = FALSE)
  }
  if (!file.exists(index_path)) {
    stop("OSV index not found: ", index_path, call. = FALSE)
  }

  index_df <- NULL
  if (isTRUE(load_index)) {
    index_df <- utils::read.csv(index_path, stringsAsFactors = FALSE, na.strings = c("", "NA"))
  }

  structure(
    list(
      db_dir = normalizePath(db_dir, winslash = "/", mustWork = FALSE),
      raw_dir = normalizePath(raw_dir, winslash = "/", mustWork = FALSE),
      index_path = normalizePath(index_path, winslash = "/", mustWork = FALSE),
      index = index_df
    ),
    class = "osv_database"
  )
}

#' Download, extract and index OSV database.
#'
#' @param db_dir Root directory for local OSV database cache.
#' @param osv_url Source URL for OSV archive.
#' @param download_overwrite Whether to force archive redownload.
#' @param extract_overwrite Whether to force re-extraction.
#' @param rebuild_index Whether to force index rebuild.
#' @param index_files_per_chunk Number of JSON files per indexing chunk.
#' @param index_workers Number of parallel workers for indexing.
#' @param index_writer CSV writer for index: `auto`, `fwrite`, `write.table`.
#' @param index_discovery File discovery mode for indexing: `auto`, `rg`, `base`.
#' @param index_mode Index strategy: `full` (slower, full parse) or `path` (faster).
#' @param index_progress_every Progress update frequency in processed files.
#' @param load_index Whether to load index data frame into memory.
#' @param backend Storage backend for prepared DB: `local` (files + CSV) or `mongo`.
#' @param mongo_url MongoDB connection URL (used when `backend = "mongo"`).
#' @param mongo_db_name MongoDB database name (used when `backend = "mongo"`).
#' @param mongo_collection MongoDB main collection name with OSV vulnerabilities
#'   (used when `backend = "mongo"`).
#' @param mongo_lookup_collection Deprecated and ignored (kept for backward compatibility).
#' @param mongo_drop_existing Drop existing Mongo collection before import.
#' @param mongo_batch_size Number of documents per insert batch for Mongo import.
#' @param mongo_progress_every Progress interval for Mongo import.
#'
#' @return An `osv_database` object for `backend = "local"` or
#'   an `osv_mongo_database` object for `backend = "mongo"`.
#' @export
prepare_osv_database <- function(
    db_dir = "osv_db",
    osv_url = "https://www.googleapis.com/download/storage/v1/b/osv-vulnerabilities/o/all.zip?alt=media",
    download_overwrite = FALSE,
    extract_overwrite = FALSE,
    rebuild_index = FALSE,
    index_files_per_chunk = 5000L,
    index_workers = 1L,
    index_writer = c("auto", "fwrite", "write.table"),
    index_discovery = c("auto", "rg", "base"),
    index_mode = c("path", "full"),
    index_progress_every = 5000L,
    load_index = TRUE,
    backend = c("local", "mongo"),
    mongo_url = "mongodb://localhost:27017",
    mongo_db_name = "osv",
    mongo_collection = "vulns",
    mongo_lookup_collection = NULL,
    mongo_drop_existing = FALSE,
    mongo_batch_size = 50000L,
    mongo_progress_every = 50000L
) {
  stopifnot(is.character(db_dir), length(db_dir) == 1L, nzchar(db_dir))
  stopifnot(is.numeric(mongo_batch_size), length(mongo_batch_size) == 1L, mongo_batch_size >= 1)
  stopifnot(is.numeric(mongo_progress_every), length(mongo_progress_every) == 1L, mongo_progress_every >= 1)
  index_writer <- match.arg(index_writer)
  index_discovery <- match.arg(index_discovery)
  index_mode <- match.arg(index_mode)
  backend <- match.arg(backend)

  if (!dir.exists(db_dir)) dir.create(db_dir, recursive = TRUE, showWarnings = FALSE)

  zip_path <- file.path(db_dir, "all.zip")
  raw_dir <- file.path(db_dir, "raw")
  index_path <- file.path(db_dir, "osv_index.csv")

  if (!file.exists(zip_path) || isTRUE(download_overwrite)) {
    download_osv_database(
      url = osv_url,
      destfile = zip_path,
      overwrite = download_overwrite
    )
  }

  if (!dir.exists(raw_dir) || isTRUE(extract_overwrite)) {
    extract_osv_database(
      zip_path = zip_path,
      output_dir = raw_dir,
      overwrite = extract_overwrite
    )
  }

  if (identical(backend, "mongo")) {
    mongo_src <- structure(
      list(
        db_dir = normalizePath(db_dir, winslash = "/", mustWork = FALSE),
        raw_dir = normalizePath(raw_dir, winslash = "/", mustWork = TRUE),
        index_path = NA_character_,
        index = NULL
      ),
      class = "osv_database"
    )
    return(prepare_osv_mongo_database(
      osv_db = mongo_src,
      mongo_url = mongo_url,
      db_name = mongo_db_name,
      collection = mongo_collection,
      lookup_collection = mongo_lookup_collection,
      drop_existing = mongo_drop_existing,
      batch_size = mongo_batch_size,
      progress_every = mongo_progress_every
    ))
  }

  if (!file.exists(index_path) || isTRUE(rebuild_index)) {
    build_osv_index(
      raw_dir = raw_dir,
      index_file = index_path,
      rebuild = rebuild_index,
      files_per_chunk = index_files_per_chunk,
      workers = index_workers,
      writer = index_writer,
      discovery = index_discovery,
      index_mode = index_mode,
      progress_every = index_progress_every
    )
  }

  load_osv_database(db_dir = db_dir, load_index = load_index)
}
