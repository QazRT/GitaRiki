.osv_mongo_connection_cache <- local({
  new.env(parent = emptyenv(), hash = TRUE)
})

.osv_get_mongo_connection <- function(osv_mongo_db, collection = NULL) {
  stopifnot(inherits(osv_mongo_db, "osv_mongo_database"))

  target_collection <- if (is.null(collection) || !nzchar(as.character(collection))) {
    osv_mongo_db$collection
  } else {
    as.character(collection)
  }

  key <- paste(
    osv_mongo_db$mongo_url,
    osv_mongo_db$db_name,
    target_collection,
    sep = "|"
  )

  if (exists(key, envir = .osv_mongo_connection_cache, inherits = FALSE)) {
    return(get(key, envir = .osv_mongo_connection_cache, inherits = FALSE))
  }

  if (!requireNamespace("mongolite", quietly = TRUE)) {
    stop("Package 'mongolite' is required for MongoDB backend.", call. = FALSE)
  }

  con <- mongolite::mongo(
    collection = target_collection,
    db = osv_mongo_db$db_name,
    url = osv_mongo_db$mongo_url
  )
  assign(key, con, envir = .osv_mongo_connection_cache)
  con
}

.osv_resolve_local_db_for_mongo <- function(osv_db) {
  if (inherits(osv_db, "osv_database")) {
    if (is.null(osv_db$raw_dir) || !dir.exists(osv_db$raw_dir)) {
      stop("`osv_db` must contain extracted `raw_dir` for Mongo import.", call. = FALSE)
    }
    return(osv_db)
  }

  if (is.character(osv_db) && length(osv_db) == 1L && nzchar(osv_db)) {
    return(load_osv_database(db_dir = osv_db, load_index = TRUE))
  }

  stop("`osv_db` must be a local `osv_database` object or path for Mongo import.", call. = FALSE)
}

.osv_read_json_text <- function(path) {
  if (!file.exists(path)) return(NA_character_)
  sz <- suppressWarnings(as.integer(file.info(path)$size))
  if (!is.finite(sz) || is.na(sz) || sz <= 0L) return(NA_character_)

  con <- file(path, open = "rb")
  on.exit(close(con), add = TRUE)

  raw <- readBin(con, what = "raw", n = sz)
  txt <- rawToChar(raw)
  txt <- trimws(txt)
  if (!nzchar(txt)) return(NA_character_)
  txt
}

.osv_mongo_format_duration <- function(seconds) {
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

#' Create a MongoDB-backed OSV database from local OSV raw JSON files.
#'
#' Stores vulnerabilities close to the original OSV structure:
#' one vulnerability record (OSV ID) per document in a single collection.
#'
#' @param osv_db Local prepared OSV database object or path (must contain `raw`).
#' @param mongo_url MongoDB connection URL.
#' @param db_name MongoDB database name.
#' @param collection MongoDB collection name for vulnerability documents.
#' @param lookup_collection Deprecated and ignored (kept for backward compatibility).
#' @param drop_existing Drop existing collection contents before import.
#' @param batch_size Number of documents inserted per batch.
#' @param progress_every Progress interval in inserted documents.
#'
#' @return An `osv_mongo_database` object.
#' @export
prepare_osv_mongo_database <- function(
    osv_db,
    mongo_url = "mongodb://localhost:27017",
    db_name = "osv",
    collection = "vulns",
    lookup_collection = NULL,
    drop_existing = FALSE,
    batch_size = 50000L,
    progress_every = 50000L
) {
  stopifnot(is.character(mongo_url), length(mongo_url) == 1L, nzchar(mongo_url))
  stopifnot(is.character(db_name), length(db_name) == 1L, nzchar(db_name))
  stopifnot(is.character(collection), length(collection) == 1L, nzchar(collection))
  stopifnot(is.logical(drop_existing), length(drop_existing) == 1L)
  stopifnot(is.numeric(batch_size), length(batch_size) == 1L, batch_size >= 1)
  stopifnot(is.numeric(progress_every), length(progress_every) == 1L, progress_every >= 1)

  if (.osv_non_empty(lookup_collection)) {
    message("`lookup_collection` is deprecated and ignored; using single-collection Mongo schema.")
  }

  if (!requireNamespace("mongolite", quietly = TRUE)) {
    stop("Package 'mongolite' is required for MongoDB backend.", call. = FALSE)
  }

  db_local <- .osv_resolve_local_db_for_mongo(osv_db)
  raw_dir <- db_local$raw_dir

  files <- list.files(raw_dir, pattern = "\\.json$", recursive = TRUE, full.names = TRUE)
  if (length(files) == 0L) {
    stop("No OSV JSON files found in raw_dir: ", raw_dir, call. = FALSE)
  }

  mongo_db <- load_osv_mongo_database(
    mongo_url = mongo_url,
    db_name = db_name,
    collection = collection,
    validate = FALSE
  )
  con_main <- .osv_get_mongo_connection(mongo_db, collection = collection)

  if (isTRUE(drop_existing)) {
    con_main$drop()
  }

  inserted <- 0L
  skipped <- 0L
  batch_cap <- as.integer(batch_size)
  batch_json <- character(batch_cap)
  batch_files <- character(batch_cap)
  batch_len <- 0L
  started_at <- Sys.time()

  flush_batch <- function(force = FALSE) {
    if (!force && batch_len < batch_cap) return(invisible(NULL))
    if (batch_len == 0L) return(invisible(NULL))

    payload <- batch_json[seq_len(batch_len)]
    payload_files <- batch_files[seq_len(batch_len)]
    batch_ok <- TRUE
    batch_err <- NULL

    tryCatch(
      con_main$insert(payload),
      error = function(e) {
        batch_ok <<- FALSE
        batch_err <<- conditionMessage(e)
      }
    )

    if (isTRUE(batch_ok)) {
      inserted <<- inserted + batch_len
    } else {
      failed_docs <- 0L
      for (i in seq_along(payload)) {
        one_ok <- TRUE
        one_err <- NULL
        tryCatch(
          con_main$insert(payload[[i]]),
          error = function(e) {
            one_ok <<- FALSE
            one_err <<- conditionMessage(e)
          }
        )
        if (isTRUE(one_ok)) {
          inserted <<- inserted + 1L
        } else {
          skipped <<- skipped + 1L
          failed_docs <- failed_docs + 1L
          if (failed_docs <= 3L) {
            warning(
              "Skipping malformed/unsupported JSON document: ",
              payload_files[[i]],
              " | ",
              one_err,
              call. = FALSE
            )
          }
        }
      }

      warning(
        "Batch insert failed; fallback single-document mode used for this batch. ",
        "Failed docs in batch: ", failed_docs, ". Error: ", batch_err,
        call. = FALSE
      )
    }

    batch_len <<- 0L

    if (inserted %% as.integer(progress_every) == 0L || force) {
      elapsed <- as.numeric(difftime(Sys.time(), started_at, units = "secs"))
      rate <- if (elapsed > 0) inserted / elapsed else NA_real_
      message(
        "[mongo-main] inserted ", inserted,
        " | skipped=", skipped,
        " | rate=", if (is.finite(rate) && !is.na(rate)) sprintf("%.1f", rate) else "n/a", " docs/s",
        " | elapsed=", .osv_mongo_format_duration(elapsed)
      )
    }

    invisible(NULL)
  }

  for (f in files) {
    txt <- .osv_read_json_text(f)
    if (!.osv_non_empty(txt)) next
    batch_len <- batch_len + 1L
    batch_json[[batch_len]] <- txt
    batch_files[[batch_len]] <- f
    flush_batch(force = FALSE)
  }

  flush_batch(force = TRUE)

  # Indexes for fast candidate lookup on original OSV structure.
  try(con_main$index(add = '{"id": 1}'), silent = TRUE)
  try(con_main$index(add = '{"modified": 1}'), silent = TRUE)
  try(con_main$index(add = '{"affected.package.purl": 1}'), silent = TRUE)
  try(con_main$index(add = '{"affected.package.ecosystem": 1}'), silent = TRUE)
  try(con_main$index(add = '{"affected.package.name": 1}'), silent = TRUE)

  message("[mongo] import completed: inserted=", inserted, ", skipped=", skipped, ", files=", length(files), ".")

  load_osv_mongo_database(
    mongo_url = mongo_url,
    db_name = db_name,
    collection = collection,
    validate = FALSE
  )
}

#' Connect to an existing MongoDB-backed OSV database.
#'
#' @param mongo_url MongoDB connection URL.
#' @param db_name MongoDB database name.
#' @param collection MongoDB collection name with vulnerabilities.
#' @param lookup_collection Deprecated and ignored (kept for backward compatibility).
#' @param validate Validate connection by reading one document.
#'
#' @return An `osv_mongo_database` object.
#' @export
load_osv_mongo_database <- function(
    mongo_url = "mongodb://localhost:27017",
    db_name = "osv",
    collection = "vulns",
    lookup_collection = NULL,
    validate = TRUE
) {
  stopifnot(is.character(mongo_url), length(mongo_url) == 1L, nzchar(mongo_url))
  stopifnot(is.character(db_name), length(db_name) == 1L, nzchar(db_name))
  stopifnot(is.character(collection), length(collection) == 1L, nzchar(collection))
  stopifnot(is.logical(validate), length(validate) == 1L)

  if (.osv_non_empty(lookup_collection)) {
    message("`lookup_collection` is deprecated and ignored; using single-collection Mongo schema.")
  }

  obj <- structure(
    list(
      backend = "mongo",
      mongo_url = mongo_url,
      db_name = db_name,
      collection = collection
    ),
    class = c("osv_mongo_database", "osv_database_backend")
  )

  if (isTRUE(validate)) {
    con <- .osv_get_mongo_connection(obj, collection = collection)
    tryCatch(
      con$find(limit = 1),
      error = function(e) {
        stop("MongoDB validation failed: ", conditionMessage(e), call. = FALSE)
      }
    )
  }

  obj
}

#' Search vulnerabilities in MongoDB-backed OSV database by purl and version.
#'
#' @param osv_db Mongo OSV object returned by `load_osv_mongo_database()`.
#' @param purl Package URL (`pkg:` format).
#' @param version Package version (optional).
#' @param include_uncertain Include records where version comparison is inconclusive.
#' @param max_results Max number of rows in result.
#'
#' @return Data frame with matched vulnerabilities.
#' @export
search_osv_vulnerabilities_mongo <- function(
    osv_db,
    purl,
    version = NULL,
    include_uncertain = TRUE,
    max_results = Inf
) {
  stopifnot(inherits(osv_db, "osv_mongo_database"))
  stopifnot(is.character(purl), length(purl) == 1L, nzchar(purl))
  stopifnot(is.logical(include_uncertain), length(include_uncertain) == 1L)

  parsed <- .osv_parse_purl(purl)
  if (!.osv_non_empty(version) && .osv_non_empty(parsed$version)) {
    version <- parsed$version
  }
  if (.osv_non_empty(version)) {
    version <- as.character(version)
  } else {
    version <- NULL
  }

  # OSV usually stores affected.package.purl as package identifier without version.
  purl_norm <- .osv_normalize_purl(parsed$raw, drop_version = TRUE)
  ecosystem <- .osv_map_purl_type_to_ecosystem(parsed$type)
  candidate_names <- unique(.osv_candidate_package_names_from_purl(parsed))
  candidate_names <- candidate_names[nzchar(candidate_names)]
  candidate_keys <- unique(stats::na.omit(vapply(
    candidate_names,
    function(nm) .osv_normalize_package_key(ecosystem, nm),
    character(1)
  )))

  escape_regex <- function(x) gsub("([\\^\\$\\.\\|\\(\\)\\[\\]\\*\\+\\?\\\\{}])", "\\\\\\1", x, perl = TRUE)

  fetch_docs <- function(query_obj, fields_obj) {
    con_main$find(
      query = jsonlite::toJSON(query_obj, auto_unbox = TRUE),
      fields = jsonlite::toJSON(fields_obj, auto_unbox = TRUE)
    )
  }
  fields <- list(
    `_id` = 0,
    id = 1,
    osv_id = 1,
    summary = 1,
    details = 1,
    published = 1,
    modified = 1,
    aliases = 1,
    references = 1,
    json_path = 1,
    affected = 1,
    affected_json = 1,
    package_purl_norm = 1,
    package_key = 1,
    package_name_lc = 1,
    package_purl_norms = 1,
    package_keys = 1,
    package_names_lc = 1
  )

  con_main <- .osv_get_mongo_connection(osv_db, collection = osv_db$collection)
  docs <- fetch_docs(
    query_obj = list("affected.package.purl" = purl_norm),
    fields_obj = fields
  )

  # Fallback 1: strict ecosystem + exact package name.
  if ((!is.data.frame(docs) || nrow(docs) == 0L) &&
      .osv_non_empty(ecosystem) && length(candidate_names) > 0L) {
    docs <- fetch_docs(
      query_obj = list(
        "affected.package.ecosystem" = ecosystem,
        "affected.package.name" = list(`$in` = as.list(candidate_names))
      ),
      fields_obj = fields
    )
  }

  # Fallback 2: ecosystem + case-insensitive package name.
  if ((!is.data.frame(docs) || nrow(docs) == 0L) &&
      .osv_non_empty(ecosystem) && length(candidate_names) > 0L) {
    name_terms <- lapply(candidate_names, function(nm) {
      list(
        "affected.package.ecosystem" = ecosystem,
        "affected.package.name" = list(
          `$regex` = paste0("^", escape_regex(nm), "$"),
          `$options` = "i"
        )
      )
    })
    name_query <- if (length(name_terms) == 1L) name_terms[[1L]] else list(`$or` = name_terms)
    docs <- fetch_docs(query_obj = name_query, fields_obj = fields)
  }

  # Legacy schema fallback (older imports before OSV-like document structure).
  if (!is.data.frame(docs) || nrow(docs) == 0L) {
    legacy_terms <- list(
      list(package_purl_norm = purl_norm),
      list(package_purl_norms = purl_norm)
    )
    if (length(candidate_keys) > 0L) {
      legacy_terms[[length(legacy_terms) + 1L]] <- list(
        package_key = list(`$in` = as.list(tolower(candidate_keys)))
      )
      legacy_terms[[length(legacy_terms) + 1L]] <- list(
        package_keys = list(`$in` = as.list(tolower(candidate_keys)))
      )
    }
    if (length(candidate_names) > 0L) {
      legacy_terms[[length(legacy_terms) + 1L]] <- list(
        package_name_lc = list(`$in` = as.list(tolower(candidate_names)))
      )
      legacy_terms[[length(legacy_terms) + 1L]] <- list(
        package_names_lc = list(`$in` = as.list(tolower(candidate_names)))
      )
    }
    docs <- fetch_docs(query_obj = list(`$or` = legacy_terms), fields_obj = fields)
  }

  if (!is.data.frame(docs) || nrow(docs) == 0L) {
    return(data.frame())
  }

  out <- list()
  idx <- 0L

  for (i in seq_len(nrow(docs))) {
    affected <- docs$affected[[i]]
    if (is.null(affected)) {
      affected_json <- .osv_to_scalar(docs$affected_json[[i]])
      if (.osv_non_empty(affected_json)) {
        affected <- tryCatch(
          jsonlite::fromJSON(affected_json, simplifyVector = FALSE),
          error = function(e) NULL
        )
      }
    }
    if (!is.null(affected) && is.list(affected) && !is.null(affected$package)) {
      # Legacy one-affected-per-document schema compatibility.
      affected <- list(affected)
    }
    if (is.null(affected)) next

    entry_like <- list(affected = affected)
    match_info <- .osv_collect_entry_match(
      entry = entry_like,
      query_purl = purl,
      version = version,
      include_uncertain = include_uncertain
    )
    if (is.null(match_info)) next

    aliases_val <- docs$aliases[[i]]
    aliases_chr <- if (is.null(aliases_val)) {
      ""
    } else if (is.character(aliases_val) && length(aliases_val) == 1L) {
      aliases_val
    } else {
      paste(as.character(unlist(aliases_val, use.names = FALSE)), collapse = ",")
    }

    refs_val <- docs$references[[i]]
    refs_chr <- ""
    if (!is.null(refs_val)) {
      if (is.character(refs_val) && length(refs_val) == 1L) {
        refs_chr <- refs_val
      } else {
        urls <- suppressWarnings(as.character(unlist(lapply(refs_val, function(r) {
          if (is.list(r) && !is.null(r$url)) return(r$url)
          NA_character_
        }), use.names = FALSE)))
        urls <- urls[!is.na(urls) & nzchar(urls)]
        refs_chr <- paste(urls, collapse = ",")
      }
    }

    osv_id_value <- .osv_to_scalar(docs$id[[i]])
    if (!.osv_non_empty(osv_id_value)) {
      osv_id_value <- .osv_to_scalar(docs$osv_id[[i]])
    }

    idx <- idx + 1L
    out[[idx]] <- data.frame(
      osv_id = osv_id_value,
      summary = .osv_to_scalar(docs$summary[[i]]),
      details = .osv_to_scalar(docs$details[[i]]),
      published = .osv_to_scalar(docs$published[[i]]),
      modified = .osv_to_scalar(docs$modified[[i]]),
      aliases = aliases_chr,
      references = refs_chr,
      query_purl = purl,
      query_version = if (is.null(version)) NA_character_ else version,
      matched_purl = match_info$matched_purl,
      matched_package = match_info$matched_package,
      matched_ecosystem = match_info$matched_ecosystem,
      affected = if (isTRUE(match_info$affected)) TRUE else if (identical(match_info$affected, FALSE)) FALSE else NA,
      match_reason = match_info$reason,
      source_json = .osv_to_scalar(docs$json_path[[i]]),
      stringsAsFactors = FALSE
    )
  }

  if (length(out) == 0L) {
    return(data.frame())
  }

  result <- unique(do.call(rbind, out))
  if (is.finite(max_results)) {
    max_n <- max(0L, as.integer(max_results))
    if (nrow(result) > max_n) {
      result <- result[seq_len(max_n), , drop = FALSE]
    }
  }

  rownames(result) <- NULL
  result
}
