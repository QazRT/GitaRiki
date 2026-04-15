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

  key <- paste(osv_mongo_db$mongo_url, osv_mongo_db$db_name, target_collection, sep = "|")
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

.osv_resolve_raw_dir_for_mongo <- function(osv_db) {
  raw_dir <- NULL

  if (inherits(osv_db, "osv_database") || (is.list(osv_db) && !is.null(osv_db$raw_dir))) {
    raw_dir <- .osv_to_scalar(osv_db$raw_dir)
  } else if (is.character(osv_db) && length(osv_db) == 1L && nzchar(osv_db)) {
    candidate <- normalizePath(osv_db, winslash = "/", mustWork = FALSE)
    candidate_raw <- file.path(candidate, "raw")
    raw_dir <- if (dir.exists(candidate_raw)) candidate_raw else candidate
  }

  if (!.osv_non_empty(raw_dir) || !dir.exists(raw_dir)) {
    stop(
      "`osv_db` must be a path to extracted OSV `raw` directory (or DB root containing `raw`).",
      call. = FALSE
    )
  }

  normalizePath(raw_dir, winslash = "/", mustWork = TRUE)
}

.osv_read_json_text <- function(path) {
  if (!file.exists(path)) return(NA_character_)
  sz <- suppressWarnings(as.integer(file.info(path)$size))
  if (!is.finite(sz) || is.na(sz) || sz <= 0L) return(NA_character_)

  con <- file(path, open = "rb")
  on.exit(close(con), add = TRUE)

  raw <- readBin(con, what = "raw", n = sz)
  txt <- trimws(rawToChar(raw))
  if (!nzchar(txt)) return(NA_character_)
  txt
}

.osv_collapse_values <- function(x) {
  if (is.null(x)) return("")
  vals <- suppressWarnings(as.character(unlist(x, use.names = FALSE)))
  vals <- vals[!is.na(vals) & nzchar(vals)]
  if (length(vals) == 0L) "" else paste(vals, collapse = ",")
}

.osv_reference_urls <- function(refs) {
  if (is.null(refs)) return("")
  if (is.character(refs) && length(refs) == 1L) return(refs)

  urls <- suppressWarnings(as.character(unlist(lapply(refs, function(r) {
    if (is.list(r) && !is.null(r$url)) return(r$url)
    NA_character_
  }), use.names = FALSE)))
  urls <- urls[!is.na(urls) & nzchar(urls)]
  if (length(urls) == 0L) "" else paste(urls, collapse = ",")
}

.osv_match_to_df <- function(doc, query_purl, query_version, match_info) {
  osv_id_value <- .osv_to_scalar(doc$id)
  if (!.osv_non_empty(osv_id_value)) {
    osv_id_value <- .osv_to_scalar(doc$osv_id)
  }

  data.frame(
    osv_id = osv_id_value,
    summary = .osv_to_scalar(doc$summary),
    details = .osv_to_scalar(doc$details),
    published = .osv_to_scalar(doc$published),
    modified = .osv_to_scalar(doc$modified),
    aliases = .osv_collapse_values(doc$aliases),
    references = .osv_reference_urls(doc$references),
    query_purl = query_purl,
    query_version = if (is.null(query_version)) NA_character_ else query_version,
    matched_purl = match_info$matched_purl,
    matched_package = match_info$matched_package,
    matched_ecosystem = match_info$matched_ecosystem,
    affected = if (isTRUE(match_info$affected)) TRUE else if (identical(match_info$affected, FALSE)) FALSE else NA,
    match_reason = match_info$reason,
    source_json = .osv_to_scalar(doc$json_path),
    stringsAsFactors = FALSE
  )
}

#' Create a MongoDB-backed OSV database from local OSV raw JSON files.
#'
#' Stores vulnerabilities close to the original OSV structure:
#' one vulnerability record (OSV ID) per document in a single collection.
#'
#' @param osv_db Path to extracted OSV `raw` directory (or DB root containing `raw`).
#' @param mongo_url MongoDB connection URL.
#' @param db_name MongoDB database name.
#' @param collection MongoDB collection name for vulnerability documents.
#' @param drop_existing Drop existing collection contents before import.
#' @param batch_size Number of documents inserted per batch.
#' @param progress_every Deprecated and ignored.
#' @param ... Deprecated/ignored compatibility parameters.
#'
#' @return An `osv_mongo_database` object.
#' @export
prepare_osv_mongo_database <- function(
    osv_db,
    mongo_url = "mongodb://localhost:27017",
    db_name = "osv",
    collection = "vulns",
    drop_existing = FALSE,
    batch_size = 50000L,
    progress_every = 50000L,
    ...
) {
  .osv_assert_scalar_character(mongo_url, "mongo_url")
  .osv_assert_scalar_character(db_name, "db_name")
  .osv_assert_scalar_character(collection, "collection")
  .osv_assert_scalar_logical(drop_existing, "drop_existing")
  .osv_assert_scalar_numeric_ge(batch_size, "batch_size", min_value = 1)
  .osv_assert_scalar_numeric_ge(progress_every, "progress_every", min_value = 1)

  if (!requireNamespace("mongolite", quietly = TRUE)) {
    stop("Package 'mongolite' is required for MongoDB backend.", call. = FALSE)
  }

  raw_dir <- .osv_resolve_raw_dir_for_mongo(osv_db)
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
  batch_len <- 0L

  flush_batch <- function(force = FALSE) {
    if (!force && batch_len < batch_cap) return(invisible(NULL))
    if (batch_len == 0L) return(invisible(NULL))

    payload <- batch_json[seq_len(batch_len)]
    batch_ok <- TRUE

    tryCatch(
      con_main$insert(payload),
      error = function(e) {
        batch_ok <<- FALSE
      }
    )

    if (isTRUE(batch_ok)) {
      inserted <<- inserted + batch_len
    } else {
      for (i in seq_along(payload)) {
        one_ok <- TRUE
        tryCatch(
          con_main$insert(payload[[i]]),
          error = function(e) {
            one_ok <<- FALSE
          }
        )
        if (isTRUE(one_ok)) {
          inserted <<- inserted + 1L
        } else {
          skipped <<- skipped + 1L
        }
      }
    }

    batch_len <<- 0L
    invisible(NULL)
  }

  for (f in files) {
    txt <- .osv_read_json_text(f)
    if (!.osv_non_empty(txt)) next

    batch_len <- batch_len + 1L
    batch_json[[batch_len]] <- txt
    flush_batch(force = FALSE)
  }

  flush_batch(force = TRUE)

  try(con_main$index(add = '{"id": 1}'), silent = TRUE)
  try(con_main$index(add = '{"modified": 1}'), silent = TRUE)
  try(con_main$index(add = '{"affected.package.purl": 1}'), silent = TRUE)
  try(con_main$index(add = '{"affected.package.ecosystem": 1}'), silent = TRUE)
  try(con_main$index(add = '{"affected.package.name": 1}'), silent = TRUE)

  if (skipped > 0L) {
    warning("Skipped malformed/unsupported JSON documents: ", skipped, call. = FALSE)
  }

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
#' @param validate Validate connection by reading one document.
#' @param ... Deprecated/ignored compatibility parameters.
#'
#' @return An `osv_mongo_database` object.
#' @export
load_osv_mongo_database <- function(
    mongo_url = "mongodb://localhost:27017",
    db_name = "osv",
    collection = "vulns",
    validate = TRUE,
    ...
) {
  .osv_assert_scalar_character(mongo_url, "mongo_url")
  .osv_assert_scalar_character(db_name, "db_name")
  .osv_assert_scalar_character(collection, "collection")
  .osv_assert_scalar_logical(validate, "validate")

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
  .osv_assert_scalar_character(purl, "purl")
  .osv_assert_scalar_logical(include_uncertain, "include_uncertain")

  parsed <- .osv_parse_purl(purl)
  if (!.osv_non_empty(version) && .osv_non_empty(parsed$version)) {
    version <- parsed$version
  }
  if (.osv_non_empty(version)) {
    version <- as.character(version)
  } else {
    version <- NULL
  }

  purl_norm <- .osv_normalize_purl(parsed$raw, drop_version = TRUE)
  ecosystem <- .osv_map_purl_type_to_ecosystem(parsed$type)
  candidate_names <- unique(.osv_candidate_package_names_from_purl(parsed))
  candidate_names <- candidate_names[nzchar(candidate_names)]

  escape_regex <- function(x) gsub("([\\^\\$\\.\\|\\(\\)\\[\\]\\*\\+\\?\\\\{}])", "\\\\\\1", x, perl = TRUE)

  con_main <- .osv_get_mongo_connection(osv_db, collection = osv_db$collection)
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
    affected = 1
  )

  fetch_docs <- function(query_obj) {
    con_main$find(
      query = jsonlite::toJSON(query_obj, auto_unbox = TRUE),
      fields = jsonlite::toJSON(fields, auto_unbox = TRUE)
    )
  }

  docs <- fetch_docs(list("affected.package.purl" = purl_norm))

  if ((!is.data.frame(docs) || nrow(docs) == 0L) &&
      .osv_non_empty(ecosystem) && length(candidate_names) > 0L) {
    docs <- fetch_docs(list(
      "affected.package.ecosystem" = ecosystem,
      "affected.package.name" = list(`$in` = as.list(candidate_names))
    ))
  }

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
    query_obj <- if (length(name_terms) == 1L) name_terms[[1L]] else list(`$or` = name_terms)
    docs <- fetch_docs(query_obj)
  }

  if (!is.data.frame(docs) || nrow(docs) == 0L) {
    return(data.frame())
  }

  out <- list()
  idx <- 0L

  for (i in seq_len(nrow(docs))) {
    affected <- docs$affected[[i]]
    if (is.null(affected) || !is.list(affected) || length(affected) == 0L) next

    match_info <- .osv_collect_entry_match(
      entry = list(affected = affected),
      query_purl = purl,
      version = version,
      include_uncertain = include_uncertain
    )
    if (is.null(match_info)) next

    idx <- idx + 1L
    out[[idx]] <- .osv_match_to_df(
      doc = docs[i, , drop = FALSE],
      query_purl = purl,
      query_version = version,
      match_info = match_info
    )
  }

  if (length(out) == 0L) {
    return(data.frame())
  }

  result <- unique(do.call(rbind, out))
  rownames(result) <- NULL

  if (is.finite(max_results)) {
    max_n <- max(0L, as.integer(max_results))
    if (nrow(result) > max_n) {
      result <- result[seq_len(max_n), , drop = FALSE]
    }
  }

  result
}
