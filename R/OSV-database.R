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
  .osv_assert_scalar_character(url, "url")
  .osv_assert_scalar_character(destfile, "destfile")
  .osv_assert_scalar_logical(overwrite, "overwrite")

  if (!requireNamespace("curl", quietly = TRUE)) {
    stop("Package 'curl' is required but not installed.", call. = FALSE)
  }

  dest_dir <- dirname(destfile)
  if (!dir.exists(dest_dir)) dir.create(dest_dir, recursive = TRUE, showWarnings = FALSE)

  if (file.exists(destfile) && !isTRUE(overwrite)) {
    return(normalizePath(destfile, winslash = "/", mustWork = FALSE))
  }

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
  .osv_assert_scalar_character(zip_path, "zip_path")
  .osv_assert_scalar_character(output_dir, "output_dir")
  .osv_assert_scalar_logical(overwrite, "overwrite")

  if (!file.exists(zip_path)) {
    stop("OSV archive does not exist: ", zip_path, call. = FALSE)
  }

  if (dir.exists(output_dir) && isTRUE(overwrite)) {
    unlink(output_dir, recursive = TRUE, force = TRUE)
  }
  if (!dir.exists(output_dir)) {
    dir.create(output_dir, recursive = TRUE, showWarnings = FALSE)
  }

  if (requireNamespace("archive", quietly = TRUE)) {
    archive::archive_extract(zip_path, dir = output_dir)
  } else {
    utils::unzip(zip_path, exdir = output_dir)
  }

  normalizePath(output_dir, winslash = "/", mustWork = FALSE)
}

#' Build local CSV OSV index.
#'
#' Local index backend is removed. Use MongoDB backend instead.
#'
#' @export
build_osv_index <- function(...) {
  stop(
    "Local CSV index backend is removed. ",
    "Use `prepare_osv_database(..., backend='mongo')`.",
    call. = FALSE
  )
}

#' Load local OSV database metadata.
#'
#' Local backend is removed. Use MongoDB backend instead.
#'
#' @export
load_osv_database <- function(...) {
  stop(
    "Local OSV backend is removed. ",
    "Use `load_osv_mongo_database()`.",
    call. = FALSE
  )
}

#' Download, extract and import OSV database into MongoDB.
#'
#' @param db_dir Root directory for local OSV archive/extraction cache.
#' @param osv_url Source URL for OSV archive.
#' @param download_overwrite Whether to force archive redownload.
#' @param extract_overwrite Whether to force re-extraction.
#' @param backend Must be `"mongo"`.
#' @param mongo_url MongoDB connection URL.
#' @param mongo_db_name MongoDB database name.
#' @param mongo_collection MongoDB collection name with OSV vulnerabilities.
#' @param mongo_drop_existing Drop existing Mongo collection before import.
#' @param mongo_batch_size Number of documents per insert batch for Mongo import.
#' @param mongo_progress_every Progress interval for Mongo import.
#' @param ... Deprecated/ignored compatibility parameters.
#'
#' @return An `osv_mongo_database` object.
#' @export
prepare_osv_database <- function(
    db_dir = "osv_db",
    osv_url = "https://www.googleapis.com/download/storage/v1/b/osv-vulnerabilities/o/all.zip?alt=media",
    download_overwrite = FALSE,
    extract_overwrite = FALSE,
    backend = "mongo",
    mongo_url = "mongodb://localhost:27017",
    mongo_db_name = "osv",
    mongo_collection = "vulns",
    mongo_drop_existing = FALSE,
    mongo_batch_size = 50000L,
    mongo_progress_every = 50000L,
    ...
) {
  .osv_assert_scalar_character(db_dir, "db_dir")
  .osv_assert_scalar_numeric_ge(mongo_batch_size, "mongo_batch_size", min_value = 1)
  .osv_assert_scalar_numeric_ge(mongo_progress_every, "mongo_progress_every", min_value = 1)

  if (!identical(as.character(backend), "mongo")) {
    stop("Only MongoDB backend is supported.", call. = FALSE)
  }

  if (!dir.exists(db_dir)) dir.create(db_dir, recursive = TRUE, showWarnings = FALSE)

  zip_path <- file.path(db_dir, "all.zip")
  raw_dir <- file.path(db_dir, "raw")

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

  prepare_osv_mongo_database(
    osv_db = raw_dir,
    mongo_url = mongo_url,
    db_name = mongo_db_name,
    collection = mongo_collection,
    drop_existing = mongo_drop_existing,
    batch_size = mongo_batch_size,
    progress_every = mongo_progress_every
  )
}
