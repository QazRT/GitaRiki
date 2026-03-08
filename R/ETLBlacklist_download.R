#' Download and load the OSV vulnerability dataset
#'
#' @description
#' Downloads the `all.zip` archive from the OSV dataset and loads it into
#' memory. The archive is processed without writing to disk to optimize
#' performance. The function returns a list of parsed JSON vulnerability
#' records.
#'
#' @details
#' The function uses a temporary raw connection to avoid overhead from
#' filesystem operations. The returned data should be passed to
#' `build_osv_index()` to construct lookup indices for efficient querying.
#'
#' @return A list of parsed OSV JSON objects.
#'
#' @export
#'
download_osv <- function(url = "https://www.googleapis.com/download/storage/v1/b/osv-vulnerabilities/o/all.zip?alt=media",
                         destfile = tempfile(fileext = ".zip")) {
    message("Downloading OSV to: ", destfile)
    curl::curl_download(url, destfile)
    message("Downloaded to: ", destfile)
    return(destfile)
}
