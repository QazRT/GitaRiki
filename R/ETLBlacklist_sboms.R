generate_sbom_from_github <- function(repo_url) {
  library(httr)
  library(jsonlite)
  library(archive)

  tmp <- tempdir()

  repo_url <- sub("\\.git$", "", repo_url)

  parts <- strsplit(repo_url, "/")[[1]]

  owner <- parts[length(parts) - 1]
  repo <- parts[length(parts)]

  repo_api <- paste0("https://api.github.com/repos/", owner, "/", repo)

  message("Detecting default branch...")

  res <- GET(repo_api)

  if (status_code(res) != 200) {
    stop("Cannot access GitHub API")
  }

  repo_info <- fromJSON(content(res, "text"))

  default_branch <- repo_info$default_branch

  message("Default branch: ", default_branch)

  zip_url <- paste0(
    "https://github.com/",
    owner, "/", repo,
    "/archive/refs/heads/",
    default_branch,
    ".zip"
  )

  zip_file <- file.path(tmp, paste0(repo, ".zip"))

  repo_dir <- file.path(tmp, repo)

  sbom_file <- file.path(tmp, "sbom.json")

  message("Downloading repository...")

  download.file(zip_url, zip_file, mode = "wb")

  fast_unzip_safe(zip_file, out_dir = repo_dir)


  message("Generating SBOM...")

  if (length(dir(repo_dir, all.files = TRUE, no.. = TRUE)) > 0) {
    system2(
      "..\\syft.exe",
      args = c(
        repo_dir,
        "-o",
        paste0("cyclonedx-json=", sbom_file)
      )
    )
    if (!file.exists(sbom_file)) {
      stop("SBOM generation failed")
    }
  } else {
    message("Repository is empty")
  }


  sbom <- fromJSON(sbom_file)

  packages <- data.frame(
    name = sbom$components$name,
    version = sbom$components$version,
    type = sbom$components$type,
    stringsAsFactors = FALSE
  )

  return(packages)
}


fast_unzip_safe <- function(zip_path, out_dir = tempdir()) {

  if (!is_zip(zip_path)) return(NULL)
  dir.create(out_dir, showWarnings = FALSE, recursive = TRUE)

  options(archive.extract.filter = function(path) gsub(":", "_", path))
  archive_extract(zip_path, dir = out_dir)
}

is_zip <- function(path) {
  sig <- readBin(path, "raw", 4)
  identical(sig, as.raw(c(0x50,0x4B,0x03,0x04)))
}