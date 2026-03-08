#' Download and load the OSV vulnerability dataset
#'
#' @description
#' Предоставляет интерфейс для упрощенной работы с модулем
#'
#' @details
#' Позволяет в несколько действий собрать информацию
#' об уязвимостях в пакетах разработчика
#'
#' @return Список уязвимостей в пакетах
#'
#' @export
#'



# Подключаем пакет
source("ETLBlacklist_download.R")
source("ETLBlacklist_load.R")
source("ETLBlacklist_search.R")
source("ETLBlacklist_utils.R")
source("ETLBlacklist_version_check.R")
source("ETLBlacklist_sboms.R")
source("ETL-Repos.R")

check4blacklist <- function(username, deepReseach = FALSE,
                            version = NULL, verbose = FALSE) {
  # Загружаем базу OSV (если не загружена)
  if (!exists("OSV_DATA", envir = .GlobalEnv)) {
    zip <- download_osv()
    load_osv(zip)
  }

  pkgs <- get_github_repos(username)

  packages <- data.frame()
  if (deepReseach) {
    packages <- sapply(pkgs$clone_url, generate_sbom_from_github)
    pkgs <- dplyr::bind_rows(packages)
    print(pkgs)
  }

  sapply(pkgs$name, pkg_summary)
}

pkg_summary <- function(pkg) {
  cat("=-=-=-=-=-=-=-= Package ", pkg, " =-=-=-=-=-=-=-=\n")
  # Поиск уязвимостей
  res <- search_vulnerabilities(pkg, version)

  # if (length(res) == 0) {
  #   cat("No vulnerabilities found for package:", pkg, "\n")
  #   quit(status = 0)
  # }


  return(res)
  ## Вывод
  for (entry in res) {
    cat("ID:", entry$id, "\n")
    cat("Summary:", entry$summary, "\n")
    cat("Published:", entry$published, "\n")
    # Получаем все версии из всех affected
    all_versions <- unlist(lapply(entry$affected, function(a) a$versions))
    if (length(all_versions) == 0) all_versions <- "N/A"

    cat("Versions:", paste(all_versions, collapse = ", "), "\n\n")
  }

  cat("Total vulnerabilities found:", length(res), "\n")
  # if (verbose) {
  # }
}
