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
source(ETLBlacklist_download)
source(ETLBlacklist_load)
source(ETLBlacklist_search)
source(ETLBlacklist_utils)
source(ETLBlacklist_version_check)
# source(ETLRepos_connect) # заготовка для подключения сбора списка репозиториев пользователя

check4blacklist <- function(pkg, version = NULL, verbose = FALSE) {
  # Загружаем базу OSV (если не загружена)
  if (!exists("OSV_DATA", envir = .GlobalEnv)) {
    zip <- download_osv()
    load_osv(zip)
  }

  # Поиск уязвимостей
  res <- search_vulnerabilities(pkg, version)

  if (length(res) == 0) {
    cat("No vulnerabilities found for package:", pkg, "\n")
    quit(status = 0)
  }


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

  if (verbose) {
    cat("Total vulnerabilities found:", length(res), "\n")
  }
}
