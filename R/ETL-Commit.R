get_user_commits <- function(profile,
                             token = NULL,
                             repos = NULL,
                             max_repos = NULL) {
  
  # Проверка и загрузка зависимостей
  if (!requireNamespace("gh", quietly = TRUE)) install.packages("gh")
  if (!requireNamespace("dplyr", quietly = TRUE)) install.packages("dplyr")
  if (!requireNamespace("purrr", quietly = TRUE)) install.packages("purrr")
  if (!requireNamespace("httr", quietly = TRUE)) install.packages("httr")
  library(gh)
  library(dplyr)
  library(purrr)
  library(httr)
  
  # Проверка наличия файла с вспомогательными функциями
  if (!file.exists("ETL-Repos.R")) {
    stop("Файл ETL-Repos.R не найден в текущей рабочей директории.")
  }
  source("ETL-Repos.R", local = TRUE)
  
  # Извлечение имени пользователя
  username <- extract_github_username(profile)
  if (is.null(username)) {
    stop("Не удалось извлечь имя пользователя из: ", profile)
  }
  
  # Определяем список репозиториев для обработки
  if (is.null(repos)) {
    # Получаем все репозитории пользователя
    repos_df <- get_github_repos(username, token)
    if (nrow(repos_df) == 0) {
      message("У пользователя нет публичных репозиториев.")
      return(data.frame())
    }
    # Применяем ограничение max_repos, если задано
    if (!is.null(max_repos) && max_repos > 0 && max_repos < nrow(repos_df)) {
      repos_df <- head(repos_df, max_repos)
      message("Ограничено первыми ", max_repos, " репозиториями.")
    }
    repo_list <- repos_df$full_name
  } else {
    # Используем переданный список репозиториев
    repo_list <- repos
    message("Будут обработаны указанные репозитории (", length(repo_list), " шт.)")
  }
  
  all_commits <- list()
  
  for (repo_full in repo_list) {
    message("Обрабатываю репозиторий: ", repo_full)
    
    tryCatch({
      commits <- gh::gh(
        "GET /repos/{repo_full}/commits",
        repo_full = repo_full,
        author = username,
        .limit = Inf,
        .token = token,
        .progress = FALSE
      )
      
      if (length(commits) > 0) {
        repo_commits <- map_df(commits, function(cmt) {
          data.frame(
            repository = repo_full,
            sha        = cmt$sha %||% NA,
            date       = cmt$commit$author$date %||% NA,
            message    = cmt$commit$message %||% NA,
            url        = cmt$html_url %||% NA,
            stringsAsFactors = FALSE
          )
        })
        all_commits[[repo_full]] <- repo_commits
        message("  Найдено коммитов: ", nrow(repo_commits))
      } else {
        message("  Коммитов не найдено.")
      }
    }, error = function(e) {
      warning("Ошибка при обработке репозитория ", repo_full, ": ", e$message)
    })
  }
  
  if (length(all_commits) == 0) {
    message("Коммиты не найдены ни в одном репозитории.")
    return(data.frame())
  }
  
  commits_df <- bind_rows(all_commits)
  message("Всего собрано коммитов: ", nrow(commits_df))
  return(commits_df)
}