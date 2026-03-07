get_user_commits <- function(profile,
                             token = NULL,
                             repos = NULL,
                             max_repos = NULL,
                             include_stats = FALSE) {
  
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
    repos_df <- get_github_repos(username, token)
    if (nrow(repos_df) == 0) {
      message("У пользователя нет публичных репозиториев.")
      return(data.frame())
    }
    if (!is.null(max_repos) && max_repos > 0 && max_repos < nrow(repos_df)) {
      repos_df <- head(repos_df, max_repos)
      message("Ограничено первыми ", max_repos, " репозиториями.")
    }
    repo_list <- repos_df$full_name
  } else {
    repo_list <- repos
    message("Будут обработаны указанные репозитории (", length(repo_list), " шт.)")
  }
  
  if (include_stats) {
    message("ВНИМАНИЕ: запрошена статистика изменений. Это может занять дополнительное время.")
  }
  
  all_commits <- list()
  
  for (repo_full in repo_list) {
    message("Обрабатываю репозиторий: ", repo_full)
    
    tryCatch({
      # Получаем список коммитов автора
      commits <- gh::gh(
        "GET /repos/{repo_full}/commits",
        repo_full = repo_full,
        author = username,
        .limit = Inf,
        .token = token,
        .progress = FALSE
      )
      
      if (length(commits) > 0) {
        # Базовая информация о коммитах
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
        
        # Если нужна статистика изменений
        if (include_stats && nrow(repo_commits) > 0) {
          message("  Запрашиваю статистику для ", nrow(repo_commits), " коммитов...")
          stats <- map_df(repo_commits$sha, function(sha) {
            tryCatch({
              cmt_detail <- gh::gh(
                "GET /repos/{repo_full}/commits/{sha}",
                repo_full = repo_full,
                sha = sha,
                .token = token
              )
              stats <- cmt_detail$stats %||% list(additions = 0, deletions = 0, total = 0)
              data.frame(
                sha = sha,
                additions = stats$additions %||% 0,
                deletions = stats$deletions %||% 0,
                changes   = stats$total %||% 0,
                stringsAsFactors = FALSE
              )
            }, error = function(e) {
              warning("Не удалось получить статистику для коммита ", sha, ": ", e$message)
              data.frame(
                sha = sha,
                additions = NA,
                deletions = NA,
                changes = NA,
                stringsAsFactors = FALSE
              )
            })
          })
          # Присоединяем статистику
          repo_commits <- repo_commits %>%
            left_join(stats, by = "sha")
        }
        
        all_commits[[repo_full]] <- repo_commits
        message("  Найдено коммитов: ", nrow(repo_commits))
      } else {
        message("  Коммитов не найдено.")
      }
    }, error = function(e) {
      if (grepl("409", e$message)) {
        message("  Репозиторий пуст (Git Repository is empty). Пропускаю.")
      } else {
        warning("Ошибка при обработке репозитория ", repo_full, ": ", e$message)
      }
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