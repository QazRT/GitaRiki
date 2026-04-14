# ETL-Commit.R
# Функция для получения коммитов пользователя GitHub с сохранением в ClickHouse.
# При каждом вызове данные перезаписываются (предотвращает дублирование).

get_user_commits <- function(profile,
                             token = NULL,
                             repos = NULL,
                             max_repos = NULL,
                             include_stats = FALSE,
                             conn = NULL) {
  
  # Проверка подключения к ClickHouse
  if (is.null(conn)) {
    stop("Необходимо передать объект подключения ClickHouse в параметре 'conn'.")
  }
  
  # Проверка пакетов (без автоматической установки)
  required_packages <- c("gh", "dplyr", "purrr", "httr")
  for (pkg in required_packages) {
    if (!requireNamespace(pkg, quietly = TRUE)) {
      stop("Пакет '", pkg, "' не установлен. Установите вручную: install.packages('", pkg, "')")
    }
  }
  library(gh)
  library(dplyr)
  library(purrr)
  library(httr)
  
  # Подгружаем вспомогательный файл ETL-Repos.R (если он не адаптирован под ClickHouse,
  # то будет работать без кэширования, что допустимо)
  if (!file.exists("ETL-Repos.R")) {
    stop("Файл ETL-Repos.R не найден в текущей рабочей директории.")
  }
  source("ETL-Repos.R", local = TRUE)
  
  # Извлечение имени пользователя
  username <- extract_github_username(profile)
  if (is.null(username)) {
    stop("Не удалось извлечь имя пользователя из: ", profile)
  }
  
  # Имя таблицы ClickHouse
  table_name <- "github_user_commits"
  escaped_username <- gsub("'", "''", username)
  
  # Проверяем существование таблицы (функция из ClickHouseConnector.R)
  table_exists <- exists("table_exists_custom", mode = "function") &&
    table_exists_custom(conn, table_name)
  
  # Если таблица существует, удаляем старые записи для данного пользователя
  if (table_exists) {
    sql_delete <- paste0(
      "ALTER TABLE ", table_name,
      " DELETE WHERE profile = '", escaped_username, "'"
    )
    tryCatch(
      clickhouse_request(conn, sql_delete, parse_json = FALSE),
      error = function(e) warning("Не удалось удалить старые записи: ", e$message)
    )
  }
  
  # Определяем список репозиториев
  if (is.null(repos)) {
    # Используем get_github_repos из ETL-Repos.R (без кэша, если он не адаптирован)
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
  
  # Добавляем служебные колонки
  commits_df <- commits_df %>%
    mutate(
      profile = username,
      fetched_at = Sys.time()
    )
  
  # Загружаем в ClickHouse
  message("Сохраняю ", nrow(commits_df), " записей в ClickHouse...")
  load_df_to_clickhouse(
    df = commits_df,
    table_name = table_name,
    conn = conn,
    overwrite = FALSE,
    append = TRUE
  )
  
  return(commits_df)
}