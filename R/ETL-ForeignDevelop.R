# ETL-ForeignDevelop.R
# Функция для получения расширенной информации о пользователе GitHub
# (репозитории, подписки, звёзды, активность по коммитам)
# с сохранением в ClickHouse.

get_github_user_info <- function(profile,
                                 token = NULL,
                                 include_commit_stats = FALSE,
                                 max_commits = 1000,
                                 conn = NULL) {
  
  # Проверка подключения к ClickHouse
  if (is.null(conn)) {
    stop("Необходимо передать объект подключения ClickHouse в параметре 'conn'.")
  }
  
  # Проверка наличия пакетов (без автоматической установки)
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
  
  # Вспомогательные функции -------------------------------------------------
  `%||%` <- function(x, y) if (is.null(x)) y else x
  
  extract_github_username <- function(input) {
    if (grepl("^[a-zA-Z0-9_-]+$", input)) {
      return(input)
    }
    parsed <- httr::parse_url(input)
    if (!is.null(parsed$path)) {
      parts <- strsplit(gsub("^/|/$", "", parsed$path), "/")[[1]]
      if (length(parts) >= 1) {
        return(parts[1])
      }
    }
    return(NULL)
  }
  
  repo_to_df <- function(repo_list) {
    if (length(repo_list) == 0) return(data.frame())
    bind_rows(lapply(repo_list, function(r) {
      data.frame(
        full_name        = r$full_name %||% NA,
        html_url         = r$html_url %||% NA,
        description      = r$description %||% NA,
        created_at       = r$created_at %||% NA,
        updated_at       = r$updated_at %||% NA,
        language         = r$language %||% NA,
        stargazers_count = r$stargazers_count %||% 0,
        forks_count      = r$forks_count %||% 0,
        stringsAsFactors = FALSE
      )
    }))
  }
  
  # Извлечение имени пользователя
  username <- extract_github_username(profile)
  if (is.null(username)) {
    stop("Не удалось извлечь имя пользователя из: ", profile)
  }
  escaped_username <- gsub("'", "''", username)
  
  # Функция для удаления старых записей из таблицы перед вставкой
  delete_old_records <- function(tbl_name) {
    if (!exists("table_exists_custom", mode = "function")) return(FALSE)
    if (!table_exists_custom(conn, tbl_name)) return(FALSE)
    sql <- paste0("ALTER TABLE ", tbl_name, " DELETE WHERE profile = '", escaped_username, "'")
    tryCatch({
      clickhouse_request(conn, sql, parse_json = FALSE)
      TRUE
    }, error = function(e) {
      warning("Не удалось удалить старые записи из ", tbl_name, ": ", e$message)
      FALSE
    })
  }
  
  # Функция для загрузки data.frame в ClickHouse с предварительным удалением
  save_to_clickhouse <- function(df, tbl_name) {
    if (nrow(df) == 0) {
      message("Таблица ", tbl_name, ": нет данных для сохранения.")
      return(invisible(NULL))
    }
    delete_old_records(tbl_name)
    df <- df %>% mutate(profile = username, fetched_at = Sys.time())
    load_df_to_clickhouse(df = df, table_name = tbl_name, conn = conn,
                          overwrite = FALSE, append = TRUE)
    message("Сохранено в ", tbl_name, ": ", nrow(df), " записей.")
  }
  
  # 1. Репозитории, принадлежащие пользователю
  message("Запрашиваю репозитории, принадлежащие пользователю ", username, "...")
  owned_repos <- tryCatch({
    repos <- gh::gh("GET /users/{username}/repos",
                    username = username, .limit = Inf, .token = token, .progress = TRUE)
    repo_to_df(repos)
  }, error = function(e) {
    warning("Не удалось получить репозитории: ", e$message)
    data.frame()
  })
  
  # 2. Подписки (watched)
  message("Запрашиваю подписки пользователя ", username, "...")
  subscriptions <- tryCatch({
    subs <- gh::gh("GET /users/{username}/subscriptions",
                   username = username, .limit = Inf, .token = token, .progress = TRUE)
    repo_to_df(subs)
  }, error = function(e) {
    warning("Не удалось получить подписки: ", e$message)
    data.frame()
  })
  
  # 3. Звёзды (starred)
  message("Запрашиваю звёзды пользователя ", username, "...")
  starred <- tryCatch({
    stars <- gh::gh("GET /users/{username}/starred",
                    username = username, .limit = Inf, .token = token, .progress = TRUE)
    repo_to_df(stars)
  }, error = function(e) {
    warning("Не удалось получить звёзды: ", e$message)
    data.frame()
  })
  
  # 4. Поиск коммитов во всех публичных репозиториях
  message("Выполняю поиск коммитов автора ", username, " (макс. ", max_commits, ")...")
  commits_df <- data.frame()
  activity_summary <- data.frame()
  
  if (is.null(token)) {
    warning("Поиск коммитов требует аутентификации. Без токена эта часть будет пропущена.")
  } else {
    tryCatch({
      search_result <- gh::gh("GET /search/commits",
                              q = paste0("author:", username),
                              .token = token,
                              .limit = max_commits,
                              .progress = TRUE)
      items <- search_result$items
      
      if (length(items) > 0) {
        commits_df <- map_df(items, function(cmt) {
          repo_full <- cmt$repository$full_name %||% NA
          data.frame(
            repository = repo_full,
            sha        = cmt$sha %||% NA,
            date       = cmt$commit$author$date %||% NA,
            message    = cmt$commit$message %||% NA,
            url        = cmt$html_url %||% NA,
            stringsAsFactors = FALSE
          )
        })
        
        if (include_commit_stats && nrow(commits_df) > 0) {
          message("Запрашиваю статистику изменений для ", nrow(commits_df), " коммитов...")
          stats_list <- list()
          for (i in seq_len(nrow(commits_df))) {
            if (i %% 10 == 0) message("  Обработано ", i, " из ", nrow(commits_df))
            sha <- commits_df$sha[i]
            repo <- commits_df$repository[i]
            if (is.na(repo) || is.na(sha)) next
            tryCatch({
              cmt_detail <- gh::gh("GET /repos/{repo}/commits/{sha}",
                                   repo = repo, sha = sha, .token = token)
              s <- cmt_detail$stats %||% list(additions = 0, deletions = 0, total = 0)
              stats_list[[i]] <- data.frame(
                sha = sha,
                additions = s$additions %||% 0,
                deletions = s$deletions %||% 0,
                changes   = s$total %||% 0,
                stringsAsFactors = FALSE
              )
            }, error = function(e) {
              warning("Не удалось получить статистику для коммита ", sha, ": ", e$message)
              stats_list[[i]] <- data.frame(
                sha = sha,
                additions = NA,
                deletions = NA,
                changes = NA,
                stringsAsFactors = FALSE
              )
            })
          }
          stats_df <- bind_rows(stats_list)
          commits_df <- commits_df %>% left_join(stats_df, by = "sha")
        }
        
        # Агрегированная статистика по репозиториям
        if (nrow(commits_df) > 0) {
          if (include_commit_stats && "additions" %in% names(commits_df)) {
            activity_summary <- commits_df %>%
              group_by(repository) %>%
              summarise(
                total_commits = n(),
                first_commit = min(date, na.rm = TRUE),
                last_commit = max(date, na.rm = TRUE),
                total_additions = sum(additions, na.rm = TRUE),
                total_deletions = sum(deletions, na.rm = TRUE),
                total_changes = sum(changes, na.rm = TRUE),
                .groups = "drop"
              ) %>%
              arrange(desc(total_commits))
          } else {
            activity_summary <- commits_df %>%
              group_by(repository) %>%
              summarise(
                total_commits = n(),
                first_commit = min(date, na.rm = TRUE),
                last_commit = max(date, na.rm = TRUE),
                .groups = "drop"
              ) %>%
              arrange(desc(total_commits))
          }
        }
      } else {
        message("Коммитов не найдено.")
      }
    }, error = function(e) {
      warning("Ошибка при поиске коммитов: ", e$message)
    })
  }
  
  # Сохранение всех полученных данных в ClickHouse
  message("Сохраняю данные в ClickHouse...")
  save_to_clickhouse(owned_repos, "github_owned_repos")
  save_to_clickhouse(subscriptions, "github_subscriptions")
  save_to_clickhouse(starred, "github_starred")
  if (nrow(commits_df) > 0) {
    save_to_clickhouse(commits_df, "github_raw_commits")
  }
  if (nrow(activity_summary) > 0) {
    save_to_clickhouse(activity_summary, "github_commits_activity")
  }
  
  # Формируем результат (тот же, что и раньше)
  result <- list(
    owned_repos      = owned_repos,
    subscriptions    = subscriptions,
    starred          = starred,
    commits_activity = activity_summary,
    raw_commits      = commits_df
  )
  
  message("Сбор данных завершён.")
  return(result)
}