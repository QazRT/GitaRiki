# ETL-Repos.R
# Функция для получения списка репозиториев пользователя GitHub
# с сохранением в ClickHouse (перезапись данных при каждом вызове).

get_github_repos <- function(input, token = NULL, conn = NULL) {
  
  # Проверка подключения к ClickHouse
  if (is.null(conn)) {
    stop("Необходимо передать объект подключения ClickHouse в параметре 'conn'.")
  }
  
  # Проверка наличия пакетов (без автоматической установки)
  required_packages <- c("gh", "httr", "dplyr")
  for (pkg in required_packages) {
    if (!requireNamespace(pkg, quietly = TRUE)) {
      stop("Пакет '", pkg, "' не установлен. Установите вручную: install.packages('", pkg, "')")
    }
  }
  library(gh)
  library(httr)
  library(dplyr)
  
  # Вспомогательный оператор %||%
  `%||%` <- function(x, y) if (is.null(x)) y else x
  
  # Извлечение имени пользователя
  username <- extract_github_username(input)
  if (is.null(username)) {
    stop("Не удалось извлечь имя пользователя из: ", input)
  }
  
  # Имя таблицы ClickHouse
  table_name <- "github_repos"
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
  
  # Запрос к GitHub API
  message("Запрашиваю репозитории пользователя ", username, "...")
  tryCatch({
    repos <- gh::gh(
      "GET /users/{username}/repos",
      username = username,
      .limit = Inf,
      .token = token,
      .progress = TRUE
    )
  }, error = function(e) {
    stop("Ошибка при запросе к GitHub API: ", e$message)
  })
  
  if (length(repos) == 0) {
    message("Репозитории не найдены.")
    return(data.frame())
  }
  
  # Преобразование в data.frame
  repos_df <- bind_rows(lapply(repos, function(repo) {
    data.frame(
      name             = repo$name %||% NA,
      full_name        = repo$full_name %||% NA,
      description      = repo$description %||% NA,
      html_url         = repo$html_url %||% NA,
      ssh_url          = repo$ssh_url %||% NA,
      language         = repo$language %||% NA,
      stargazers_count = repo$stargazers_count %||% 0,
      forks_count      = repo$forks_count %||% 0,
      open_issues      = repo$open_issues_count %||% 0,
      created_at       = repo$created_at %||% NA,
      updated_at       = repo$updated_at %||% NA,
      pushed_at        = repo$pushed_at %||% NA,
      size             = repo$size %||% 0,
      stringsAsFactors = FALSE
    )
  }))
  
  message("Получено репозиториев: ", nrow(repos_df))
  
  # Добавляем служебные колонки
  repos_df <- repos_df %>%
    mutate(
      profile = username,
      fetched_at = Sys.time()
    )
  
  # Загружаем в ClickHouse
  message("Сохраняю ", nrow(repos_df), " записей в ClickHouse...")
  load_df_to_clickhouse(
    df = repos_df,
    table_name = table_name,
    conn = conn,
    overwrite = FALSE,
    append = TRUE
  )
  
  return(repos_df)
}

# Вспомогательная функция для извлечения имени пользователя из URL или строки
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

# Оператор %||% для удобства
`%||%` <- function(x, y) if (is.null(x)) y else x