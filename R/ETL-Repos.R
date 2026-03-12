# Функция для получения списка репозиториев пользователя GitHub
get_github_repos <- function(input, token = NULL) {
  # Проверка и установка необходимых пакетов
  if (!requireNamespace("gh", quietly = TRUE)) {
    install.packages("gh")
  }
  if (!requireNamespace("httr", quietly = TRUE)) {
    install.packages("httr")
  }
  if (!requireNamespace("dplyr", quietly = TRUE)) {
    install.packages("dplyr")
  }
  library(gh)
  library(httr)
  library(dplyr)
  
  # Извлечение имени пользователя из ссылки или прямое использование
  username <- extract_github_username(input)
  if (is.null(username)) {
    stop("Не удалось извлечь имя пользователя из ссылки: ", input)
  }
  
  # Получение всех репозиториев (с автопагинацией через .limit = Inf)
  message("Запрашиваю репозитории пользователя ", username, "...")
  tryCatch({
    repos <- gh::gh(
      "GET /users/{username}/repos",
      username = username,
      .limit = Inf,
      .token = token,
      .progress = TRUE  # покажет прогресс, если много страниц
    )
  }, error = function(e) {
    stop("Ошибка при запросе к GitHub API: ", e$message)
  })
  
  # Если репозиториев нет, возвращаем пустую таблицу
  if (length(repos) == 0) {
    message("Репозитории не найдены.")
    return(data.frame())
  }
  
  # Преобразование списка репозиториев в data frame
  # Выбираем только нужные поля
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
  return(repos_df)
}

# Вспомогательная функция для извлечения имени пользователя из URL
extract_github_username <- function(input) {
  # Если входная строка уже похожа на имя (без слешей и протокола)
  if (grepl("^[a-zA-Z0-9_-]+$", input)) {
    return(input)
  }
  
  # Парсим URL
  parsed <- httr::parse_url(input)
  # Путь должен содержать что-то после домена
  if (!is.null(parsed$path)) {
    # Убираем ведущие и замыкающие слеши и разбиваем по слешам
    parts <- strsplit(gsub("^/|/$", "", parsed$path), "/")[[1]]
    # Первая часть пути — это имя пользователя
    if (length(parts) >= 1) {
      return(parts[1])
    }
  }
  return(NULL)
}

# Оператор %||% для подстановки значений по умолчанию (из base R)
`%||%` <- function(x, y) if (is.null(x)) y else x

# Пример использования:
# 1. Без токена (ограничение 60 запросов в час)
# repos_df <- get_github_repos("https://github.com/torvalds")
# head(repos_df)

# 2. С токеном (рекомендуется). Токен можно передать напрямую или установить
#    переменную окружения GITHUB_PAT (например, Sys.setenv(GITHUB_PAT = "ваш_токен"))
# repos_df <- get_github_repos("https://github.com/torvalds", token = Sys.getenv("GITHUB_PAT"))
