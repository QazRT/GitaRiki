# ETL-RepStruct.R
# Функция для получения структуры файлов и папок репозитория GitHub
# с кэшированием результатов в ClickHouse.

# Предполагается, что файл ClickHouseConnector.R уже загружен через source()
# и доступны функции: query_clickhouse, load_df_to_clickhouse, table_exists_custom.

get_repo_structure <- function(repo,
                               token = NULL,
                               recursive = FALSE,
                               get_content = FALSE,
                               ref = NULL,
                               conn = NULL) {
  
  # Проверяем, что передан объект подключения ClickHouse
  if (is.null(conn)) {
    stop("Необходимо передать объект подключения ClickHouse в параметре 'conn'.")
  }
  
  # Загружаем необходимые пакеты (предполагаем, что они уже установлены)
  if (!requireNamespace("gh", quietly = TRUE)) stop("Пакет 'gh' не установлен.")
  if (!requireNamespace("dplyr", quietly = TRUE)) stop("Пакет 'dplyr' не установлен.")
  if (!requireNamespace("purrr", quietly = TRUE)) stop("Пакет 'purrr' не установлен.")
  if (!requireNamespace("httr", quietly = TRUE)) stop("Пакет 'httr' не установлен.")
  if (!requireNamespace("base64enc", quietly = TRUE)) stop("Пакет 'base64enc' не установлен.")
  
  library(gh)
  library(dplyr)
  library(purrr)
  library(httr)
  library(base64enc)
  
  # Вспомогательная функция для извлечения owner/repo из ссылки
  parse_repo <- function(input) {
    if (grepl("^[a-zA-Z0-9_-]+/[a-zA-Z0-9_-]+$", input)) {
      parts <- strsplit(input, "/")[[1]]
      return(list(owner = parts[1], repo = parts[2]))
    }
    parsed <- httr::parse_url(input)
    if (!is.null(parsed$path)) {
      path_parts <- strsplit(gsub("^/|/$", "", parsed$path), "/")[[1]]
      if (length(path_parts) >= 2) {
        return(list(owner = path_parts[1], repo = path_parts[2]))
      }
    }
    stop("Не удалось распарсить репозиторий: ", input, ". Используйте формат 'owner/repo' или ссылку.")
  }
  
  repo_info <- parse_repo(repo)
  owner <- repo_info$owner
  repo_name <- repo_info$repo
  
  # Имя таблицы в ClickHouse для хранения структур репозиториев
  table_name <- "github_repo_structure"
  
  # 1. Проверяем, есть ли уже данные в ClickHouse
  # Экранируем значения для безопасного SQL
  escaped_owner <- gsub("'", "''", owner)
  escaped_repo  <- gsub("'", "''", repo_name)
  escaped_ref   <- if (is.null(ref)) "NULL" else paste0("'", gsub("'", "''", ref), "'")
  
  sql_check <- paste0(
    "SELECT COUNT(*) AS cnt FROM ", table_name,
    " WHERE owner = '", escaped_owner, "'",
    " AND repo = '", escaped_repo, "'",
    " AND (ref = ", escaped_ref, " OR (ref IS NULL AND ", escaped_ref, " IS NULL))"
  )
  
  # Проверяем существование таблицы; если её нет – создадим позже
  table_exists <- exists("table_exists_custom", mode = "function") &&
    table_exists_custom(conn, table_name)
  
  if (table_exists) {
    cnt_df <- tryCatch(
      query_clickhouse(conn, sql_check),
      error = function(e) {
        warning("Не удалось проверить наличие данных в ClickHouse: ", e$message)
        data.frame(cnt = 0)
      }
    )
    if (nrow(cnt_df) > 0 && cnt_df$cnt[1] > 0) {
      message("Данные для ", owner, "/", repo_name,
              if (!is.null(ref)) paste0("@", ref) else "",
              " уже есть в ClickHouse. Загружаю из базы...")
      # Читаем все записи для этого репозитория и ref
      sql_read <- paste0(
        "SELECT * FROM ", table_name,
        " WHERE owner = '", escaped_owner, "'",
        " AND repo = '", escaped_repo, "'",
        " AND (ref = ", escaped_ref, " OR (ref IS NULL AND ", escaped_ref, " IS NULL))"
      )
      result <- query_clickhouse(conn, sql_read)
      return(result)
    }
  }
  
  # 2. Данных нет – выполняем запрос к GitHub API
  message("Получаю структуру репозитория ", owner, "/", repo_name, " через GitHub API...")
  
  # Внутренняя рекурсивная функция для получения содержимого по пути
  fetch_contents <- function(path = "") {
    endpoint <- if (path == "") {
      "GET /repos/{owner}/{repo}/contents"
    } else {
      "GET /repos/{owner}/{repo}/contents/{path}"
    }
    
    contents <- tryCatch({
      gh::gh(
        endpoint,
        owner = owner,
        repo = repo_name,
        path = path,
        ref = ref,
        .token = token,
        .limit = Inf
      )
    }, error = function(e) {
      stop("Ошибка при запросе ", path, ": ", e$message)
    })
    
    # Приводим к списку для единообразия
    if (!is.list(contents) || is.null(names(contents))) {
      items <- contents
    } else {
      items <- list(contents)
    }
    
    items_df <- map_df(items, function(item) {
      base <- data.frame(
        path = item$path %||% NA,
        type = item$type %||% NA,
        size = ifelse(item$type == "file", item$size %||% 0, NA),
        sha = item$sha %||% NA,
        download_url = ifelse(item$type == "file", item$download_url %||% NA, NA),
        stringsAsFactors = FALSE
      )
      
      if (get_content && item$type == "file") {
        if (!is.null(item$content)) {
          encoded <- gsub("\n", "", item$content)
          decoded <- tryCatch({
            rawToChar(base64decode(encoded))
          }, error = function(e) {
            warning("Не удалось декодировать содержимое файла ", item$path, ": ", e$message)
            NA_character_
          })
          base$content <- decoded
        } else {
          base$content <- NA_character_
        }
      } else {
        base$content <- NA_character_
      }
      
      base
    })
    
    if (recursive) {
      dirs <- items_df %>% filter(type == "dir")
      if (nrow(dirs) > 0) {
        sub_dfs <- map(dirs$path, fetch_contents)
        items_df <- bind_rows(items_df, sub_dfs)
      }
    }
    
    return(items_df)
  }
  
  result <- fetch_contents("")
  
  # Добавляем служебные колонки: owner, repo, ref, fetched_at
  result <- result %>%
    mutate(
      owner = owner,
      repo = repo_name,
      ref = if (is.null(ref)) NA_character_ else ref,
      fetched_at = Sys.time()
    )
  
  # 3. Загружаем результат в ClickHouse
  message("Загружаю структуру в ClickHouse...")
  # Если таблицы нет, она будет создана автоматически функцией load_df_to_clickhouse
  load_df_to_clickhouse(
    df = result,
    table_name = table_name,
    conn = conn,
    overwrite = FALSE,   # не перезаписываем всю таблицу
    append = TRUE        # добавляем новые записи
  )
  
  return(result)
}