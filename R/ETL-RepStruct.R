get_repo_structure <- function(repo,
                               token = NULL,
                               recursive = FALSE,
                               get_content = FALSE,
                               ref = NULL) {
  
  # Проверка и установка пакетов
  required_packages <- c("gh", "dplyr", "purrr", "httr", "base64enc")
  for (pkg in required_packages) {
    if (!requireNamespace(pkg, quietly = TRUE)) {
      install.packages(pkg)
    }
  }
  library(gh)
  library(dplyr)
  library(purrr)
  library(httr)
  library(base64enc)
  
  # Вспомогательная функция для извлечения owner/repo из ссылки
  parse_repo <- function(input) {
    # Если уже в формате "owner/repo" (без протокола и слешей)
    if (grepl("^[a-zA-Z0-9_-]+/[a-zA-Z0-9_-]+$", input)) {
      parts <- strsplit(input, "/")[[1]]
      return(list(owner = parts[1], repo = parts[2]))
    }
    # Парсим URL
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
  
  # Внутренняя рекурсивная функция для получения содержимого по пути
  fetch_contents <- function(path = "") {
    # Формируем эндпоинт: если path пустой, то запрашиваем корень
    if (path == "") {
      endpoint <- "GET /repos/{owner}/{repo}/contents"
    } else {
      endpoint <- "GET /repos/{owner}/{repo}/contents/{path}"
    }
    
    tryCatch({
      contents <- gh::gh(
        endpoint,
        owner = owner,
        repo = repo_name,
        path = path,
        ref = ref,
        .token = token,
        .limit = Inf  # на случай, если файлов много (но обычно лимит 1000)
      )
    }, error = function(e) {
      # Если ошибка 404 - возможно, путь не существует
      stop("Ошибка при запросе ", path, ": ", e$message)
    })
    
    # GitHub API может вернуть либо объект (если это файл), либо массив (если папка)
    # Приведём к списку для единообразия
    if (!is.list(contents) || is.null(names(contents))) {
      # Это массив? Обычно для папки возвращается список элементов
      items <- contents
    } else {
      # Это объект (файл) - обернём в список
      items <- list(contents)
    }
    
    # Преобразуем каждый элемент в data.frame
    items_df <- map_df(items, function(item) {
      base <- data.frame(
        path = item$path %||% NA,
        type = item$type %||% NA,
        size = ifelse(item$type == "file", item$size %||% 0, NA),
        sha = item$sha %||% NA,
        download_url = ifelse(item$type == "file", item$download_url %||% NA, NA),
        stringsAsFactors = FALSE
      )
      
      # Если запрошено содержимое и это файл, пытаемся получить содержимое
      if (get_content && item$type == "file") {
        if (!is.null(item$content)) {
          # Содержимое уже есть в ответе (если файл небольшой)
          encoded <- item$content
          # Убираем переносы строк (base64 может содержать \n)
          encoded <- gsub("\n", "", encoded)
          decoded <- tryCatch({
            rawToChar(base64decode(encoded))
          }, error = function(e) {
            warning("Не удалось декодировать содержимое файла ", item$path, ": ", e$message)
            NA_character_
          })
          base$content <- decoded
        } else {
          # Если содержимого нет (файл слишком большой), можно попробовать отдельный запрос
          # но для простоты оставим NA
          base$content <- NA_character_
        }
      } else {
        base$content <- NA_character_
      }
      
      base
    })
    
    # Если нужна рекурсия, обрабатываем папки
    if (recursive) {
      # Отбираем папки
      dirs <- items_df %>% filter(type == "dir")
      if (nrow(dirs) > 0) {
        # Для каждой папки вызываем fetch_contents с её путём
        sub_dfs <- map(dirs$path, fetch_contents)
        items_df <- bind_rows(items_df, sub_dfs)
      }
    }
    
    return(items_df)
  }
  
  message("Получаю структуру репозитория ", owner, "/", repo_name, "...")
  result <- fetch_contents("")
  return(result)
}