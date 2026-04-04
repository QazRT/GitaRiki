get_github_social_links <- function(profile, token = NULL) {
  # Проверка и установка необходимых пакетов
  required_packages <- c("gh", "httr", "dplyr", "stringr", "purrr")
  for (pkg in required_packages) {
    if (!requireNamespace(pkg, quietly = TRUE)) {
      install.packages(pkg)
    }
  }
  library(gh)
  library(httr)
  library(dplyr)
  library(stringr)
  library(purrr)
  
  # Вспомогательная функция для извлечения имени пользователя
  extract_username <- function(input) {
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
  
  username <- extract_username(profile)
  if (is.null(username)) {
    stop("Не удалось извлечь имя пользователя из: ", profile)
  }
  
  # Получаем данные пользователя из API
  message("Получаю информацию о пользователе ", username, "...")
  user_data <- tryCatch({
    gh::gh("GET /users/{username}", username = username, .token = token)
  }, error = function(e) {
    stop("Ошибка при запросе к GitHub API: ", e$message)
  })
  
  # Инициализируем пустой список для ссылок
  links <- list()
  
  # 1. Поле blog
  if (!is.null(user_data$blog) && user_data$blog != "") {
    blog <- user_data$blog
    # Проверяем, является ли blog ссылкой (содержит http:// или https://)
    if (grepl("^https?://", blog)) {
      links <- append(links, list(data.frame(
        source = "blog",
        url = blog,
        type = classify_url(blog),
        username = NA_character_,
        stringsAsFactors = FALSE
      )))
    } else {
      # Возможно, это просто текст, но может быть ссылкой без протокола
      # Попробуем добавить https:// и проверить
      if (grepl("\\.", blog)) {
        tentative_url <- paste0("https://", blog)
        links <- append(links, list(data.frame(
          source = "blog",
          url = tentative_url,
          type = classify_url(tentative_url),
          username = NA_character_,
          stringsAsFactors = FALSE
        )))
      } else {
        # Иначе сохраняем как текст, но без ссылки
        links <- append(links, list(data.frame(
          source = "blog",
          url = NA_character_,
          type = "text",
          username = NA_character_,
          stringsAsFactors = FALSE
        )))
      }
    }
  }
  
  # 2. Поле twitter_username
  if (!is.null(user_data$twitter_username) && user_data$twitter_username != "") {
    tw <- user_data$twitter_username
    links <- append(links, list(data.frame(
      source = "twitter",
      url = paste0("https://twitter.com/", tw),
      type = "twitter",
      username = tw,
      stringsAsFactors = FALSE
    )))
  }
  
  # 3. Поле bio
  if (!is.null(user_data$bio) && user_data$bio != "") {
    bio <- user_data$bio
    
    # Ищем все URL в тексте (http/https)
    url_pattern <- "https?://[^\\s]+"
    urls <- str_extract_all(bio, url_pattern)[[1]]
    if (length(urls) > 0) {
      for (u in urls) {
        links <- append(links, list(data.frame(
          source = "bio_url",
          url = u,
          type = classify_url(u),
          username = NA_character_,
          stringsAsFactors = FALSE
        )))
      }
    }
    
    # Ищем паттерны социальных сетей без протокола (например, t.me/username, vk.com/id123, @username)
    # Telegram: t.me/username или telegram.me/username
    tg_pattern <- "(?:t\\.me/|telegram\\.me/)([a-zA-Z0-9_]+)"
    tg_matches <- str_match_all(bio, tg_pattern)[[1]]
    if (nrow(tg_matches) > 0) {
      for (i in 1:nrow(tg_matches)) {
        links <- append(links, list(data.frame(
          source = "bio_telegram",
          url = paste0("https://t.me/", tg_matches[i, 2]),
          type = "telegram",
          username = tg_matches[i, 2],
          stringsAsFactors = FALSE
        )))
      }
    }
    
    # VK: vk.com/id123 или vk.com/username
    vk_pattern <- "vk\\.com/([a-zA-Z0-9_.]+)"
    vk_matches <- str_match_all(bio, vk_pattern)[[1]]
    if (nrow(vk_matches) > 0) {
      for (i in 1:nrow(vk_matches)) {
        links <- append(links, list(data.frame(
          source = "bio_vk",
          url = paste0("https://vk.com/", vk_matches[i, 2]),
          type = "vk",
          username = vk_matches[i, 2],
          stringsAsFactors = FALSE
        )))
      }
    }
    
    # WhatsApp: wa.me/1234567890 или whatsapp.com/channel/...
    wa_pattern <- "(?:wa\\.me/|whatsapp\\.com/)([a-zA-Z0-9_/]+)"
    wa_matches <- str_match_all(bio, wa_pattern)[[1]]
    if (nrow(wa_matches) > 0) {
      for (i in 1:nrow(wa_matches)) {
        links <- append(links, list(data.frame(
          source = "bio_whatsapp",
          url = paste0("https://wa.me/", wa_matches[i, 2]),
          type = "whatsapp",
          username = wa_matches[i, 2],
          stringsAsFactors = FALSE
        )))
      }
    }
    
    # Поиск username в формате @username (но может быть Twitter, Telegram и т.д.)
    at_pattern <- "@([a-zA-Z0-9_]+)"
    at_matches <- str_match_all(bio, at_pattern)[[1]]
    if (nrow(at_matches) > 0) {
      for (i in 1:nrow(at_matches)) {
        # Не можем точно определить тип, помечаем как "mention"
        links <- append(links, list(data.frame(
          source = "bio_mention",
          url = NA_character_,
          type = "mention",
          username = at_matches[i, 2],
          stringsAsFactors = FALSE
        )))
      }
    }
  }
  
  # Объединяем все найденные ссылки
  if (length(links) == 0) {
    message("Не найдено ни одной ссылки на другие профили.")
    return(data.frame())
  }
  
  result <- bind_rows(links) %>%
    distinct()  # убираем дубликаты
  
  return(result)
}

# Вспомогательная функция для классификации ссылки по домену
classify_url <- function(url) {
  if (is.na(url)) return("other")
  domain <- urltools::domain(url)  # требует пакет urltools? можно и без него
  # Но чтобы не добавлять зависимость, реализуем простую классификацию
  if (grepl("twitter\\.com|x\\.com", url)) {
    return("twitter")
  } else if (grepl("t\\.me|telegram\\.me", url)) {
    return("telegram")
  } else if (grepl("vk\\.com", url)) {
    return("vk")
  } else if (grepl("wa\\.me|whatsapp\\.com", url)) {
    return("whatsapp")
  } else if (grepl("linkedin\\.com", url)) {
    return("linkedin")
  } else if (grepl("instagram\\.com", url)) {
    return("instagram")
  } else if (grepl("facebook\\.com", url)) {
    return("facebook")
  } else if (grepl("youtube\\.com|youtu\\.be", url)) {
    return("youtube")
  } else if (grepl("github\\.com", url)) {
    return("github")  # может ссылаться на другой профиль GitHub?
  } else {
    return("other")
  }
}