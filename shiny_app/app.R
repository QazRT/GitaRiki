renviron_candidates <- c(
  file.path(getwd(), ".Renviron"),
  file.path(dirname(normalizePath(getwd(), winslash = "/", mustWork = FALSE)), ".Renviron")
)
for (renviron_path in renviron_candidates[file.exists(renviron_candidates)]) {
  readRenviron(renviron_path)
}

local_lib_candidates <- c(
  file.path(getwd(), ".Rlibs"),
  file.path(dirname(normalizePath(getwd(), winslash = "/", mustWork = FALSE)), ".Rlibs")
)
local_libs <- local_lib_candidates[dir.exists(local_lib_candidates)]
if (length(local_libs) > 0L) {
  .libPaths(unique(c(normalizePath(local_libs, winslash = "/", mustWork = FALSE), .libPaths())))
}

library(shiny)

source_candidates <- c(
  file.path(getwd(), "account_storage.R"),
  file.path(getwd(), "shiny_app", "account_storage.R")
)
existing_source_paths <- source_candidates[file.exists(source_candidates)]
if (length(existing_source_paths) == 0L) {
  stop(
    "Не найден account_storage.R. Ожидался файл account_storage.R в текущей директории или в shiny_app/.",
    call. = FALSE
  )
}
source_path <- existing_source_paths[[1]]
source(source_path, encoding = "UTF-8")

set_protocol_candidates <- c(
  file.path(getwd(), "set_protocol.R"),
  file.path(getwd(), "shiny_app", "set_protocol.R")
)
existing_set_protocol_paths <- set_protocol_candidates[file.exists(set_protocol_candidates)]
if (length(existing_set_protocol_paths) == 0L) {
  stop(
    "Не найден set_protocol.R. Ожидался файл set_protocol.R в текущей директории или в shiny_app/.",
    call. = FALSE
  )
}
set_protocol_path <- existing_set_protocol_paths[[1]]
source(set_protocol_path, encoding = "UTF-8")

app_dir <- dirname(normalizePath(set_protocol_path, winslash = "/", mustWork = FALSE))
project_root <- find_githound_project_root()
analysis_resource_roots <- list(
  analysis_output = normalizePath(file.path(project_root, "R", "analysis_output"), winslash = "/", mustWork = FALSE),
  analysis_output_app = normalizePath(file.path(app_dir, "analysis_output"), winslash = "/", mustWork = FALSE)
)
for (resource_name in names(analysis_resource_roots)) {
  dir.create(analysis_resource_roots[[resource_name]], recursive = TRUE, showWarnings = FALSE)
  addResourcePath(resource_name, analysis_resource_roots[[resource_name]])
}

load_clickhouse_connector()
host <- githound_env("CLICKHOUSE_HOST", "CH_HOST")
port <- as.integer(githound_env("CLICKHOUSE_PORT", "CH_PORT"))
dbname <- githound_env("CLICKHOUSE_DB", "CH_DB")
user <- githound_env("CLICKHOUSE_USER", "CH_USER")
password <- githound_env("CLICKHOUSE_PASSWORD", "CH_PASSWORD")
conn <- connect_clickhouse(
  host = host,
  port = port,
  dbname = dbname,
  user = user,
  password = password
)

avatar_options <- data.frame(
  id = c(
    "egypt_1", "egypt_2", "egypt_3", "egypt_4",
    "greece_1", "greece_2", "greece_3", "greece_4",
    "norse_1", "norse_2", "norse_3", "norse_4"
  ),
  group = rep(c("egypt", "greece", "norse"), each = 4),
  title = c(
    "Фараон", "Страж Анубиса", "Жрица солнца", "Оракул пустыни",
    "Стратегиня", "Солнечный поэт", "Вестник", "Лавровый воин",
    "Один", "Провидица", "Валькирия", "Маг рун"
  ),
  description = c(
    "Золото, власть и спокойная уверенность.",
    "Тёмный защитник с мотивами Анубиса.",
    "Солнечный образ с сияющей пустынной энергией.",
    "Бирюза, песок и мистический взгляд оракула.",
    "Олимпийская тактика, шлем и ясный разум.",
    "Свет Аполлона, лавр и мягкое золото.",
    "Быстрый вестник с крылатым шлемом.",
    "Мраморный воин с лавровым знаком победы.",
    "Северный мудрец с мотивами воронов.",
    "Золотая провидица холодных земель.",
    "Воительница в серебре и ледяном свете.",
    "Рунный маг с северным сиянием."
  ),
  stringsAsFactors = FALSE
)

avatar_css <- function(id) {
  row <- avatar_options[avatar_options$id == id, , drop = FALSE]
  if (nrow(row) == 0L) {
    row <- avatar_options[1, , drop = FALSE]
  }

  index <- as.integer(sub(".*_", "", row$id[[1]]))
  positions <- c("0% 0%", "100% 0%", "0% 100%", "100% 100%")

  paste0(
    "background-image: url('avatars-", row$group[[1]], ".png');",
    "background-position: ", positions[[index]], ";"
  )
}

avatar_choice <- function(id, title, description) {
  tags$span(
    class = "avatar-card",
    tags$span(class = "avatar-thumb", style = avatar_css(id)),
    tags$span(
      class = "avatar-meta",
      tags$span(class = "avatar-name", title),
      tags$span(class = "avatar-desc", description)
    )
  )
}

brand_block <- function() {
  tagList(
    div(
      class = "logo-wrap",
      img(src = "githound-hell-logo.png", class = "logo hell-logo", alt = "GitHound logo"),
      img(src = "githound-heaven-logo.png", class = "logo heaven-logo", alt = "GitHound logo"),
      img(src = "githound-norse-logo.png", class = "logo norse-logo", alt = "GitHound Norse logo"),
      img(src = "githound-norse-heaven-logo.png", class = "logo norse-heaven-logo", alt = "GitHound Norse heaven logo")
    ),
    h1(class = "project-title", "GitHound")
  )
}

mini_thoth_tip <- function(text, norse_text = NULL) {
  norse_text <- norse_text %||% text
  div(
    class = "mini-thoth",
    tags$button(
      type = "button",
      class = "mini-thoth-close",
      onclick = "$(this).closest('.mini-thoth').remove();",
      `aria-label` = "Закрыть совет",
      "x"
    ),
    img(src = "mini-thoth.svg", class = "mini-thoth-avatar mini-thoth-avatar-egypt", alt = "Мини-Тот"),
    img(src = "mini-raven.png", class = "mini-thoth-avatar mini-thoth-avatar-norse", alt = "Мини-ворон"),
    div(
      class = "mini-thoth-scroll",
      div(class = "mini-thoth-title mini-thoth-title-egypt", "Совет мини-Тота"),
      div(class = "mini-thoth-title mini-thoth-title-norse", "Совет мини-ворона"),
      div(class = "mini-thoth-text mini-thoth-text-egypt", text),
      div(class = "mini-thoth-text mini-thoth-text-norse", norse_text)
    )
  )
}

landing_screen <- function() {
  div(
    class = "home",
    brand_block(),
    div(
      class = "form menu-form",
      actionButton("register", "Зарегистрироваться", class = "menu-button primary-button"),
      actionButton("login", "Вход", class = "menu-button secondary-button")
    )
  )
}

registration_screen <- function() {
  div(
    class = "home page-wide",
    brand_block(),
    div(
      class = "form",
      h2(class = "section-title", "Регистрация"),
      textInput("register_email", "Почта", placeholder = "you@example.com"),
      passwordInput("register_password", "Пароль", placeholder = "10+ символов: Aa, цифра и спецсимвол"),
      textInput("register_nickname", "Ник", placeholder = "Ваш ник в GitHound"),
      actionButton("submit_registration", "Создать аккаунт", class = "run-button"),
      actionButton("github_login", "Войти через GitHub", class = "menu-button secondary-button github-login-button"),
      actionButton("back_to_landing", "Назад", class = "menu-button secondary-button")
    )
  )
}

login_screen <- function() {
  github_icon <- HTML(
    "<svg viewBox='0 0 16 16' aria-hidden='true' class='social-login-icon'><path fill='currentColor' d='M8 0C3.58 0 0 3.58 0 8c0 3.54 2.29 6.53 5.47 7.59.4.07.55-.17.55-.38 0-.19-.01-.82-.01-1.49-2.01.37-2.53-.49-2.69-.94-.09-.23-.48-.94-.82-1.13-.28-.15-.68-.52-.01-.53.63-.01 1.08.58 1.23.82.72 1.21 1.87.87 2.33.66.07-.52.28-.87.51-1.07-1.78-.2-3.64-.89-3.64-3.95 0-.87.31-1.59.82-2.15-.08-.2-.36-1.02.08-2.12 0 0 .67-.21 2.2.82A7.65 7.65 0 0 1 8 3.86c.68 0 1.36.09 2 .27 1.53-1.04 2.2-.82 2.2-.82.44 1.1.16 1.92.08 2.12.51.56.82 1.27.82 2.15 0 3.07-1.87 3.75-3.65 3.95.29.25.54.73.54 1.48 0 1.07-.01 1.93-.01 2.2 0 .21.15.46.55.38A8.01 8.01 0 0 0 16 8c0-4.42-3.58-8-8-8Z'/></svg>"
  )

  div(
    class = "home page-wide",
    brand_block(),
    div(
      class = "form login-form",
      h2(class = "section-title", "Вход"),
      div(
        class = "social-login-block",
        div(class = "social-login-title", "Войти через"),
        div(
          class = "social-login-row",
          actionButton(
            "github_login",
            tagList(github_icon, span(class = "social-login-text", "GitHub")),
            class = "social-login-button github-login-button",
            title = "Войти через GitHub"
          )
        )
      ),
      div(class = "login-divider", span("или")),
      textInput("login_email", "Почта", placeholder = "you@example.com"),
      passwordInput("login_password", "Пароль", placeholder = "Введите пароль"),
      checkboxInput("remember_me", "Запомнить меня", value = FALSE),
      actionButton("submit_login", "Войти", class = "run-button"),
      actionButton("back_to_landing_login", "Назад", class = "menu-button secondary-button")
    ),
    tags$script(HTML("
      setTimeout(fillRememberedLogin, 0);
      setTimeout(fillRememberedLogin, 150);
      setTimeout(fillRememberedLogin, 500);
    "))
  )
}

analysis_screen <- function() {
  div(
    class = "home",
    brand_block(),
    div(
      class = "form",
      textInput("analysis_user", "Пользователь для анализа", placeholder = "Введите GitHub username"),
      actionButton("run_analysis", "Запуск", class = "run-button")
    ),
    mini_thoth_tip("Логин цели — это имя профиля GitHub в адресе страницы. Например, в github.com/octocat нужно ввести octocat.")
  )
}

protocol_screen <- function(target) {
  div(
    class = "home protocol-page",
    brand_block(),
    div(
      class = "form protocol-form",
      h2(class = "section-title", paste("Цель:", target)),
      div(
        class = "protocol-grid",
        div(
          class = "protocol-card",
          img(src = "protocol-isis.png", class = "protocol-image protocol-egypt-image protocol-hell-image", alt = "Исида"),
          img(src = "protocol-isis-heaven.png", class = "protocol-image protocol-egypt-image protocol-heaven-image", alt = "Исида"),
          img(src = "protocol-odin.png", class = "protocol-image protocol-norse-image protocol-norse-hell-image", alt = "Один"),
          img(src = "protocol-odin-heaven.png", class = "protocol-image protocol-norse-image protocol-norse-heaven-image", alt = "Один"),
          actionButton(
            "run_isis",
            tagList(
              span(class = "protocol-label-egypt", "Запуск Исиды"),
              span(class = "protocol-label-norse", "Запуск ОДИНА")
            ),
            class = "run-button"
          )
        ),
        div(
          class = "protocol-card",
          img(src = "protocol-set.png", class = "protocol-image protocol-egypt-image protocol-hell-image", alt = "Сет"),
          img(src = "protocol-set-heaven.png", class = "protocol-image protocol-egypt-image protocol-heaven-image", alt = "Сет"),
          img(src = "protocol-tyr.png", class = "protocol-image protocol-norse-image protocol-norse-hell-image", alt = "Тюр"),
          img(src = "protocol-tyr-heaven.png", class = "protocol-image protocol-norse-image protocol-norse-heaven-image", alt = "Тюр"),
          actionButton(
            "run_set",
            tagList(
              span(class = "protocol-label-egypt", "Запуск протокола Сет"),
              span(class = "protocol-label-norse", "Запуск протокола ТЮР")
            ),
            class = "run-button"
          )
        ),
        div(
          class = "protocol-card",
          img(src = "protocol-maat.png", class = "protocol-image protocol-egypt-image protocol-hell-image", alt = "Маат"),
          img(src = "protocol-maat-heaven.png", class = "protocol-image protocol-egypt-image protocol-heaven-image", alt = "Маат"),
          img(src = "protocol-thor.png", class = "protocol-image protocol-norse-image protocol-norse-hell-image", alt = "Тор"),
          img(src = "protocol-thor-heaven.png", class = "protocol-image protocol-norse-image protocol-norse-heaven-image", alt = "Тор"),
          actionButton(
            "run_quality",
            tagList(
              span(class = "protocol-label-egypt", "Запуск протокола Маат"),
              span(class = "protocol-label-norse", "Запуск протокола Тор")
            ),
            class = "run-button"
          )
        )
      ),
      actionButton("go_analysis", "Вернуться на главный экран", class = "menu-button secondary-button")
    ),
    mini_thoth_tip(
      "ИСИДА — Интеллектуальная Система Интерпретации и Детального Анализа. Сет — сухой протокол анализа без использования ИИ.",
      "ОДИН — Обобщающая Диагностическая Интерпретационная Нейросистема. ТЮР — Точный Юнит Разбора: сухой протокол анализа без использования ИИ."
    )
  )
}

account_screen <- function(nickname = "Профиль", selected_avatar = "egypt_1", github_token = "") {
  div(
    class = "home account-page",
    div(
      class = "form account-form",
      div(
        class = "account-portrait",
        uiOutput("profile_photo"),
        h2(class = "section-title", nickname),
        p(class = "account-copy", "Личный аккаунт GitHound"),
        uiOutput("mythology_style_picker"),
        actionButton("toggle_avatars", "Выбрать стандартное фото", class = "menu-button secondary-button")
      ),
      passwordInput("account_token", "GitHub токен", value = github_token, placeholder = "Введите токен для анализа"),
      uiOutput("account_token_status"),
      uiOutput("account_avatar_picker"),
      actionButton("save_account", "Сохранить профиль", class = "run-button"),
      actionButton("go_analysis", "Перейти к анализу", class = "menu-button secondary-button")
    ),
    mini_thoth_tip("Токен можно создать в GitHub: Settings → Developer settings → Personal access tokens. Для анализа вставьте его сюда.")
  )
}

set_loading_screen <- function(target, protocol_type = "set") {
  protocol_label <- archive_protocol_label(protocol_type)
  div(
    class = "home set-loading-page",
    brand_block(),
    div(
      class = "form set-loading-form",
      h2(class = "section-title", paste("Протокол", protocol_label, ":", target)),
      uiOutput("set_progress_ui"),
      actionButton("go_analysis", "Вернуться на главный экран", class = "menu-button secondary-button")
    )
  )
}

set_column_labels <- c(
  profile = "Профиль",
  login = "Логин",
  name = "Имя",
  company = "Компания",
  location = "Локация",
  repository = "Репозиторий",
  repo = "Репозиторий",
  repo_name = "Репозиторий",
  repository_name = "Репозиторий",
  owner = "Владелец",
  ownership_type = "Тип репозитория",
  language = "Язык",
  url = "Ссылка",
  source = "Источник",
  domain = "Домен",
  status = "Статус",
  message = "Комментарий",
  metric = "Показатель",
  value = "Значение",
  total_commits = "Всего коммитов",
  commits = "Коммиты",
  total_repositories = "Репозитории",
  repositories = "Репозитории",
  public_repos = "Публичные репозитории",
  active_days = "Активных дней",
  first_commit = "Первый коммит",
  last_commit = "Последний коммит",
  avg_commits_per_repo = "Среднее коммитов на репозиторий",
  avg_commits_per_active_day = "Среднее коммитов в активный день",
  median_commits_per_day = "Медиана коммитов в день",
  total_additions = "Добавлено строк",
  total_deletions = "Удалено строк",
  total_changes = "Всего изменений",
  avg_changes_per_commit = "Среднее изменений на коммит",
  commit_share = "Доля коммитов",
  activity_type = "Тип активности",
  activity_value = "Значение активности",
  bucket_time = "Период",
  fetched_at = "Дата сбора",
  created_at = "Создано",
  updated_at = "Обновлено",
  account_created_at = "Аккаунт создан",
  followers = "Подписчики",
  following = "Подписки",
  stars = "Звезды",
  forks = "Форки",
  total_stars = "Всего звезд",
  total_forks = "Всего форков",
  total_repo_stars = "Звезды",
  total_repo_forks = "Форки",
  total_repo_size_kb = "Размер, КБ",
  popularity_index = "Индекс популярности",
  vulnerability_check = "Проверка уязвимостей",
  package = "Пакет",
  package_name = "Пакет",
  ecosystem = "Экосистема",
  severity = "Критичность",
  summary = "Описание",
  details = "Подробности",
  aliases = "Алиасы",
  references = "Ссылки",
  osv_id = "OSV ID",
  vuln_id = "ID уязвимости",
  introduced = "Появилась",
  fixed = "Исправлена",
  introduced_at = "Дата появления",
  fixed_at = "Дата исправления",
  affected_range = "Затронутый диапазон"
)

set_pretty_column_name <- function(name) {
  original <- as.character(name)
  key <- tolower(gsub("[^[:alnum:]_]+", "_", original))
  key <- gsub("^_+|_+$", "", key)
  label <- unname(set_column_labels[[key]])
  if (!is.null(label) && length(label) == 1L && !is.na(label) && nzchar(label)) {
    return(label)
  }
  if (grepl("[_]", original)) {
    return(gsub("\\s+", " ", trimws(gsub("_+", " ", original))))
  }
  original
}

set_apply_column_labels <- function(df) {
  if (is.data.frame(df) && ncol(df) > 0L) {
    names(df) <- vapply(names(df), set_pretty_column_name, character(1), USE.NAMES = FALSE)
  }
  df
}

set_table_ui <- function(df) {
  if (!is.data.frame(df) || nrow(df) == 0L) {
    return(div(class = "set-empty", "Нет данных для этого раздела."))
  }

  copy_commit_at_date <- attr(df, "copy_commit_at_date", exact = TRUE)
  if (is.null(copy_commit_at_date) &&
      ncol(df) >= 10L &&
      "OSV ID" %in% names(df) &&
      any(grepl("[[:xdigit:]]{7,}", as.character(df[[6L]])), na.rm = TRUE)) {
    introduced_commits <- df[[6L]]
    fixed_commits <- df[[9L]]
    df <- df[, -c(6L, 9L), drop = FALSE]
    copy_commit_at_date <- list(
      list(date_col = 6L, commits = introduced_commits),
      list(date_col = 8L, commits = fixed_commits)
    )
  }
  if (is.null(copy_commit_at_date) &&
      ncol(df) >= 9L &&
      "OSV ID" %in% names(df) &&
      any(grepl("[[:xdigit:]]{7,}", as.character(df[[5L]])), na.rm = TRUE)) {
    copy_commit_at_date <- list(date_col = 6L, commit_col = 5L)
  }
  if (is.list(copy_commit_at_date) &&
      !is.null(copy_commit_at_date$commit_col) &&
      identical(copy_commit_at_date$date_col, 6L) &&
      identical(copy_commit_at_date$commit_col, 5L) &&
      ncol(df) >= 9L) {
    introduced_commits <- df[[5L]]
    df <- df[, -c(5L, 8L), drop = FALSE]
    copy_commit_at_date <- list(date_col = 5L, commits = introduced_commits)
  }
  copy_commit_in_repository <- NULL
  repo_col <- which(names(df) %in% c("Репозиторий", "Repository", "Р РµРїРѕР·РёС‚РѕСЂРёР№"))
  vuln_commit_col <- which(names(df) %in% c("Коммит", "Commit", "РљРѕРјРјРёС‚"))
  is_found_vulnerability_table <- length(repo_col) > 0L &&
    length(vuln_commit_col) > 0L &&
    "Vuln ID" %in% names(df) &&
    "EPSS" %in% names(df)
  if (isTRUE(is_found_vulnerability_table)) {
    commit_col <- vuln_commit_col[[1L]]
    repo_col_i <- repo_col[[1L]]
    vulnerability_commits <- as.character(df[[commit_col]])
    df <- df[, -commit_col, drop = FALSE]
    if (commit_col < repo_col_i) {
      repo_col_i <- repo_col_i - 1L
    }
    copy_commit_in_repository <- list(repo_col = repo_col_i, commits = vulnerability_commits)
  }
  copy_commit_mappings <- if (is.list(copy_commit_at_date) && !is.null(copy_commit_at_date$mappings)) {
    copy_commit_at_date$mappings
  } else if (is.list(copy_commit_at_date) && !is.null(copy_commit_at_date$date_col)) {
    list(copy_commit_at_date)
  } else if (is.list(copy_commit_at_date)) {
    copy_commit_at_date
  } else {
    list()
  }
  copy_commit_mappings <- Filter(function(mapping) {
    is.list(mapping) && !is.null(mapping[["date_col"]])
  }, copy_commit_mappings)
  commit_mapping_for_col <- function(col_index) {
    matches <- Filter(function(mapping) {
      identical(as.integer(col_index), as.integer(mapping[["date_col"]]))
    }, copy_commit_mappings)
    if (length(matches) > 0L) matches[[1L]] else NULL
  }

  full_text_mappings <- list()
  full_text_col <- function(candidates) {
    found <- which(names(df) %in% candidates)
    if (length(found) > 0L) found[[1L]] else NA_integer_
  }
  full_summary_col <- full_text_col(c("Описание полностью", "Full description", "Full summary"))
  full_details_col <- full_text_col(c("Full details", "Details"))
  full_aliases_col <- full_text_col(c("Full aliases", "Aliases"))
  full_references_col <- full_text_col(c("Full references", "References"))
  description_col <- which(names(df) %in% c("Описание", "Description"))
  hidden_full_cols <- c(full_summary_col, full_details_col, full_aliases_col, full_references_col)
  hidden_full_cols <- unique(hidden_full_cols[!is.na(hidden_full_cols)])
  if (length(hidden_full_cols) > 0L && length(description_col) > 0L) {
    full_values <- list(
      summary = if (!is.na(full_summary_col)) as.character(df[[full_summary_col]]) else rep("", nrow(df)),
      details = if (!is.na(full_details_col)) as.character(df[[full_details_col]]) else rep("", nrow(df)),
      aliases = if (!is.na(full_aliases_col)) as.character(df[[full_aliases_col]]) else rep("", nrow(df)),
      references = if (!is.na(full_references_col)) as.character(df[[full_references_col]]) else rep("", nrow(df))
    )
    df <- df[, -hidden_full_cols, drop = FALSE]
    description_col <- which(names(df) %in% c("Описание", "Description"))
    if (length(description_col) > 0L) {
      title_col <- which(names(df) %in% c("Vuln ID", "OSV ID"))
      full_text_mappings <- list(list(
        text_col = description_col[[1L]],
        values = full_values$summary,
        details = full_values$details,
        aliases = full_values$aliases,
        references = full_values$references,
        title_col = if (length(title_col) > 0L) title_col[[1L]] else NA_integer_
      ))
    }
  }
  if (length(full_text_mappings) == 0L) {
    fallback_description_col <- which(names(df) %in% c("\u041e\u043f\u0438\u0441\u0430\u043d\u0438\u0435", "Description"))
    if (length(fallback_description_col) == 0L) {
      fallback_description_col <- grep("descr|\\u041e\\u043f\\u0438\\u0441", names(df), ignore.case = TRUE)
    }
    if (length(fallback_description_col) > 0L && "Vuln ID" %in% names(df)) {
      fallback_description_col <- fallback_description_col[[1L]]
      full_values <- list(
        summary = if (!is.na(full_summary_col)) as.character(df[[full_summary_col]]) else as.character(df[[fallback_description_col]]),
        details = if (!is.na(full_details_col)) as.character(df[[full_details_col]]) else rep("", nrow(df)),
        aliases = if (!is.na(full_aliases_col)) as.character(df[[full_aliases_col]]) else rep("", nrow(df)),
        references = if (!is.na(full_references_col)) as.character(df[[full_references_col]]) else rep("", nrow(df))
      )
      if (length(hidden_full_cols) > 0L) {
        df <- df[, -hidden_full_cols, drop = FALSE]
        fallback_description_col <- which(names(df) %in% c("\u041e\u043f\u0438\u0441\u0430\u043d\u0438\u0435", "Description"))
        if (length(fallback_description_col) == 0L) {
          fallback_description_col <- grep("descr|\\u041e\\u043f\\u0438\\u0441", names(df), ignore.case = TRUE)
        }
      }
      if (length(fallback_description_col) > 0L) {
        title_col <- which(names(df) %in% c("Vuln ID", "OSV ID"))
        full_text_mappings <- list(list(
          text_col = fallback_description_col[[1L]],
          values = full_values$summary,
          details = full_values$details,
          aliases = full_values$aliases,
          references = full_values$references,
          title_col = if (length(title_col) > 0L) title_col[[1L]] else NA_integer_
        ))
      }
    }
  }
  full_text_mapping_for_col <- function(col_index) {
    matches <- Filter(function(mapping) {
      identical(as.integer(col_index), as.integer(mapping[["text_col"]]))
    }, full_text_mappings)
    if (length(matches) > 0L) matches[[1L]] else NULL
  }
  df <- set_apply_column_labels(df)
  epss_cve_col <- which(names(df) %in% c("EPSS CVE", "epss_cve"))
  epss_cves <- if (length(epss_cve_col) > 0L) {
    as.character(df[[epss_cve_col[[1L]]]])
  } else {
    rep("", nrow(df))
  }
  if (length(epss_cve_col) > 0L) {
    removed_col <- epss_cve_col[[1L]]
    df <- df[, -removed_col, drop = FALSE]
    full_text_mappings <- lapply(full_text_mappings, function(mapping) {
      if (is.list(mapping) && !is.null(mapping[["text_col"]]) && mapping[["text_col"]] > removed_col) {
        mapping[["text_col"]] <- mapping[["text_col"]] - 1L
      }
      if (is.list(mapping) && !is.null(mapping[["title_col"]]) && !is.na(mapping[["title_col"]]) && mapping[["title_col"]] > removed_col) {
        mapping[["title_col"]] <- mapping[["title_col"]] - 1L
      }
      mapping
    })
  }
  df <- utils::head(df, 12L)
  epss_cves <- utils::head(epss_cves, nrow(df))
  if (is.list(copy_commit_in_repository) && !is.null(copy_commit_in_repository$commits)) {
    copy_commit_in_repository$commits <- utils::head(copy_commit_in_repository$commits, nrow(df))
  }
  return(div(
    class = "set-report-table-wrap",
    tags$table(
      class = "set-report-table set-sortable-table",
      tags$thead(
        tags$tr(lapply(names(df), function(name) {
          if (identical(name, "EPSS")) {
            tags$th(
              div(
                class = "epss-header-cell",
                span(name),
                tags$button(
                  type = "button",
                  class = "epss-fetch-all-button",
                  title = "Запросить EPSS для всех строк",
                  HTML("&#128065;")
                )
              )
            )
          } else {
            tags$th(name)
          }
        }))
      ),
      tags$tbody(
        lapply(seq_len(nrow(df)), function(i) {
          tags$tr(lapply(seq_along(df), function(j) {
            value <- as.character(df[[j]][[i]])
            display_value <- value
            if (is.na(display_value) || !nzchar(display_value)) {
              display_value <- "\u2014"
            }
            if (is.list(copy_commit_in_repository) &&
                identical(j, copy_commit_in_repository$repo_col) &&
                !is.null(copy_commit_in_repository$commits) &&
                length(copy_commit_in_repository$commits) >= i) {
              commit <- as.character(copy_commit_in_repository$commits[[i]])
              can_copy <- !is.na(commit) && nzchar(commit) && grepl("[[:xdigit:]]{7,}", commit)
              if (isTRUE(can_copy)) {
                return(tags$td(
                  div(
                    class = "commit-date-cell",
                    span(display_value),
                    tags$button(
                      type = "button",
                      class = "copy-commit-button",
                      `data-commit` = commit,
                      title = commit,
                      "SHA"
                    )
                  )
                ))
              }
            }
            if (identical(names(df)[[j]], "EPSS")) {
              cve <- if (length(epss_cves) >= i) as.character(epss_cves[[i]]) else ""
              if (!is.na(cve) && nzchar(cve)) {
                return(tags$td(
                  div(
                    class = "epss-cell",
                    span(class = "epss-value", "\u2014"),
                    tags$button(
                      type = "button",
                      class = "epss-fetch-button",
                      `data-cve` = cve,
                      title = paste("Fetch EPSS for", cve),
                      "EPSS"
                    )
                  )
                ))
              }
            }
            commit_mapping <- commit_mapping_for_col(j)
            if (is.list(commit_mapping)) {
              commit <- if (!is.null(commit_mapping[["commits"]]) &&
                            length(commit_mapping[["commits"]]) >= i) {
                as.character(commit_mapping[["commits"]][[i]])
              } else if (!is.null(commit_mapping[["commit_col"]]) &&
                         commit_mapping[["commit_col"]] %in% seq_along(df)) {
                as.character(df[[commit_mapping[["commit_col"]]]][[i]])
              } else {
                ""
              }
              can_copy <- !is.na(commit) && nzchar(commit) && grepl("[[:xdigit:]]{7,}", commit)
              if (isTRUE(can_copy)) {
                return(tags$td(
                  `data-sort` = value,
                  div(
                    class = "commit-date-cell",
                    span(display_value),
                    tags$button(
                      type = "button",
                      class = "copy-commit-button",
                      `data-commit` = commit,
                      title = commit,
                      "SHA"
                    )
                  )
                ))
              }
            }
            full_text_mapping <- full_text_mapping_for_col(j)
            if (is.list(full_text_mapping) &&
                !is.null(full_text_mapping[["values"]]) &&
                length(full_text_mapping[["values"]]) >= i) {
              full_summary <- as.character(full_text_mapping[["values"]][[i]])
              full_details <- if (!is.null(full_text_mapping[["details"]]) &&
                                  length(full_text_mapping[["details"]]) >= i) {
                as.character(full_text_mapping[["details"]][[i]])
              } else {
                ""
              }
              full_aliases <- if (!is.null(full_text_mapping[["aliases"]]) &&
                                  length(full_text_mapping[["aliases"]]) >= i) {
                as.character(full_text_mapping[["aliases"]][[i]])
              } else {
                ""
              }
              full_references <- if (!is.null(full_text_mapping[["references"]]) &&
                                     length(full_text_mapping[["references"]]) >= i) {
                as.character(full_text_mapping[["references"]][[i]])
              } else {
                ""
              }
              full_title <- ""
              title_col <- suppressWarnings(as.integer(full_text_mapping[["title_col"]]))
              if (!is.na(title_col) && title_col %in% seq_along(df)) {
                full_title <- as.character(df[[title_col]][[i]])
              }
              has_full_text <- any(nzchar(trimws(c(full_summary, full_details, full_aliases, full_references))), na.rm = TRUE)
              if (isTRUE(has_full_text)) {
                return(tags$td(
                  `data-sort` = value,
                  div(
                    class = "description-cell",
                    span(display_value),
                    tags$button(
                      type = "button",
                      class = "full-text-button",
                      `data-title` = full_title,
                      `data-full-summary` = full_summary,
                      `data-full-details` = full_details,
                      `data-full-aliases` = full_aliases,
                      `data-full-references` = full_references,
                      title = "Показать полное описание",
                      "Подробно"
                    )
                  )
                ))
              }
            }
            tags$td(`data-sort` = value, display_value)
          }))
        })
      )
    )
  ))
  div(
    class = "set-report-table-wrap",
    tags$table(
      class = "set-report-table",
      tags$thead(
        tags$tr(lapply(names(df), function(name) tags$th(name)))
      ),
      tags$tbody(
        lapply(seq_len(nrow(df)), function(i) {
          tags$tr(lapply(seq_along(df), function(j) {
            value <- as.character(df[[j]][[i]])
            display_value <- if (is.na(value) || !nzchar(value)) "вЂ”" else value
            if (is.list(copy_commit_at_date) &&
                identical(j, copy_commit_at_date$date_col) &&
                copy_commit_at_date$commit_col %in% seq_along(df)) {
              commit <- as.character(df[[copy_commit_at_date$commit_col]][[i]])
              can_copy <- !is.na(commit) && nzchar(commit) && grepl("[[:xdigit:]]{7,}", commit)
              if (isTRUE(can_copy)) {
                return(tags$td(
                  div(
                    class = "commit-date-cell",
                    span(display_value),
                    tags$button(
                      type = "button",
                      class = "copy-commit-button",
                      `data-commit` = commit,
                      title = "Copy commit",
                      "Copy"
                    )
                  )
                ))
              }
            }
            tags$td(if (is.na(value) || !nzchar(value)) "—" else value)
          }))
        })
      )
    )
  )
}

set_markdown_ui <- function(text) {
  text <- paste(as.character(text %||% ""), collapse = "\n")
  text <- trimws(text)
  if (!nzchar(text)) {
    return(div(class = "set-empty", "Нет данных для этого раздела."))
  }

  rendered <- NULL
  if (requireNamespace("commonmark", quietly = TRUE)) {
    rendered <- commonmark::markdown_html(
      text,
      hardbreaks = TRUE,
      extensions = c("table", "strikethrough", "autolink")
    )
  } else if (requireNamespace("markdown", quietly = TRUE)) {
    rendered <- markdown::markdownToHTML(
      text = text,
      fragment.only = TRUE,
      options = c("tables", "fenced_code_blocks", "autolink")
    )
  }

  if (is.null(rendered) || !nzchar(rendered)) {
    return(tags$pre(text))
  }

  div(class = "set-markdown", HTML(rendered))
}

set_plot_src <- function(path) {
  if (is.null(path) || !nzchar(path) || !file.exists(path)) {
    return(NULL)
  }

  normalized <- normalizePath(path, winslash = "/", mustWork = FALSE)
  for (resource_name in names(analysis_resource_roots)) {
    root <- analysis_resource_roots[[resource_name]]
    if (startsWith(normalized, paste0(root, "/"))) {
      relative <- substring(normalized, nchar(root) + 2L)
      relative <- utils::URLencode(relative, reserved = TRUE)
      return(paste0(resource_name, "/", relative))
    }
  }

  NULL
}

set_plots_ui <- function(report) {
  plots <- report$plots %||% character()
  plots <- plots[file.exists(plots)]
  if (length(plots) == 0L) {
    return(NULL)
  }

  div(
    class = "set-report-section set-plot-section",
    h3(class = "set-report-title", "Визуальные графики"),
    p(class = "set-report-text", "Графики, которые были построены штатными функциями анализа."),
    div(
      class = "set-plot-grid",
      lapply(plots, function(path) {
        src <- set_plot_src(path)
        if (is.null(src)) {
          return(NULL)
        }
        label <- set_plot_label(path)
        div(
          class = "set-plot-card",
          tags$button(
            type = "button",
            class = "set-plot-button",
            `data-src` = src,
            `data-label` = label,
            img(src = src, class = "set-plot-image", alt = label)
          ),
          div(class = "set-plot-caption", label)
        )
      })
    )
  )
}

report_theme_value <- function(report, fallback = "hell") {
  theme <- report$theme %||% report$launch_theme %||% fallback
  if (!theme %in% c("hell", "heaven")) {
    theme <- fallback
  }
  theme
}

report_mythology_value <- function(report, fallback = "egypt") {
  mythology <- report$mythology_style %||% report$launch_mythology %||% fallback
  mythology <- tolower(trimws(as.character(mythology)))
  if (!mythology %in% c("egypt", "norse", "greece")) {
    mythology <- fallback
  }
  mythology
}

set_title_page <- function(report) {
  theme <- report$view_theme %||% report_theme_value(report, fallback = "hell")
  if (!theme %in% c("hell", "heaven")) {
    theme <- "hell"
  }
  is_heaven <- identical(theme, "heaven")
  mythology <- report_mythology_value(report, fallback = "egypt")
  is_norse <- identical(mythology, "norse")
  world <- if (is_heaven) "божественного мира" else "загробного мира"
  oath <- if (is_heaven) {
    "Свидетельство собрано под светом небесного суда: факты отделены от догадок, а следы цели сохранены в порядке."
  } else {
    "Свидетельство собрано у врат нижнего суда: факты отделены от догадок, а следы цели сохранены в порядке."
  }
  logo_src <- if (is_norse) {
    if (is_heaven) "githound-norse-heaven-logo.png" else "githound-norse-logo.png"
  } else {
    if (is_heaven) "githound-heaven-logo.png" else "githound-hell-logo.png"
  }
  seals <- if (is_norse) {
    list(
      list(file = if (is_heaven) "seal-odin-heaven.png" else "seal-odin.png", name = "Один", note = "печать мудрого толкования"),
      list(file = if (is_heaven) "seal-tyr-heaven.png" else "seal-tyr.png", name = "Тюр", note = "печать точного разбора"),
      list(file = if (is_heaven) "seal-heimdall-heaven.png" else "seal-heimdall.png", name = "Хеймдалль", note = "печать зорких следов")
    )
  } else {
    list(
      list(file = "seal-isis.png", name = "Исида", note = "печать ясного толкования"),
      list(file = "seal-set.png", name = "Сет", note = "печать сухого протокола"),
      list(file = "seal-anubis.png", name = "Анубис", note = "печать взвешенных следов")
    )
  }

  div(
    class = "set-title-page",
    div(
      class = "set-title-brand",
      img(
        src = logo_src,
        class = "set-title-logo",
        alt = "GitHound"
      ),
      div(class = "set-title-project", "GitHound")
    ),
    h1(class = "set-title-main", paste("Отчет", world)),
    div(class = "set-title-target", paste("Цель:", report$profile %||% "—")),
    p(class = "set-title-oath", oath),
    div(
      class = "divine-seals",
      lapply(seals, function(seal) {
        div(
          class = "divine-seal",
          img(src = seal$file, class = "divine-seal-image", alt = paste("Печать", seal$name)),
          span(seal$name),
          tags$small(seal$note)
        )
      })
    ),
    div(class = "set-title-date", paste("Собран:", report$generated_at %||% ""))
  )
}

set_report_screen <- function(report) {
  if (is.null(report) || !is.list(report)) {
    return(div(class = "home", div(class = "form", h2(class = "section-title", "Отчет еще не готов"))))
  }

  protocol_type <- tolower(report$protocol_type %||% "set")
  protocol_label <- report$protocol_label %||% if (identical(protocol_type, "isis")) "Исида" else "Сет"
  switch_button <- if (identical(protocol_type, "set")) {
    actionButton("run_isis_after_report", "Запустить Исиду", class = "run-button")
  } else if (identical(protocol_type, "quality")) {
    tagList(
      actionButton("run_set_after_report", "Запустить Сета", class = "run-button"),
      actionButton("run_isis_after_report", "Запустить Исиду", class = "run-button")
    )
  } else {
    actionButton("run_set_after_report", "Запустить Сета", class = "run-button")
  }

  div(
    class = "home set-report-page",
    brand_block(),
    div(
      class = "form set-report-form",
      h2(class = "section-title", paste("Отчет", protocol_label, ":", report$profile %||% "")),
      p(class = "account-copy", paste("Собран:", report$generated_at %||% "")),
      set_title_page(report),
      lapply(report$sections, function(section) {
        div(
          class = "set-report-section",
          h3(class = "set-report-title", section$title %||% "Раздел"),
          p(class = "set-report-text", section$text %||% ""),
          if (isTRUE(section$render_markdown)) set_markdown_ui(section$markdown %||% "") else set_table_ui(section$table)
        )
      }),
      set_plots_ui(report),
      switch_button,
      actionButton("go_analysis", "Вернуться на главный экран", class = "menu-button secondary-button")
    )
  )
}

boot_screen <- function() {
  div(
    class = "home",
    brand_block(),
    div(
      class = "form menu-form",
      h2(class = "section-title", "Пробуждение врат"),
      p(class = "account-copy", "Проверяем сохраненный вход.")
    )
  )
}

archive_status_label <- function(status) {
  switch(status %||% "active", active = "активен", archived = "заархивировано", deleted = "удален", status)
}

github_oauth_redirect_uri <- function(session) {
  protocol <- session$clientData$url_protocol %||% "http:"
  hostname <- session$clientData$url_hostname %||% "127.0.0.1"
  port <- session$clientData$url_port %||% ""
  pathname <- session$clientData$url_pathname %||% "/"
  if (!nzchar(pathname)) pathname <- "/"
  port_part <- if (nzchar(port)) paste0(":", port) else ""
  paste0(protocol, "//", hostname, port_part, pathname)
}

github_oauth_authorize_url <- function(state, redirect_uri) {
  client_id <- githound_env("GITHUB_CLIENT_ID", required = TRUE)
  if (grepl("^your_|^ваш_|placeholder|oauth_client_id", client_id, ignore.case = TRUE)) {
    stop("Заполните GITHUB_CLIENT_ID и GITHUB_CLIENT_SECRET реальными значениями OAuth App из GitHub.", call. = FALSE)
  }
  scope <- githound_env("GITHUB_OAUTH_SCOPE", required = FALSE)
  if (!nzchar(scope)) {
    scope <- "read:user user:email public_repo"
  }
  query <- paste0(
    "client_id=", utils::URLencode(client_id, reserved = TRUE),
    "&redirect_uri=", utils::URLencode(redirect_uri, reserved = TRUE),
    "&scope=", utils::URLencode(scope, reserved = TRUE),
    "&state=", utils::URLencode(state, reserved = TRUE)
  )
  paste0("https://github.com/login/oauth/authorize?", query)
}

github_api_get <- function(url, access_token) {
  response <- httr::GET(
    url,
    httr::add_headers(
      Authorization = paste("Bearer", access_token),
      Accept = "application/vnd.github+json",
      `User-Agent` = "GitHound-Shiny"
    )
  )
  if (httr::http_error(response)) {
    stop("GitHub API вернул ошибку: ", httr::status_code(response), call. = FALSE)
  }
  jsonlite::fromJSON(httr::content(response, as = "text", encoding = "UTF-8"), simplifyVector = FALSE)
}

complete_github_oauth <- function(code, redirect_uri) {
  if (!requireNamespace("httr", quietly = TRUE) || !requireNamespace("jsonlite", quietly = TRUE)) {
    stop("Для входа через GitHub нужны R-пакеты httr и jsonlite.", call. = FALSE)
  }

  client_id <- githound_env("GITHUB_CLIENT_ID", required = TRUE)
  client_secret <- githound_env("GITHUB_CLIENT_SECRET", required = TRUE)
  if (grepl("^your_|^ваш_|placeholder|oauth_client_id", client_id, ignore.case = TRUE) ||
      grepl("^your_|^ваш_|placeholder|oauth_client_secret", client_secret, ignore.case = TRUE)) {
    stop("Заполните GITHUB_CLIENT_ID и GITHUB_CLIENT_SECRET реальными значениями OAuth App из GitHub.", call. = FALSE)
  }

  response <- httr::POST(
    "https://github.com/login/oauth/access_token",
    body = list(
      client_id = client_id,
      client_secret = client_secret,
      code = code,
      redirect_uri = redirect_uri
    ),
    encode = "form",
    httr::accept_json()
  )

  token_payload <- jsonlite::fromJSON(httr::content(response, as = "text", encoding = "UTF-8"), simplifyVector = FALSE)
  access_token <- token_payload$access_token %||% ""
  if (httr::http_error(response) || !nzchar(access_token)) {
    stop(token_payload$error_description %||% "GitHub не выдал OAuth-токен.", call. = FALSE)
  }

  user <- github_api_get("https://api.github.com/user", access_token)
  email <- user$email %||% ""
  if (!nzchar(email)) {
    emails <- tryCatch(github_api_get("https://api.github.com/user/emails", access_token), error = function(e) list())
    primary <- Filter(function(item) isTRUE(item$primary) && isTRUE(item$verified), emails)
    if (length(primary) > 0L) {
      email <- primary[[1]]$email %||% ""
    }
  }

  list(
    id = as.character(user$id %||% ""),
    login = user$login %||% "",
    name = user$name %||% "",
    email = email,
    access_token = access_token,
    token_name = "GitHoundToken",
    token_expires_at = Sys.time() + 30 * 24 * 60 * 60
  )
}

archive_protocol_label <- function(protocol_type) {
  switch(tolower(protocol_type %||% "set"), set = "Сет", isis = "Исида", quality = "Маат", protocol_type %||% "—")
}

pdf_theme <- function(mode = "hell") {
  if (identical(mode, "heaven")) {
    return(list(
      bg = "#eef8ff",
      panel = "#f8fcff",
      line = "#84c9e7",
      accent = "#c78a00",
      gold = "#d4a32c",
      ink = "#183244",
      muted = "#5b7f93"
    ))
  }

  list(
    bg = "#130305",
    panel = "#22070a",
    line = "#9b121d",
    accent = "#e01824",
    gold = "#d8a927",
    ink = "#fff5f5",
    muted = "#d6a0a0"
  )
}

pdf_asset_path <- function(filename) {
  file.path(app_dir, "www", filename)
}

pdf_read_raster <- local({
  cache <- new.env(parent = emptyenv())
  function(path) {
    normalized <- normalizePath(path, winslash = "/", mustWork = FALSE)
    if (exists(normalized, envir = cache, inherits = FALSE)) {
      return(get(normalized, envir = cache, inherits = FALSE))
    }
    if (!file.exists(normalized) || !requireNamespace("png", quietly = TRUE)) {
      return(NULL)
    }
    img <- tryCatch(png::readPNG(normalized), error = function(e) NULL)
    if (is.null(img)) {
      return(NULL)
    }
    raster <- grDevices::as.raster(img)
    assign(normalized, raster, envir = cache)
    raster
  }
})

pdf_draw_raster <- function(path, xleft, ybottom, xright, ytop) {
  raster <- pdf_read_raster(path)
  if (is.null(raster)) {
    return(invisible(FALSE))
  }
  graphics::rasterImage(raster, xleft, ybottom, xright, ytop, interpolate = TRUE)
  invisible(TRUE)
}

pdf_new_page <- function(theme = pdf_theme()) {
  graphics::par(
    mar = c(0, 0, 0, 0),
    oma = c(0, 0, 0, 0),
    xaxs = "i",
    yaxs = "i",
    xpd = NA
  )
  graphics::plot.new()
  graphics::plot.window(xlim = c(0, 1), ylim = c(0, 1), asp = NA)
  graphics::rect(-0.02, -0.02, 1.02, 1.02, col = theme$bg, border = NA)
  graphics::rect(0.008, 0.008, 0.992, 0.992, col = theme$panel, border = theme$line, lwd = 1.2)
  graphics::rect(0.028, 0.028, 0.972, 0.972, border = theme$gold, lwd = 0.8)
}

pdf_text_page <- function(title, lines, cex = 0.82, theme = pdf_theme()) {
  pdf_new_page(theme)
  y <- 0.95
  graphics::text(0.06, y, title, adj = c(0, 1), cex = 1.25, font = 2, col = theme$ink)
  graphics::segments(0.06, y - 0.035, 0.94, y - 0.035, col = theme$line, lwd = 1)
  y <- y - 0.06
  for (line in lines) {
    wrapped <- strwrap(as.character(line), width = 88)
    for (part in wrapped) {
      if (y < 0.06) {
        pdf_new_page(theme)
        y <- 0.95
      }
      graphics::text(0.06, y, part, adj = c(0, 1), cex = cex, col = theme$muted)
      y <- y - 0.035
    }
    y <- y - 0.012
  }
}

pdf_prepare_table <- function(df) {
  if (!is.data.frame(df) || nrow(df) == 0L || ncol(df) == 0L) {
    return(NULL)
  }
  df <- set_apply_column_labels(df)
  df <- df[, seq_len(min(ncol(df), 4L)), drop = FALSE]
  as.data.frame(lapply(df, function(col) {
    value <- as.character(col)
    value[is.na(value) | !nzchar(value)] <- "—"
    gsub("[\r\n\t]+", " ", value)
  }), stringsAsFactors = FALSE, check.names = FALSE)
}

pdf_draw_table_rows <- function(df, rows, top, theme) {
  if (is.null(df) || length(rows) == 0L) {
    return(top)
  }
  left <- 0.07
  right <- 0.93
  row_h <- 0.058
  header_h <- 0.054
  col_w <- (right - left) / ncol(df)
  graphics::rect(left, top - header_h, right, top, col = theme$accent, border = theme$line, lwd = 1)
  for (j in seq_len(ncol(df))) {
    x0 <- left + (j - 1L) * col_w
    graphics::rect(x0, top - header_h, x0 + col_w, top, border = theme$line, lwd = 0.8)
    graphics::text(x0 + 0.009, top - 0.014, names(df)[j], adj = c(0, 1), cex = 0.72, font = 2, col = theme$ink)
  }
  y <- top - header_h
  for (i in rows) {
    graphics::rect(left, y - row_h, right, y, col = adjustcolor(theme$panel, alpha.f = 0.92), border = theme$line, lwd = 0.6)
    for (j in seq_len(ncol(df))) {
      x0 <- left + (j - 1L) * col_w
      graphics::rect(x0, y - row_h, x0 + col_w, y, border = theme$line, lwd = 0.5)
      parts <- strwrap(df[i, j][[1]], width = max(10L, floor(col_w * 56)))
      parts <- utils::head(parts, 3L)
      graphics::text(x0 + 0.009, y - 0.010, paste(parts, collapse = "\n"), adj = c(0, 1), cex = 0.66, col = theme$ink)
    }
    y <- y - row_h
  }
  y
}

pdf_draw_section <- function(section, theme) {
  title <- section$title %||% "Раздел"
  text_lines <- strwrap(section$text %||% "", width = 90)
  if (length(text_lines) == 0L) {
    text_lines <- "Без текстового описания."
  }
  df <- pdf_prepare_table(section$table)
  first_text_lines <- utils::head(text_lines, 10L)
  remaining_text <- text_lines[-seq_along(first_text_lines)]

  pdf_new_page(theme)
  graphics::text(0.07, 0.93, title, adj = c(0, 1), cex = 1.15, font = 2, col = theme$ink)
  graphics::segments(0.07, 0.895, 0.93, 0.895, col = theme$line, lwd = 1)
  y <- 0.865
  for (line in first_text_lines) {
    graphics::text(0.07, y, line, adj = c(0, 1), cex = 0.77, col = theme$muted)
    y <- y - 0.028
  }
  if (length(remaining_text) > 0L) {
    graphics::text(0.07, y, "Продолжение описания перенесено на следующий лист.", adj = c(0, 1), cex = 0.72, font = 3, col = theme$gold)
    y <- y - 0.04
  }
  if (is.null(df)) {
    graphics::text(0.07, y - 0.03, "Нет данных для этого раздела.", adj = c(0, 1), cex = 0.76, col = theme$ink)
  } else {
    first_rows <- seq_len(min(nrow(df), 5L))
    y <- y - 0.03
    y <- pdf_draw_table_rows(df, first_rows, y, theme)

    if (nrow(df) > length(first_rows)) {
      remaining <- seq.int(length(first_rows) + 1L, nrow(df))
      page_groups <- split(remaining, ceiling(seq_along(remaining) / 10L))
      for (page_rows in page_groups) {
        pdf_new_page(theme)
        graphics::text(0.07, 0.93, paste(title, "— продолжение таблицы"), adj = c(0, 1), cex = 1.05, font = 2, col = theme$ink)
        graphics::segments(0.07, 0.895, 0.93, 0.895, col = theme$line, lwd = 1)
        pdf_draw_table_rows(df, page_rows, 0.86, theme)
      }
    }
  }

  if (length(remaining_text) > 0L) {
    text_groups <- split(remaining_text, ceiling(seq_along(remaining_text) / 16L))
    for (group in text_groups) {
      pdf_new_page(theme)
      graphics::text(0.07, 0.93, paste(title, "— продолжение описания"), adj = c(0, 1), cex = 1.05, font = 2, col = theme$ink)
      graphics::segments(0.07, 0.895, 0.93, 0.895, col = theme$line, lwd = 1)
      yy <- 0.865
      for (line in group) {
        graphics::text(0.07, yy, line, adj = c(0, 1), cex = 0.77, col = theme$muted)
        yy <- yy - 0.028
      }
    }
  }

  invisible(NULL)
}

pdf_draw_title_page <- function(report, record, theme_mode = "hell") {
  theme <- pdf_theme(theme_mode)
  pdf_new_page(theme)
  is_heaven <- identical(theme_mode, "heaven")
  mythology <- report_mythology_value(report, fallback = record$mythology_style %||% "egypt")
  is_norse <- identical(mythology, "norse")
  world_title <- if (is_heaven) "ОТЧЕТ БОЖЕСТВЕННОГО\nМИРА" else "ОТЧЕТ ЗАГРОБНОГО\nМИРА"
  oath <- if (is_heaven) {
    "Свидетельство собрано под светом небесного суда: факты отделены от догадок, а следы цели сохранены в порядке."
  } else {
    "Свидетельство собрано у врат нижнего суда: факты отделены от догадок, а следы цели сохранены в порядке."
  }
  logo_file <- if (is_norse) {
    if (is_heaven) pdf_asset_path("githound-norse-heaven-logo.png") else pdf_asset_path("githound-norse-logo.png")
  } else {
    if (is_heaven) pdf_asset_path("githound-heaven-logo.png") else pdf_asset_path("githound-hell-logo.png")
  }
  seal_files <- if (is_norse) {
    c(
      if (is_heaven) "seal-odin-heaven.png" else "seal-odin.png",
      if (is_heaven) "seal-tyr-heaven.png" else "seal-tyr.png",
      if (is_heaven) "seal-heimdall-heaven.png" else "seal-heimdall.png"
    )
  } else {
    c("seal-isis.png", "seal-set.png", "seal-anubis.png")
  }
  seal_names <- if (is_norse) c("Один", "Тюр", "Хеймдалль") else c("Исида", "Сет", "Анубис")
  pdf_draw_raster(logo_file, 0.36, 0.78, 0.64, 0.98)
  graphics::text(0.5, 0.73, "GITHOUND", adj = c(0.5, 0.5), cex = 2.1, font = 2, col = if (is_heaven) "#d31d1d" else theme$ink)
  graphics::text(0.5, 0.58, world_title, adj = c(0.5, 0.5), cex = 2.55, font = 2, col = theme$ink)
  graphics::text(0.5, 0.42, paste("Цель:", report$profile %||% record$target %||% "—"), adj = c(0.5, 0.5), cex = 1.24, font = 2, col = theme$ink)
  oath_lines <- strwrap(oath, width = 70)
  y <- 0.35
  for (line in oath_lines) {
    graphics::text(0.5, y, line, adj = c(0.5, 0.5), cex = 0.94, col = theme$muted)
    y <- y - 0.03
  }
  seal_y0 <- 0.065
  seal_y1 <- 0.255
  pdf_draw_raster(pdf_asset_path(seal_files[[1]]), 0.11, seal_y0, 0.33, seal_y1)
  pdf_draw_raster(pdf_asset_path(seal_files[[2]]), 0.39, seal_y0, 0.61, seal_y1)
  pdf_draw_raster(pdf_asset_path(seal_files[[3]]), 0.67, seal_y0, 0.89, seal_y1)
  graphics::text(0.22, 0.055, seal_names[[1]], adj = c(0.5, 0.5), cex = 1.02, font = 2, col = theme$ink)
  graphics::text(0.50, 0.055, seal_names[[2]], adj = c(0.5, 0.5), cex = 1.02, font = 2, col = theme$ink)
  graphics::text(0.78, 0.055, seal_names[[3]], adj = c(0.5, 0.5), cex = 1.02, font = 2, col = theme$ink)
}

pdf_draw_plot_page <- function(path, label, theme) {
  pdf_new_page(theme)
  graphics::text(0.07, 0.93, "Визуальный график", adj = c(0, 1), cex = 1.1, font = 2, col = theme$ink)
  graphics::text(0.07, 0.885, label, adj = c(0, 1), cex = 0.8, col = theme$muted)
  graphics::rect(0.07, 0.12, 0.93, 0.84, col = "#ffffff", border = theme$line, lwd = 1)
  if (!pdf_draw_raster(path, 0.085, 0.135, 0.915, 0.825)) {
    graphics::text(0.5, 0.5, "Не удалось встроить график в PDF.", adj = c(0.5, 0.5), cex = 0.9, col = theme$ink)
  }
}

write_single_report_pdf <- function(record, file) {
  report <- record$report
  theme_mode <- report_theme_value(report, fallback = "hell")
  theme <- pdf_theme(theme_mode)
  grDevices::cairo_pdf(file, width = 8.27, height = 11.69, family = "Arial")
  on.exit(grDevices::dev.off(), add = TRUE)

  pdf_draw_title_page(report, record, theme_mode = theme_mode)

  for (section in report$sections %||% list()) {
    pdf_draw_section(section, theme = theme)
  }

  plot_paths <- report$plots %||% character()
  plot_paths <- plot_paths[file.exists(plot_paths)]
  if (length(plot_paths) > 0L) {
    for (path in plot_paths) {
      pdf_draw_plot_page(path, set_plot_label(path), theme = theme)
    }
  }

  invisible(file)
}

safe_report_filename <- function(record) {
  protocol <- tolower(record$protocol_type %||% record$report$protocol_type %||% "report")
  target <- gsub("[^[:alnum:]_-]+", "_", record$target %||% "report")
  target <- gsub("_+", "_", target)
  target <- trimws(target, whitespace = "_")
  if (!nzchar(target)) target <- "report"
  paste0("githound_", protocol, "_", target, "_", format(Sys.time(), "%Y%m%d_%H%M%S"), ".pdf")
}

write_reports_zip <- function(records, file) {
  if (length(records) == 0L) {
    stop("Не выбраны отчеты для скачивания.", call. = FALSE)
  }
  if (!requireNamespace("zip", quietly = TRUE)) {
    zip_bin <- Sys.which("zip")
    if (!nzchar(zip_bin)) {
      stop("Для скачивания нескольких PDF нужен R-пакет zip или системная утилита zip.", call. = FALSE)
    }
  }

  temp_dir <- tempfile("githound_pdf_")
  dir.create(temp_dir, recursive = TRUE, showWarnings = FALSE)
  on.exit(unlink(temp_dir, recursive = TRUE, force = TRUE), add = TRUE)

  pdf_files <- vapply(seq_along(records), function(i) {
    record <- records[[i]]
    filename <- sub("\\.pdf$", paste0("_", i, ".pdf"), safe_report_filename(record))
    path <- file.path(temp_dir, filename)
    write_single_report_pdf(record, path)
    path
  }, character(1))

  if (requireNamespace("zip", quietly = TRUE)) {
    zip::zipr(zipfile = file, files = basename(pdf_files), root = temp_dir)
  } else {
    old <- setwd(temp_dir)
    on.exit(setwd(old), add = TRUE)
    utils::zip(zipfile = file, files = basename(pdf_files), flags = "-q")
  }
  invisible(file)
}

archive_table_ui <- function(records, page = 1L, page_size = 15L) {
  if (!is.list(records) || length(records) == 0L) {
    return(div(class = "set-empty", "Тот еще собирает свои свитки."))
  }

  total_pages <- max(1L, ceiling(length(records) / page_size))
  page <- min(max(1L, as.integer(page)), total_pages)
  start <- (page - 1L) * page_size + 1L
  end <- min(length(records), start + page_size - 1L)
  page_records <- records[start:end]

  tagList(
    div(
      class = "archive-table-wrap",
      tags$table(
        class = "archive-table",
        tags$thead(
          tags$tr(
            tags$th(class = "archive-check-cell", ""),
            tags$th("Цель"),
            tags$th("Протокол"),
            tags$th("Статус"),
            tags$th("Создан"),
            tags$th("Хранится до"),
            tags$th("Действие")
          )
        ),
        tags$tbody(
          lapply(page_records, function(item) {
            tags$tr(
              tags$td(class = "archive-check-cell", tags$input(type = "checkbox", class = "archive-row-select", value = item$id %||% "")),
              tags$td(item$target %||% ""),
              tags$td(item$protocol_label %||% archive_protocol_label(item$protocol_type %||% "")),
              tags$td(archive_status_label(item$status %||% "active")),
              tags$td(item$created_at_label %||% ""),
              tags$td(item$expires_at_label %||% ""),
              tags$td(tags$button(type = "button", class = "archive-open-btn", `data-id` = item$id %||% "", "Открыть"))
            )
          })
        )
      )
    ),
    div(
      class = "archive-pager",
      actionButton("archive_prev", "Назад", class = "menu-button secondary-button"),
      span(class = "archive-page-label", paste("Страница", page, "из", total_pages)),
      actionButton("archive_next", "Вперед", class = "menu-button secondary-button")
    )
  )
}

archive_screen <- function(records, page = 1L) {
  if (!is.list(records)) {
    records <- list()
  }

  div(
    class = "home archive-page",
    div(
      class = "form archive-form",
      h2(class = "section-title", "Архив отчетов"),
      p(class = "account-copy", "Активные отчеты хранятся 7 дней. После этого они получают статус «заархивировано»."),
      archive_table_ui(records, page = page, page_size = 15L),
      if (length(records) > 0L) {
        div(
          class = "archive-actions",
          downloadButton("download_archive_reports", "Скачать выбранные", class = "menu-button secondary-button"),
          actionButton("delete_archive_reports", "Удалить выбранные", class = "menu-button secondary-button")
        )
      },
      actionButton("go_analysis", "Вернуться на главный экран", class = "menu-button secondary-button")
    ),
    div(
      class = "archive-thoth",
      img(src = "archive-thoth.png", class = "archive-thoth-img archive-thoth-hell", alt = "Тот с папирусом"),
      img(src = "archive-thoth-heaven.png", class = "archive-thoth-img archive-thoth-heaven", alt = "Тот с папирусом"),
      img(src = "archive-raven-norse.png", class = "archive-thoth-img archive-raven-norse", alt = "Ворон Одина со свитком"),
      img(src = "archive-raven-norse-heaven.png", class = "archive-thoth-img archive-raven-norse-heaven", alt = "Ворон Одина со свитком")
    )
  )
}

ui <- fluidPage(
  tags$head(
    tags$title("GitHound"),
    tags$script(HTML("
      function setInputValueIfPresent(id, value) {
        var input = $('#' + id);
        if (input.length) {
          input.val(value).trigger('input').trigger('change');
        }
      }

      function setupLoginAutofill() {
        var email = $('#login_email');
        var password = $('#login_password');
        if (email.length) {
          email.attr('autocomplete', 'username');
          email.attr('name', 'username');
        }
        if (password.length) {
          password.attr('autocomplete', 'current-password');
          password.attr('name', 'current-password');
        }
      }

      function setCookie(name, value, maxAgeSeconds) {
        document.cookie = name + '=' + encodeURIComponent(value) +
          '; max-age=' + maxAgeSeconds + '; path=/; SameSite=Lax';
      }

      function getCookie(name) {
        var prefix = name + '=';
        var parts = document.cookie.split(';');
        for (var i = 0; i < parts.length; i++) {
          var part = parts[i].trim();
          if (part.indexOf(prefix) === 0) {
            return decodeURIComponent(part.substring(prefix.length));
          }
        }
        return null;
      }

      function clearLegacyRememberedLogin() {
        window.localStorage.removeItem('githound_login_memory');
        setCookie('githound_login_memory', '', 0);
      }

      function pushRememberToken() {
        if (!window.Shiny) return;
        var token = getCookie('githound_remember_token');
        Shiny.setInputValue('remember_token', token || '', {priority: 'event'});
      }

      function fillRememberedLogin() {
        var token = getCookie('githound_remember_token');
        if ($('#remember_me').length) {
          var enabled = !!token;
          $('#remember_me').prop('checked', enabled).trigger('change');
          if (window.Shiny) {
            Shiny.setInputValue('remember_me', enabled, {priority: 'event'});
          }
        }
        try {
          var rememberedEmail = window.localStorage.getItem('githound_remember_email') || '';
          if (rememberedEmail) {
            setInputValueIfPresent('login_email', rememberedEmail);
            if (window.Shiny) {
              Shiny.setInputValue('login_email', rememberedEmail, {priority: 'event'});
            }
          }
        } catch (error) {}
      }

      function storeRememberedLoginFromForm() {
        return $('#remember_me').is(':checked');
      }

      function applyMythologyStyle(style) {
        style = style || 'egypt';
        document.body.classList.remove('myth-egypt', 'myth-norse', 'myth-greece');
        document.body.classList.add('myth-' + style);
      }

      function getGithubOAuthState() {
        try {
          return getCookie('githound_github_oauth_state') ||
            window.localStorage.getItem('githound_github_oauth_state') ||
            '';
        } catch (error) {
          return getCookie('githound_github_oauth_state') || '';
        }
      }

      function pushGithubOAuthState() {
        if (!window.Shiny) return;
        Shiny.setInputValue('github_oauth_state_cookie', getGithubOAuthState(), {priority: 'event'});
      }

      $(document).ready(function() {
        clearLegacyRememberedLogin();
        fillRememberedLogin();
        setupLoginAutofill();
        setTimeout(pushRememberToken, 0);
        setTimeout(pushGithubOAuthState, 0);
        setTimeout(pushGithubOAuthState, 150);
        setTimeout(pushGithubOAuthState, 500);
        if (window.Shiny) {
          Shiny.setInputValue('theme_mode', document.body.classList.contains('heaven-theme') ? 'heaven' : 'hell', {priority: 'event'});
        }
        applyMythologyStyle('egypt');
        var observer = new MutationObserver(function() {
          fillRememberedLogin();
          setupLoginAutofill();
          restoreArchiveSelection();
        });
        observer.observe(document.body, { childList: true, subtree: true });
      });

      Shiny.addCustomMessageHandler('rememberToken', function(data) {
        if (data && data.token) {
          setCookie('githound_remember_token', data.token, data.maxAgeSeconds || (30 * 24 * 60 * 60));
          if (data.email) {
            window.localStorage.setItem('githound_remember_email', data.email);
          }
        } else {
          setCookie('githound_remember_token', '', 0);
          window.localStorage.removeItem('githound_remember_email');
        }
        fillRememberedLogin();
      });

      Shiny.addCustomMessageHandler('startGithubOAuth', function(data) {
        if (!data || !data.url || !data.state) return;
        setCookie('githound_github_oauth_state', data.state, 10 * 60);
        try {
          window.localStorage.setItem('githound_github_oauth_state', data.state);
        } catch (error) {}
        window.location.href = data.url;
      });

      Shiny.addCustomMessageHandler('finishGithubOAuth', function(data) {
        setCookie('githound_github_oauth_state', '', 0);
        try {
          window.localStorage.removeItem('githound_github_oauth_state');
        } catch (error) {}
        if (window.history && window.history.replaceState) {
          window.history.replaceState({}, document.title, window.location.pathname);
        }
      });

      Shiny.addCustomMessageHandler('applyMythologyStyle', function(data) {
        applyMythologyStyle(data && data.style ? data.style : 'egypt');
      });
      function renderSetProgressLabel(label) {
        var target = $('#set_progress_label');
        if (!target.length) return;
        var text = String(label || '');
        var parts = text.split('\\n');
        target.empty();
        $('<span/>', {
          'class': 'set-progress-stage',
          text: parts[0] || ''
        }).appendTo(target);
        if (parts.length > 1) {
          $('<span/>', {
            'class': 'set-progress-working',
            text: parts.slice(1).join('\\n')
          }).appendTo(target);
        }
      }

      Shiny.addCustomMessageHandler('setSetProgress', function(data) {
        var value = Math.max(0, Math.min(100, parseInt(data.value || 0, 10)));
        $('#set_progress_track').css('--set-progress', value + '%');
        $('#set_progress_percent').text(value + '%');
        if (data.label) {
          renderSetProgressLabel(data.label);
        }
      });

      function copyTextToClipboard(text) {
        if (navigator.clipboard && window.isSecureContext) {
          return navigator.clipboard.writeText(text);
        }
        return new Promise(function(resolve, reject) {
          var area = document.createElement('textarea');
          area.value = text;
          area.setAttribute('readonly', '');
          area.style.position = 'fixed';
          area.style.left = '-9999px';
          document.body.appendChild(area);
          area.select();
          try {
            document.execCommand('copy') ? resolve() : reject(new Error('copy failed'));
          } catch (error) {
            reject(error);
          } finally {
            document.body.removeChild(area);
          }
        });
      }

      $(document).on('click', '.copy-commit-button', function() {
        var button = $(this);
        var commit = String(button.data('commit') || '');
        if (!commit) return;
        copyTextToClipboard(commit).then(function() {
          var previous = button.text();
          button.addClass('copied').text('OK');
          window.setTimeout(function() {
            button.removeClass('copied').text(previous);
          }, 900);
        });
      });

      function fetchEpssButton(button) {
        var cve = String(button.attr('data-cve') || '').trim().toUpperCase();
        var cell = button.closest('.epss-cell');
        var value = cell.find('.epss-value');
        if (!cve || button.prop('disabled')) return Promise.resolve(false);

        button.prop('disabled', true).text('...');
        return fetch('https://api.first.org/data/v1/epss?cve=' + encodeURIComponent(cve), {
          method: 'GET',
          headers: { 'Accept': 'application/json' }
        })
          .then(function(response) {
            if (!response.ok) throw new Error('EPSS HTTP ' + response.status);
            return response.json();
          })
          .then(function(payload) {
            var row = payload && payload.data && payload.data.length ? payload.data[0] : null;
            if (!row || row.epss === undefined || row.epss === null) {
              value.text('нет данных');
              return;
            }
            var epss = Number(row.epss);
            var percentile = Number(row.percentile);
            var text = isFinite(epss) ? (epss * 100).toFixed(2) + '%' : String(row.epss);
            value.empty().append(document.createTextNode(text));
            if (isFinite(percentile)) {
              value.append($('<span/>', {
                class: 'epss-percentile',
                text: 'Perc: ' + (percentile * 100).toFixed(1) + '%'
              }));
            }
            button.hide();
          })
          .catch(function(error) {
            value.text('ошибка');
            button.attr('title', error.message || 'EPSS request failed');
          })
          .finally(function() {
            if (button.is(':visible')) {
              button.prop('disabled', false).text('EPSS');
            }
          });
      }

      $(document).on('click', '.epss-fetch-button', function() {
        fetchEpssButton($(this));
      });

      $(document).on('click', '.epss-fetch-all-button', function() {
        var allButton = $(this);
        if (allButton.prop('disabled')) return;
        var table = allButton.closest('table');
        var buttons = table.find('tbody .epss-fetch-button:visible').toArray();
        if (!buttons.length) return;

        allButton.prop('disabled', true).text('...');
        var chain = Promise.resolve();
        buttons.forEach(function(btn) {
          chain = chain.then(function() {
            return fetchEpssButton($(btn));
          });
        });
        chain.finally(function() {
          allButton.prop('disabled', false).text(String.fromCodePoint(128065));
        });
      });

      $(document).on('click', '.full-text-button', function() {
        if (!window.Shiny) return;
        var button = $(this);
        Shiny.setInputValue('show_full_text', {
          title: String(button.attr('data-title') || 'Описание уязвимости'),
          summary: String(button.attr('data-full-summary') || ''),
          details: String(button.attr('data-full-details') || ''),
          aliases: String(button.attr('data-full-aliases') || ''),
          references: String(button.attr('data-full-references') || ''),
          nonce: Date.now()
        }, {priority: 'event'});
      });

      function sortableCellValue(row, index) {
        var cell = row.children[index];
        if (!cell) return '';
        var raw = cell.getAttribute('data-sort');
        if (raw === null || raw === undefined) raw = cell.textContent || '';
        return String(raw).trim();
      }

      function compareSortableValues(a, b) {
        var emptyA = !a || a === '—';
        var emptyB = !b || b === '—';
        if (emptyA && emptyB) return 0;
        if (emptyA) return 1;
        if (emptyB) return -1;
        var normalizedA = a.replace(',', '.').replace(/\\s+/g, '');
        var normalizedB = b.replace(',', '.').replace(/\\s+/g, '');
        var numberA = Number(normalizedA);
        var numberB = Number(normalizedB);
        if (!Number.isNaN(numberA) && !Number.isNaN(numberB)) {
          return numberA - numberB;
        }
        var dateA = Date.parse(a);
        var dateB = Date.parse(b);
        if (!Number.isNaN(dateA) && !Number.isNaN(dateB)) {
          return dateA - dateB;
        }
        return a.localeCompare(b, 'ru', {numeric: true, sensitivity: 'base'});
      }

      $(document).on('click', '.set-sortable-table th', function() {
        var header = $(this);
        var table = header.closest('table')[0];
        if (!table || !table.tBodies.length) return;
        var index = Number(header.attr('data-sort-index')) - 1;
        if (Number.isNaN(index) || index < 0) return;
        var current = header.attr('data-sort-dir') || 'none';
        var direction = current === 'asc' ? 'desc' : 'asc';
        var rows = Array.prototype.slice.call(table.tBodies[0].rows);
        rows.sort(function(rowA, rowB) {
          var result = compareSortableValues(sortableCellValue(rowA, index), sortableCellValue(rowB, index));
          return direction === 'asc' ? result : -result;
        });
        rows.forEach(function(row) {
          table.tBodies[0].appendChild(row);
        });
        $(table).find('th').removeAttr('data-sort-dir').removeClass('sorted-asc sorted-desc')
          .find('.set-sort-indicator').text('↕');
        header.attr('data-sort-dir', direction).addClass(direction === 'asc' ? 'sorted-asc' : 'sorted-desc')
          .find('.set-sort-indicator').text(direction === 'asc' ? '↑' : '↓');
      });

      $(document).on('click', '#theme_hell', function() {
        document.body.classList.remove('heaven-theme');
        $('#theme_hell').addClass('active').attr('aria-pressed', 'true');
        $('#theme_heaven').removeClass('active').attr('aria-pressed', 'false');
        if (window.Shiny) {
          Shiny.setInputValue('theme_mode', 'hell', {priority: 'event'});
        }
      });

      $(document).on('click', '#submit_login', function() {
        if (window.Shiny) {
          Shiny.setInputValue('remember_me', storeRememberedLoginFromForm(), {priority: 'event'});
        }
      });

      $(document).on('click', '#theme_heaven', function() {
        document.body.classList.add('heaven-theme');
        $('#theme_heaven').addClass('active').attr('aria-pressed', 'true');
        $('#theme_hell').removeClass('active').attr('aria-pressed', 'false');
        if (window.Shiny) {
          Shiny.setInputValue('theme_mode', 'heaven', {priority: 'event'});
        }
      });

      function publishArchiveSelection() {
        if (!window.Shiny) return;
        window.githoundArchiveSelected = window.githoundArchiveSelected || {};
        $('.archive-row-select').each(function() {
          if (this.checked) {
            window.githoundArchiveSelected[this.value] = true;
          } else {
            delete window.githoundArchiveSelected[this.value];
          }
        });
        Shiny.setInputValue('archive_selected_ids', Object.keys(window.githoundArchiveSelected), {priority: 'event'});
      }

      function restoreArchiveSelection() {
        window.githoundArchiveSelected = window.githoundArchiveSelected || {};
        $('.archive-row-select').each(function() {
          this.checked = !!window.githoundArchiveSelected[this.value];
        });
      }

      $(document).on('change', '.archive-row-select', publishArchiveSelection);

      $(document).on('click', '.archive-open-btn', function() {
        if (window.Shiny) {
          Shiny.setInputValue('archive_open_id', $(this).data('id'), {priority: 'event'});
        }
      });

      $(document).on('click', '.set-plot-button', function() {
        if (window.Shiny) {
          Shiny.setInputValue('plot_modal', {
            src: $(this).data('src'),
            label: $(this).data('label')
          }, {priority: 'event'});
        }
      });

      Shiny.addCustomMessageHandler('clearArchiveSelection', function() {
        window.githoundArchiveSelected = {};
        restoreArchiveSelection();
        if (window.Shiny) {
          Shiny.setInputValue('archive_selected_ids', [], {priority: 'event'});
        }
      });
    ")),
    tags$style(HTML("
      :root {
        --page-bg: #080101;
        --panel: rgba(18, 2, 4, 0.9);
        --ink: #fff5f5;
        --muted: #c98b8b;
        --line: #7c1118;
        --field-bg: #0d0506;
        --accent: #e01824;
        --accent-hover: #ff2b34;
        --button-ink: #ffffff;
        --button-shadow: #090a0f;
        --secondary-bg: #130304;
        --secondary-ink: #ffffff;
        --secondary-line: #090a0f;
        --glitch-main: rgba(255, 43, 52, 0.88);
        --glitch-soft: rgba(255, 105, 30, 0.72);
        --blood: #8f0008;
        --theme-duration: 420ms;
        --theme-ease: cubic-bezier(0.22, 1, 0.36, 1);
      }

      html,
      body {
        min-height: 100%;
        background-color: var(--page-bg);
      }

      body {
        background:
          linear-gradient(rgba(255, 43, 52, 0.05) 1px, transparent 1px),
          linear-gradient(90deg, rgba(255, 43, 52, 0.035) 1px, transparent 1px),
          radial-gradient(circle at 50% 18%, rgba(255, 59, 31, 0.2), transparent 28%),
          radial-gradient(circle at 18% 78%, rgba(143, 0, 8, 0.24), transparent 31%),
          radial-gradient(circle at 86% 62%, rgba(92, 0, 6, 0.24), transparent 29%),
          linear-gradient(180deg, #180304 0%, #080101 48%, #020000 100%),
          var(--page-bg);
        background-size: 28px 28px, 28px 28px, auto, auto, auto, auto, auto;
        color: var(--ink);
        font-family: Arial, Helvetica, sans-serif;
        transition: color var(--theme-duration) var(--theme-ease),
          background-color var(--theme-duration) var(--theme-ease);
      }

      .recalculating,
      .shiny-bound-output.recalculating,
      .shiny-output-error,
      .shiny-output-error::before {
        opacity: 1 !important;
      }

      body.heaven-theme {
        --page-bg: #fffaf0;
        --panel: rgba(255, 255, 255, 0.84);
        --ink: #1f2a34;
        --muted: #6d7c87;
        --line: #96c8dc;
        --field-bg: #ffffff;
        --accent: #d9a52e;
        --accent-hover: #f0bd45;
        --button-ink: #1d2024;
        --button-shadow: rgba(120, 154, 174, 0.62);
        --secondary-bg: #e9f8ff;
        --secondary-ink: #1f5d78;
        --secondary-line: #8ecce3;
        --glitch-main: rgba(255, 214, 107, 0.78);
        --glitch-soft: rgba(125, 203, 235, 0.68);
        --blood: #e2b641;
      }

      body::before,
      body::after {
        content: '';
        position: fixed;
        inset: 0;
        z-index: 0;
        pointer-events: none;
      }

      body::before {
        background:
          linear-gradient(rgba(125, 203, 235, 0.12) 1px, transparent 1px),
          linear-gradient(90deg, rgba(217, 165, 46, 0.08) 1px, transparent 1px),
          radial-gradient(circle at 50% 12%, rgba(255, 229, 153, 0.64), transparent 30%),
          radial-gradient(circle at 16% 76%, rgba(255, 255, 255, 0.82), transparent 34%),
          radial-gradient(circle at 84% 66%, rgba(142, 204, 227, 0.28), transparent 32%),
          linear-gradient(180deg, #fffefe 0%, #eefaff 48%, #fff1bd 100%);
        background-size: 30px 30px, 30px 30px, auto, auto, auto, auto;
        opacity: 0;
        transition: opacity var(--theme-duration) var(--theme-ease);
      }

      body.heaven-theme::before {
        opacity: 1;
      }

      body.myth-norse:not(.heaven-theme) {
        --panel: rgba(14, 8, 8, 0.92);
        --line: #c33418;
        --accent: #ff1e28;
        --accent-hover: #ff454f;
        background:
          linear-gradient(rgba(6, 0, 0, 0.1), rgba(0, 0, 0, 0.28)),
          url('norse-hell-bg.png'),
          radial-gradient(circle at 48% 54%, rgba(255, 79, 18, 0.22), transparent 30%),
          linear-gradient(180deg, #101015 0%, #050304 55%, #000000 100%);
        background-size: auto, cover, auto, auto;
        background-position: center, center, center, center;
        background-attachment: fixed;
      }

      body.myth-norse:not(.heaven-theme)::before {
        background:
          radial-gradient(circle at 22% 35%, rgba(255, 188, 58, 0.18), transparent 18%),
          radial-gradient(circle at 64% 42%, rgba(255, 82, 15, 0.2), transparent 20%),
          radial-gradient(circle at 78% 72%, rgba(255, 142, 24, 0.14), transparent 16%);
        mix-blend-mode: screen;
        opacity: 0.68;
        animation: norse-lava-pulse 2.9s ease-in-out infinite;
      }

      body.myth-norse.heaven-theme {
        --page-bg: #061c3c;
        --panel: rgba(238, 249, 255, 0.86);
        --ink: #102334;
        --muted: #557489;
        --line: #7dbbe2;
        --accent: #9bdcff;
        --accent-hover: #ffffff;
        --secondary-bg: #effbff;
        --secondary-ink: #174d68;
        --secondary-line: #8fd4f2;
      }

      body.myth-norse.heaven-theme::before {
        background:
          linear-gradient(rgba(238, 250, 255, 0.2), rgba(197, 229, 245, 0.12)),
          url('norse-heaven-bg.png'),
          radial-gradient(circle at 50% 42%, rgba(225, 249, 255, 0.86), rgba(132, 211, 244, 0.48) 20%, rgba(20, 88, 140, 0.44) 42%, transparent 68%),
          radial-gradient(circle at 18% 82%, rgba(125, 203, 235, 0.3), transparent 34%),
          radial-gradient(circle at 84% 18%, rgba(255, 255, 255, 0.28), transparent 30%),
          linear-gradient(180deg, #07142f 0%, #0c315d 48%, #051222 100%);
        background-size: auto, cover, auto, auto, auto, auto;
        background-position: center, center, center, center, center, center;
        background-attachment: fixed;
        opacity: 1;
      }

      body.myth-norse.heaven-theme::after {
        content: '';
        inset: 0;
        width: auto;
        height: auto;
        background:
          url('data:image/svg+xml,%3Csvg xmlns=%22http://www.w3.org/2000/svg%22 width=%221280%22 height=%22720%22 viewBox=%220 0 1280 720%22%3E%3Cg fill=%22none%22 stroke=%22%23ffffff%22 stroke-linecap=%22square%22 stroke-linejoin=%22miter%22 opacity=%22.52%22%3E%3Cg stroke-width=%226%22 transform=%22translate(48 42) rotate(-8)%22%3E%3Cpath d=%22M0 0v92M0 30l58-26M0 57l52 27%22/%3E%3C/g%3E%3Cg stroke-width=%227%22 transform=%22translate(206 86) rotate(13) scale(1.15)%22%3E%3Cpath d=%22M0 0l62 38-62 42z%22/%3E%3C/g%3E%3Cg stroke-width=%226%22 transform=%22translate(392 34) rotate(0) scale(.95)%22%3E%3Cpath d=%22M0 0v96M58 0v96M0 48h58%22/%3E%3C/g%3E%3Cg stroke-width=%228%22 transform=%22translate(560 62) rotate(-18) scale(1.25)%22%3E%3Cpath d=%22M0 0l76 92M76 0L0 92%22/%3E%3C/g%3E%3Cg stroke-width=%226%22 transform=%22translate(820 42) rotate(9)%22%3E%3Cpath d=%22M0 0l62 38-62 42z%22/%3E%3C/g%3E%3Cg stroke-width=%226%22 transform=%22translate(1036 78) rotate(-4)%22%3E%3Cpath d=%22M0 0v88M58 0v88M0 44h58%22/%3E%3C/g%3E%3Cg stroke-width=%227%22 transform=%22translate(1162 44) rotate(16) scale(1.08)%22%3E%3Cpath d=%22M0 0l74 88M74 0L0 88%22/%3E%3C/g%3E%3Cg stroke-width=%226%22 transform=%22translate(88 228) rotate(2) scale(1.1)%22%3E%3Cpath d=%22M0 0v92M0 0h62M0 46h48%22/%3E%3C/g%3E%3Cg stroke-width=%227%22 transform=%22translate(286 258) rotate(-24) scale(1.2)%22%3E%3Cpath d=%22M64 0L0 45l64 45%22/%3E%3C/g%3E%3Cg stroke-width=%226%22 transform=%22translate(462 216) rotate(14)%22%3E%3Cpath d=%22M0 0v92M0 44l70-36M0 44l70 36%22/%3E%3C/g%3E%3Cg stroke-width=%227%22 transform=%22translate(650 246) rotate(-2) scale(1.18)%22%3E%3Cpath d=%22M0 0v94M0 0h58v94%22/%3E%3C/g%3E%3Cg stroke-width=%226%22 transform=%22translate(858 230) rotate(21)%22%3E%3Cpath d=%22M0 0l62 38-62 42z%22/%3E%3C/g%3E%3Cg stroke-width=%227%22 transform=%22translate(1030 250) rotate(-15) scale(1.14)%22%3E%3Cpath d=%22M0 0v92M0 44l70-36M0 44l70 36%22/%3E%3C/g%3E%3Cg stroke-width=%226%22 transform=%22translate(1188 226) rotate(8) scale(1.08)%22%3E%3Cpath d=%22M0 0v96h54%22/%3E%3C/g%3E%3Cg stroke-width=%227%22 transform=%22translate(38 438) rotate(-13) scale(1.2)%22%3E%3Cpath d=%22M0 0l72 86M72 0L0 86%22/%3E%3C/g%3E%3Cg stroke-width=%226%22 transform=%22translate(220 490) rotate(8)%22%3E%3Cpath d=%22M0 0l62 38-62 42z%22/%3E%3C/g%3E%3Cg stroke-width=%227%22 transform=%22translate(418 450) rotate(-3) scale(1.1)%22%3E%3Cpath d=%22M0 0v92M58 0v92M0 46h58%22/%3E%3C/g%3E%3Cg stroke-width=%226%22 transform=%22translate(612 512) rotate(18)%22%3E%3Cpath d=%22M0 0v92M0 0h58v92%22/%3E%3C/g%3E%3Cg stroke-width=%227%22 transform=%22translate(802 452) rotate(-18) scale(1.15)%22%3E%3Cpath d=%22M64 0L0 45l64 45%22/%3E%3C/g%3E%3Cg stroke-width=%226%22 transform=%22translate(1002 488) rotate(6)%22%3E%3Cpath d=%22M0 0v92M0 44l70-36M0 44l70 36%22/%3E%3C/g%3E%3Cg stroke-width=%227%22 transform=%22translate(1182 448) rotate(-10) scale(1.1)%22%3E%3Cpath d=%22M0 0l70 88M70 0L0 88%22/%3E%3C/g%3E%3C/g%3E%3C/svg%3E');
        background-size: cover;
        background-position: center;
        box-shadow: none;
        opacity: 0.78;
        filter: drop-shadow(0 0 18px rgba(255, 255, 255, 0.95))
          drop-shadow(0 0 42px rgba(143, 216, 255, 0.82));
        animation: norse-runes-glow 4.8s ease-in-out infinite;
      }

      body::after {
        width: 8px;
        height: 8px;
        background: var(--glitch-main);
        box-shadow:
          8vw 18vh 0 var(--glitch-main),
          21vw 64vh 0 var(--glitch-soft),
          36vw 32vh 0 var(--glitch-main),
          52vw 74vh 0 rgba(255, 245, 245, 0.42),
          68vw 23vh 0 var(--glitch-main),
          81vw 57vh 0 var(--glitch-soft),
          93vw 39vh 0 var(--glitch-main);
        opacity: 0;
        animation: site-glitch-blocks 4.2s infinite steps(1, end);
        transition: background-color var(--theme-duration) var(--theme-ease),
          box-shadow var(--theme-duration) var(--theme-ease);
      }

      .container-fluid {
        position: relative;
        z-index: 1;
        box-sizing: border-box;
        min-height: 100vh;
        padding: 32px 18px;
        display: flex;
        align-items: center;
        justify-content: center;
      }

      .theme-switch {
        position: fixed;
        top: 18px;
        right: 18px;
        z-index: 3;
        display: flex;
        gap: 6px;
        padding: 6px;
        border: 1px solid rgba(255, 43, 52, 0.36);
        border-radius: 8px;
        background: rgba(10, 1, 2, 0.78);
        box-shadow: 0 14px 36px rgba(0, 0, 0, 0.32);
        transition: border-color var(--theme-duration) var(--theme-ease),
          background-color var(--theme-duration) var(--theme-ease),
          box-shadow var(--theme-duration) var(--theme-ease);
      }

      body.heaven-theme .theme-switch {
        border-color: rgba(125, 203, 235, 0.6);
        background: rgba(255, 255, 255, 0.76);
        box-shadow: 0 14px 36px rgba(120, 154, 174, 0.24);
      }

      .theme-button {
        min-width: 58px;
        height: 36px;
        border: 1px solid transparent;
        border-radius: 8px;
        background: transparent;
        color: var(--muted);
        font-size: 14px;
        font-weight: 800;
        transition: border-color var(--theme-duration) var(--theme-ease),
          background var(--theme-duration) var(--theme-ease),
          color var(--theme-duration) var(--theme-ease),
          box-shadow var(--theme-duration) var(--theme-ease);
      }

      .theme-button.active {
        border-color: rgba(255, 43, 52, 0.42);
        background: #e01824;
        color: #ffffff;
        box-shadow: 0 0 18px rgba(224, 24, 36, 0.26);
      }

      body.heaven-theme .theme-button.active {
        border-color: rgba(217, 165, 46, 0.58);
        background: linear-gradient(135deg, #fff4bc, #83d5ef);
        color: #1f2a34;
        box-shadow: 0 0 22px rgba(217, 165, 46, 0.3);
      }

      .github-rate-card {
        position: fixed;
        top: 72px;
        right: 18px;
        z-index: 3;
        width: min(236px, calc(100vw - 36px));
        padding: 9px 10px 10px;
        border: 1px solid rgba(255, 43, 52, 0.34);
        border-radius: 8px;
        background: rgba(10, 1, 2, 0.78);
        box-shadow: 0 14px 36px rgba(0, 0, 0, 0.28);
        color: var(--ink);
        transition: border-color var(--theme-duration) var(--theme-ease),
          background-color var(--theme-duration) var(--theme-ease),
          box-shadow var(--theme-duration) var(--theme-ease);
      }

      body.heaven-theme .github-rate-card {
        border-color: rgba(125, 203, 235, 0.6);
        background: rgba(255, 255, 255, 0.76);
        box-shadow: 0 14px 36px rgba(120, 154, 174, 0.24);
      }

      .github-rate-top {
        display: flex;
        align-items: baseline;
        justify-content: space-between;
        gap: 8px;
        margin-bottom: 7px;
      }

      .github-rate-title {
        font-size: 12px;
        font-weight: 900;
        color: var(--muted);
      }

      .github-rate-value {
        font-size: 13px;
        font-weight: 900;
        color: var(--ink);
        white-space: nowrap;
      }

      .github-rate-track {
        position: relative;
        overflow: hidden;
        height: 8px;
        border-radius: 8px;
        background: rgba(255, 255, 255, 0.1);
        border: 1px solid rgba(255, 255, 255, 0.1);
      }

      .github-rate-fill {
        width: var(--github-rate-percent, 0%);
        height: 100%;
        border-radius: inherit;
        background: linear-gradient(90deg, #e01824, #f3b64d);
        transition: width 280ms ease;
      }

      body.heaven-theme .github-rate-fill {
        background: linear-gradient(90deg, #83d5ef, #d9a52e);
      }

      .github-rate-meta {
        margin-top: 6px;
        color: var(--muted);
        font-size: 11px;
        line-height: 1.25;
        white-space: nowrap;
        overflow: hidden;
        text-overflow: ellipsis;
      }

      .account-menu {
        position: fixed;
        top: 18px;
        left: 18px;
        z-index: 3;
        display: grid;
        gap: 8px;
        width: 220px;
      }

      .account-menu-trigger {
        justify-self: start;
        min-width: 96px;
        padding: 8px 10px;
        border: 1px solid rgba(255, 43, 52, 0.36);
        border-radius: 8px;
        background: rgba(10, 1, 2, 0.88);
        color: var(--ink);
        font-size: 14px;
        font-weight: 900;
        text-align: center;
        box-shadow: 0 12px 28px rgba(0, 0, 0, 0.28);
        transition: border-color var(--theme-duration) var(--theme-ease),
          background-color var(--theme-duration) var(--theme-ease),
          color var(--theme-duration) var(--theme-ease),
          box-shadow var(--theme-duration) var(--theme-ease);
      }

      .account-menu-panel {
        display: grid;
        gap: 10px;
        padding: 12px;
        border: 1px solid rgba(255, 43, 52, 0.36);
        border-radius: 8px;
        background: rgba(10, 1, 2, 0.88);
        box-shadow: 0 18px 42px rgba(0, 0, 0, 0.34);
        opacity: 0;
        pointer-events: none;
        transform: translateY(-8px);
        transition: opacity 160ms ease,
          transform 160ms ease,
          border-color var(--theme-duration) var(--theme-ease),
          background-color var(--theme-duration) var(--theme-ease),
          box-shadow var(--theme-duration) var(--theme-ease);
      }

      .account-menu.account-menu-open .account-menu-panel {
        opacity: 1;
        pointer-events: auto;
        transform: translateY(0);
      }

      body.heaven-theme .account-menu-trigger,
      body.heaven-theme .account-menu-panel {
        border-color: rgba(125, 203, 235, 0.6);
        background: rgba(255, 255, 255, 0.84);
        box-shadow: 0 18px 42px rgba(120, 154, 174, 0.2);
      }

      .home {
        width: min(100%, 520px);
        margin-right: auto;
        margin-left: auto;
        text-align: center;
      }

      .page-wide,
      .account-page {
        width: min(100%, 760px);
      }

      .logo-wrap {
        position: relative;
        width: clamp(118px, 18vw, 178px);
        aspect-ratio: 1;
        margin: 0 auto 16px;
        filter: drop-shadow(7px 7px 0 rgba(143, 0, 8, 0.72))
          drop-shadow(-5px 4px 0 rgba(255, 59, 31, 0.32));
        transition: filter var(--theme-duration) var(--theme-ease);
      }

      .logo-wrap::before,
      .logo-wrap::after {
        content: '';
        position: absolute;
        inset: 0;
        background: url('githound-hell-logo.png') center / cover no-repeat;
        mix-blend-mode: screen;
        pointer-events: none;
        transition: opacity var(--theme-duration) var(--theme-ease),
          filter var(--theme-duration) var(--theme-ease),
          background-image var(--theme-duration) var(--theme-ease);
      }

      .logo-wrap::before {
        transform: translate(5px, 0);
        opacity: 0.3;
        filter: saturate(1.8) hue-rotate(330deg);
        animation: logo-glitch-a 2.7s infinite steps(1, end);
      }

      .logo-wrap::after {
        transform: translate(-5px, 0);
        opacity: 0.24;
        filter: contrast(1.5) sepia(1) hue-rotate(315deg);
        animation: logo-glitch-b 3.1s infinite steps(1, end);
      }

      body.heaven-theme .logo-wrap {
        filter: drop-shadow(7px 7px 0 rgba(125, 203, 235, 0.28))
          drop-shadow(-5px 4px 0 rgba(255, 229, 153, 0.68));
      }

      body.heaven-theme .logo-wrap::before,
      body.heaven-theme .logo-wrap::after {
        background-image: url('githound-heaven-logo.png');
        opacity: 0.12;
        filter: saturate(1.18);
      }

      body.myth-norse .logo-wrap {
        filter: drop-shadow(7px 7px 0 rgba(20, 0, 5, 0.75))
          drop-shadow(-5px 4px 0 rgba(86, 211, 255, 0.38));
      }

      body.myth-norse .logo-wrap::before,
      body.myth-norse .logo-wrap::after,
      body.myth-norse.heaven-theme .logo-wrap::before,
      body.myth-norse.heaven-theme .logo-wrap::after {
        background-image: url('githound-norse-logo.png');
        opacity: 0.18;
        filter: saturate(1.25) contrast(1.08);
      }

      body.myth-norse.heaven-theme .logo-wrap::before,
      body.myth-norse.heaven-theme .logo-wrap::after {
        background-image: url('githound-norse-heaven-logo.png');
      }

      .logo {
        position: absolute;
        inset: 0;
        z-index: 1;
        display: block;
        width: 100%;
        height: 100%;
        object-fit: cover;
        border: 2px solid rgba(255, 43, 52, 0.5);
        border-radius: 8px;
        box-shadow: 0 0 0 1px rgba(255, 43, 52, 0.42),
          0 18px 70px rgba(224, 24, 36, 0.28);
        transition: opacity var(--theme-duration) var(--theme-ease),
          border-color var(--theme-duration) var(--theme-ease),
          box-shadow var(--theme-duration) var(--theme-ease);
      }

      .heaven-logo {
        display: none;
        opacity: 0;
      }

      .norse-logo {
        display: none;
        opacity: 0;
      }

      .norse-heaven-logo {
        display: none;
        opacity: 0;
      }

      body:not(.heaven-theme) .hell-logo {
        display: block;
        opacity: 1;
      }

      body:not(.heaven-theme) .heaven-logo {
        display: none;
        opacity: 0;
      }

      body.heaven-theme .hell-logo {
        display: none;
        opacity: 0;
      }

      body.heaven-theme .heaven-logo {
        display: block;
        opacity: 1;
      }

      body.myth-norse .hell-logo,
      body.myth-norse .heaven-logo,
      body.myth-norse .norse-heaven-logo,
      body.myth-norse.heaven-theme .hell-logo,
      body.myth-norse.heaven-theme .heaven-logo,
      body.myth-norse.heaven-theme .norse-logo {
        display: none;
        opacity: 0;
      }

      body.myth-norse .norse-logo {
        display: block;
        opacity: 1;
      }

      body.myth-norse.heaven-theme .norse-heaven-logo {
        display: block;
        opacity: 1;
      }

      body.heaven-theme .logo {
        border-color: rgba(125, 203, 235, 0.62);
        box-shadow: 0 0 0 1px rgba(217, 165, 46, 0.36),
          0 20px 70px rgba(125, 203, 235, 0.26);
      }

      .project-title {
        position: relative;
        z-index: 1;
        margin: 0 0 24px;
        font-family: Impact, 'Arial Black', sans-serif;
        font-size: clamp(36px, 6.4vw, 56px);
        line-height: 0.96;
        font-weight: 900;
        letter-spacing: 1px;
        text-transform: uppercase;
        color: var(--ink);
        text-shadow: 0 0 20px rgba(255, 43, 52, 0.62), 0 3px 0 var(--blood);
        transition: color var(--theme-duration) var(--theme-ease),
          text-shadow var(--theme-duration) var(--theme-ease);
      }

      body.heaven-theme .project-title {
        text-shadow: 0 0 28px rgba(255, 214, 107, 0.72), 0 3px 0 rgba(255, 255, 255, 0.9);
      }

      .form {
        display: grid;
        gap: 18px;
        padding: 22px;
        border: 1px solid rgba(255, 43, 52, 0.36);
        border-radius: 8px;
        background: var(--panel);
        box-shadow: 0 22px 70px rgba(0, 0, 0, 0.55),
          inset 0 0 34px rgba(224, 24, 36, 0.08);
        text-align: left;
        transition: border-color var(--theme-duration) var(--theme-ease),
          background-color var(--theme-duration) var(--theme-ease),
          box-shadow var(--theme-duration) var(--theme-ease);
      }

      body.heaven-theme .form {
        border-color: rgba(125, 203, 235, 0.52);
        box-shadow: 0 22px 70px rgba(120, 154, 174, 0.18),
          inset 0 0 34px rgba(217, 165, 46, 0.12);
      }

      .menu-form {
        text-align: center;
      }

      .login-form {
        gap: 16px;
      }

      .social-login-block {
        display: grid;
        gap: 10px;
        justify-items: center;
        padding: 4px 0 2px;
      }

      .social-login-title {
        color: var(--muted);
        font-size: 15px;
        font-weight: 800;
        text-align: center;
        transition: color var(--theme-duration) var(--theme-ease);
      }

      .social-login-row {
        display: flex;
        flex-wrap: wrap;
        justify-content: center;
        gap: 10px;
        width: 100%;
      }

      .social-login-button {
        display: inline-flex;
        align-items: center;
        justify-content: center;
        gap: 8px;
        width: 56px;
        height: 56px;
        min-height: 56px;
        padding: 0;
        border: 1px solid var(--line);
        border-radius: 8px;
        background: rgba(8, 1, 2, 0.76);
        color: var(--ink);
        box-shadow: 4px 4px 0 var(--button-shadow), 0 0 22px rgba(224, 24, 36, 0.18);
        transition: border-color var(--theme-duration) var(--theme-ease),
          background-color var(--theme-duration) var(--theme-ease),
          color var(--theme-duration) var(--theme-ease),
          box-shadow var(--theme-duration) var(--theme-ease),
          transform 140ms ease;
      }

      .social-login-button:hover,
      .social-login-button:focus {
        color: var(--button-ink);
        border-color: var(--accent-hover);
        background: rgba(224, 24, 36, 0.18);
        transform: translateY(-1px);
      }

      .social-login-icon {
        width: 26px;
        height: 26px;
      }

      .social-login-text {
        position: absolute;
        width: 1px;
        height: 1px;
        overflow: hidden;
        clip: rect(0 0 0 0);
        white-space: nowrap;
      }

      .login-divider {
        display: grid;
        grid-template-columns: 1fr auto 1fr;
        gap: 10px;
        align-items: center;
        color: var(--muted);
        font-size: 14px;
        font-weight: 800;
        text-align: center;
        transition: color var(--theme-duration) var(--theme-ease);
      }

      .login-divider::before,
      .login-divider::after {
        content: '';
        height: 1px;
        background: var(--line);
        opacity: 0.72;
        transition: background-color var(--theme-duration) var(--theme-ease);
      }

      body.heaven-theme .social-login-button {
        background: rgba(255, 255, 255, 0.9);
        border-color: #8ecce3;
        color: #1f2a34;
        box-shadow: 4px 4px 0 rgba(120, 154, 174, 0.34),
          0 0 22px rgba(217, 165, 46, 0.16);
      }

      body.heaven-theme .social-login-button:hover,
      body.heaven-theme .social-login-button:focus {
        background: #ffffff;
        border-color: #d9a52e;
        color: #1f2a34;
      }

      .section-title {
        margin: 0;
        color: var(--ink);
        font-size: 26px;
        font-weight: 900;
        transition: color var(--theme-duration) var(--theme-ease);
      }

      .account-copy {
        margin: 6px 0 0;
        color: var(--muted);
        transition: color var(--theme-duration) var(--theme-ease);
      }

      .myth-style-picker {
        display: grid;
        gap: 8px;
        width: 100%;
        margin-top: 8px;
      }

      .myth-style-title {
        color: var(--muted);
        font-size: 14px;
        font-weight: 900;
        text-align: center;
        transition: color var(--theme-duration) var(--theme-ease);
      }

      .myth-style-buttons {
        display: grid;
        grid-template-columns: repeat(3, minmax(0, 1fr));
        gap: 8px;
      }

      .myth-style-button {
        min-height: 40px;
        border: 1px solid var(--line);
        border-radius: 8px;
        background: var(--secondary-bg);
        color: var(--secondary-ink);
        font-size: 13px;
        font-weight: 900;
        box-shadow: 3px 3px 0 var(--button-shadow);
        transition: border-color var(--theme-duration) var(--theme-ease),
          background-color var(--theme-duration) var(--theme-ease),
          color var(--theme-duration) var(--theme-ease),
          box-shadow var(--theme-duration) var(--theme-ease),
          opacity var(--theme-duration) var(--theme-ease);
      }

      .myth-style-button.active {
        border-color: var(--accent);
        background: var(--accent);
        color: var(--button-ink);
        box-shadow: 0 0 22px rgba(224, 24, 36, 0.24);
      }

      .myth-style-button.disabled {
        cursor: not-allowed;
        opacity: 0.44;
      }

      .protocol-page {
        width: min(100%, 1080px);
        max-width: 1080px;
      }

      .protocol-form {
        text-align: center;
      }

      .protocol-grid {
        display: grid;
        grid-template-columns: repeat(3, minmax(220px, 1fr));
        gap: 18px;
        width: 100%;
        justify-content: center;
      }

      .protocol-card {
        display: grid;
        gap: 12px;
        align-items: start;
        justify-items: center;
        grid-template-areas:
          'image'
          'button';
      }

      .protocol-image {
        grid-area: image;
        width: min(100%, 340px);
        aspect-ratio: 1;
        object-fit: cover;
        border: 2px solid var(--line);
        border-radius: 8px;
        box-shadow: 0 16px 42px rgba(0, 0, 0, 0.34);
        pointer-events: none;
        transition: opacity var(--theme-duration) var(--theme-ease),
          border-color var(--theme-duration) var(--theme-ease),
          box-shadow var(--theme-duration) var(--theme-ease);
      }

      .protocol-card .run-button {
        grid-area: button;
        width: min(100%, 340px);
      }

      .protocol-heaven-image {
        display: none;
      }

      .protocol-norse-image,
      .protocol-label-norse {
        display: none;
      }

      body.heaven-theme .protocol-hell-image {
        display: none;
      }

      body.heaven-theme .protocol-heaven-image {
        display: block;
      }

      body.myth-norse .protocol-egypt-image,
      body.myth-norse .protocol-label-egypt {
        display: none;
      }

      body.myth-norse .protocol-norse-image {
        display: none;
      }

      body.myth-norse .protocol-label-norse {
        display: inline;
      }

      body.myth-norse:not(.heaven-theme) .protocol-norse-hell-image,
      body.myth-norse.heaven-theme .protocol-norse-heaven-image {
        display: block;
      }

      .set-loading-page,
      .set-report-page {
        width: min(100%, 920px);
        max-width: 920px;
      }

      .archive-page {
        position: relative;
        width: min(100%, 920px);
        max-width: 920px;
      }

      .set-loading-form,
      .set-report-form,
      .archive-form {
        text-align: left;
      }

      .archive-actions {
        display: grid;
        grid-template-columns: repeat(2, minmax(0, 1fr));
        gap: 10px;
      }

      .archive-table-wrap {
        width: 100%;
        overflow-x: auto;
        border: 1px solid var(--line);
        border-radius: 8px;
        background: rgba(8, 1, 2, 0.55);
      }

      body.heaven-theme .archive-table-wrap {
        border-color: rgba(125, 203, 235, 0.72);
        background: rgba(255, 255, 255, 0.86);
        box-shadow: 0 18px 36px rgba(120, 154, 174, 0.14);
      }

      .archive-table {
        width: 100%;
        min-width: 780px;
        border-collapse: collapse;
        color: var(--ink);
        table-layout: fixed;
      }

      .archive-table th,
      .archive-table td {
        padding: 12px 10px;
        border-bottom: 1px solid rgba(224, 24, 36, 0.58);
        border-right: 1px solid rgba(224, 24, 36, 0.42);
        text-align: left;
        vertical-align: middle;
        overflow-wrap: anywhere;
      }

      body.heaven-theme .archive-table th,
      body.heaven-theme .archive-table td {
        border-bottom-color: rgba(125, 203, 235, 0.52);
        border-right-color: rgba(125, 203, 235, 0.42);
        background: rgba(246, 251, 255, 0.86);
        color: #1f3545;
      }

      .archive-table tr:last-child td {
        border-bottom: 0;
      }

      .archive-table th {
        color: var(--accent-hover);
        font-size: 14px;
        letter-spacing: 0;
      }

      body.heaven-theme .archive-table th {
        background: linear-gradient(180deg, rgba(255, 245, 200, 0.95), rgba(235, 248, 255, 0.95));
        color: #b88408;
      }

      .archive-check-cell {
        width: 48px;
        text-align: center !important;
      }

      .archive-row-select {
        width: 18px;
        height: 18px;
        accent-color: var(--accent);
      }

      .archive-open-btn {
        width: 100%;
        min-height: 38px;
        border: 1px solid var(--line);
        border-radius: 8px;
        background: var(--secondary-bg);
        color: var(--secondary-ink);
        font-weight: 800;
      }

      .archive-open-btn:hover {
        border-color: var(--accent-hover);
        color: var(--accent-hover);
      }

      body.heaven-theme .archive-open-btn {
        border-color: rgba(125, 203, 235, 0.88);
        background: rgba(237, 249, 255, 0.98);
        color: #1d6283;
        box-shadow: 4px 4px 0 rgba(120, 154, 174, 0.24);
      }

      body.heaven-theme .archive-open-btn:hover {
        border-color: #d1a036;
        color: #b88408;
      }

      .archive-pager {
        display: grid;
        grid-template-columns: minmax(0, 130px) 1fr minmax(0, 130px);
        align-items: center;
        gap: 10px;
        margin-top: 12px;
      }

      .archive-page-label {
        color: var(--muted);
        font-weight: 800;
        text-align: center;
      }

      .set-progress-wrap {
        display: grid;
        gap: 10px;
        opacity: 1 !important;
      }

      body .set-loading-page,
      body .set-loading-page *,
      body .set-progress-wrap,
      body .set-progress-wrap * {
        opacity: 1 !important;
      }

      #page.recalculating,
      #page.shiny-bound-output.recalculating,
      .set-loading-page.recalculating,
      .set-progress-wrap.recalculating {
        opacity: 1 !important;
      }

      .set-progress-label {
        display: grid;
        gap: 2px;
        color: var(--muted);
        font-size: 14px;
        font-weight: 700;
        line-height: 1.45;
        max-width: 100%;
        overflow-wrap: anywhere;
        transition: color var(--theme-duration) var(--theme-ease);
      }

      .set-progress-working {
        color: rgba(255, 216, 160, 0.68);
        font-size: 12px;
        font-style: italic;
        white-space: pre-line;
      }

      body.heaven-theme .set-progress-working {
        color: rgba(67, 96, 115, 0.64);
      }

      .set-progress-track {
        position: relative;
        height: 34px;
        border: 2px solid rgba(255, 211, 90, 1);
        border-radius: 999px;
        background: rgba(18, 3, 3, 0.9);
        overflow: visible;
        box-shadow: inset 0 0 16px rgba(0, 0, 0, 0.55),
          0 0 28px rgba(255, 211, 90, 0.58);
        opacity: 1 !important;
        transition: border-color var(--theme-duration) var(--theme-ease),
          background-color var(--theme-duration) var(--theme-ease),
          box-shadow var(--theme-duration) var(--theme-ease);
      }

      body.heaven-theme .set-progress-track {
        border-color: rgba(196, 135, 0, 1);
        background: rgba(255, 250, 224, 0.96);
        box-shadow: inset 0 0 14px rgba(196, 135, 0, 0.36),
          0 0 30px rgba(217, 165, 46, 0.52);
      }

      body.myth-norse .set-progress-track {
        border-color: rgba(173, 239, 255, 1);
        background: rgba(5, 18, 31, 0.84);
        box-shadow: inset 0 0 18px rgba(0, 18, 35, 0.72),
          0 0 30px rgba(162, 232, 255, 0.58),
          0 0 54px rgba(75, 168, 225, 0.28);
      }

      body.myth-norse.heaven-theme .set-progress-track {
        border-color: rgba(222, 250, 255, 1);
        background: rgba(241, 252, 255, 0.9);
        box-shadow: inset 0 0 18px rgba(98, 174, 216, 0.32),
          0 0 36px rgba(220, 251, 255, 0.72),
          0 0 64px rgba(120, 210, 255, 0.44);
      }

      .set-progress-fill {
        position: absolute;
        inset: 7px auto 7px 7px;
        width: var(--set-progress, 0%);
        max-width: calc(100% - 14px);
        border-radius: 999px;
        background: linear-gradient(90deg, #d48200, #ffd000 36%, #fff4a8 62%, #ffb000);
        box-shadow: 0 0 28px rgba(255, 211, 90, 0.95),
          0 0 56px rgba(255, 184, 31, 0.58);
        overflow: hidden;
        opacity: 1 !important;
        transition: width 620ms var(--theme-ease);
      }

      body.heaven-theme .set-progress-fill {
        background: linear-gradient(90deg, #b87900, #ffca1f 38%, #fff3a4 66%, #d6a000);
        box-shadow: 0 0 24px rgba(217, 165, 46, 0.98),
          0 0 52px rgba(125, 203, 235, 0.42);
      }

      body.myth-norse .set-progress-fill {
        background: linear-gradient(90deg, #57c7ff, #dffaff 38%, #ffffff 58%, #78d8ff);
        box-shadow: 0 0 26px rgba(203, 246, 255, 0.96),
          0 0 58px rgba(79, 187, 255, 0.64);
      }

      body.myth-norse.heaven-theme .set-progress-fill {
        background: linear-gradient(90deg, #8de8ff, #ffffff 45%, #c9f7ff 70%, #58bce8);
        box-shadow: 0 0 30px rgba(255, 255, 255, 0.98),
          0 0 62px rgba(128, 221, 255, 0.72);
      }

      .set-progress-fill::after {
        content: '';
        position: absolute;
        inset: 0;
        background: linear-gradient(90deg, transparent, rgba(255, 255, 255, 0.72), transparent);
        transform: translateX(-100%);
        animation: progress-shine 1.45s infinite;
      }

      .mini-horus {
        position: absolute;
        top: 50%;
        left: max(8px, calc(var(--set-progress, 0%) - 28px));
        width: 64px;
        height: auto;
        transform: translateY(-50%);
        filter: drop-shadow(0 6px 12px rgba(0, 0, 0, 0.55))
          drop-shadow(0 0 12px rgba(255, 211, 90, 0.7));
        opacity: 1 !important;
        transition: left 620ms var(--theme-ease);
      }

      .mini-mjolnir {
        position: absolute;
        top: 50%;
        left: max(8px, calc(var(--set-progress, 0%) - 28px));
        width: 62px;
        height: auto;
        transform: translateY(-50%);
        filter: drop-shadow(0 6px 12px rgba(0, 18, 35, 0.58))
          drop-shadow(0 0 14px rgba(210, 250, 255, 0.86));
        opacity: 1 !important;
        transition: left 620ms var(--theme-ease);
        display: none;
      }

      body.myth-norse .mini-horus {
        display: none;
      }

      body.myth-norse .mini-mjolnir {
        display: block;
      }

      .set-progress-percent {
        color: var(--ink);
        font-size: 18px;
        font-weight: 900;
        text-align: right;
        transition: color var(--theme-duration) var(--theme-ease);
      }

      .set-report-section {
        display: grid;
        gap: 10px;
        padding-top: 16px;
        border-top: 1px solid rgba(255, 43, 52, 0.28);
      }

      .set-title-page {
        position: relative;
        display: grid;
        gap: 18px;
        justify-items: center;
        min-height: 560px;
        padding: 48px 28px;
        border: 2px solid var(--line);
        border-radius: 8px;
        overflow: hidden;
        text-align: center;
        background:
          radial-gradient(circle at 50% 18%, rgba(255, 43, 52, 0.22), transparent 26%),
          linear-gradient(180deg, rgba(36, 2, 4, 0.96), rgba(8, 0, 1, 0.98));
        box-shadow: inset 0 0 80px rgba(224, 24, 36, 0.18),
          0 20px 50px rgba(0, 0, 0, 0.26);
        transition: border-color var(--theme-duration) var(--theme-ease),
          background var(--theme-duration) var(--theme-ease),
          box-shadow var(--theme-duration) var(--theme-ease);
      }

      .set-title-page::before {
        content: '';
        position: absolute;
        inset: 18px;
        border: 1px solid rgba(255, 211, 90, 0.42);
        border-radius: 8px;
        pointer-events: none;
        z-index: 2;
        transition: border-color var(--theme-duration) var(--theme-ease);
      }

      .set-title-page::after {
        content: '';
        position: absolute;
        inset: 0;
        background:
          radial-gradient(circle at 50% 16%, rgba(255, 235, 160, 0.72), transparent 28%),
          linear-gradient(180deg, rgba(255, 255, 255, 0.96), rgba(239, 250, 255, 0.96));
        opacity: 0;
        pointer-events: none;
        transition: opacity var(--theme-duration) var(--theme-ease);
      }

      .set-title-page > * {
        position: relative;
        z-index: 1;
      }

      body.heaven-theme .set-title-page,
      .set-title-heaven {
        box-shadow: inset 0 0 80px rgba(217, 165, 46, 0.16),
          0 20px 50px rgba(120, 154, 174, 0.2);
      }

      body.heaven-theme .set-title-page::after,
      .set-title-heaven::after {
        opacity: 1;
      }

      .set-title-brand {
        display: grid;
        justify-items: center;
        gap: 8px;
      }

      .set-title-logo {
        width: 112px;
        aspect-ratio: 1;
        object-fit: cover;
        border: 1px solid var(--line);
        border-radius: 8px;
        box-shadow: 0 0 22px rgba(224, 24, 36, 0.2);
        transition: border-color var(--theme-duration) var(--theme-ease),
          box-shadow var(--theme-duration) var(--theme-ease);
      }

      body.heaven-theme .set-title-logo,
      .set-title-heaven .set-title-logo {
        box-shadow: 0 0 22px rgba(217, 165, 46, 0.24);
      }

      .set-title-project {
        color: var(--accent);
        font-family: Impact, Haettenschweiler, 'Arial Black', sans-serif;
        font-size: 34px;
        font-weight: 900;
        line-height: 1;
        text-transform: uppercase;
        transition: color var(--theme-duration) var(--theme-ease);
      }

      .set-title-main {
        max-width: 760px;
        margin: 0;
        color: var(--ink);
        font-family: Impact, Haettenschweiler, 'Arial Black', sans-serif;
        font-size: clamp(42px, 7vw, 78px);
        line-height: 0.95;
        letter-spacing: 0;
        text-transform: uppercase;
        text-shadow: 0 0 24px rgba(255, 43, 52, 0.42);
        transition: color var(--theme-duration) var(--theme-ease),
          text-shadow var(--theme-duration) var(--theme-ease);
      }

      body.heaven-theme .set-title-main,
      .set-title-heaven .set-title-main {
        text-shadow: 0 0 24px rgba(217, 165, 46, 0.46);
      }

      .set-title-target {
        color: var(--ink);
        font-size: 24px;
        font-weight: 900;
        transition: color var(--theme-duration) var(--theme-ease);
      }

      .set-title-oath {
        max-width: 660px;
        margin: 0;
        color: var(--muted);
        font-size: 16px;
        line-height: 1.55;
        transition: color var(--theme-duration) var(--theme-ease);
      }

      .divine-seals {
        display: grid;
        grid-template-columns: repeat(3, minmax(0, 1fr));
        gap: 18px;
        width: min(100%, 680px);
        margin-top: 10px;
      }

      .divine-seal {
        display: grid;
        place-items: center;
        gap: 8px;
        min-width: 0;
        padding: 0;
        border: 0;
        border-radius: 8px;
        color: var(--ink);
        background: transparent;
        transition: color var(--theme-duration) var(--theme-ease);
      }

      body.heaven-theme .divine-seal,
      .set-title-heaven .divine-seal {
        background: transparent;
      }

      .divine-seal-image {
        box-sizing: border-box;
        width: min(100%, 196px);
        aspect-ratio: 1;
        padding: 0;
        border: 0;
        border-radius: 50%;
        object-fit: cover;
        background: rgba(6, 1, 1, 0.62);
        filter: drop-shadow(0 10px 18px rgba(0, 0, 0, 0.36));
        transition: background-color var(--theme-duration) var(--theme-ease),
          filter var(--theme-duration) var(--theme-ease);
      }

      body.heaven-theme .divine-seal-image,
      .set-title-heaven .divine-seal-image {
        background: rgba(255, 255, 255, 0.62);
      }

      .archive-thoth {
        position: fixed;
        right: 18px;
        bottom: 0;
        z-index: 1;
        width: min(28vw, 270px);
        opacity: 0.2;
        pointer-events: none;
        mix-blend-mode: screen;
      }

      .archive-thoth-img {
        display: block;
        width: 100%;
        height: auto;
        transform: scaleX(-1);
        filter: drop-shadow(0 18px 26px rgba(0, 0, 0, 0.45));
      }

      .archive-thoth-heaven {
        display: none;
      }

      .archive-raven-norse,
      .archive-raven-norse-heaven {
        display: none;
      }

      body.heaven-theme .archive-thoth {
        opacity: 0.62;
        mix-blend-mode: multiply;
      }

      body.heaven-theme .archive-thoth-hell {
        display: none;
      }

      body.heaven-theme .archive-thoth-heaven {
        display: block;
        -webkit-mask-image: radial-gradient(ellipse at center, #000 42%, rgba(0, 0, 0, 0.72) 62%, transparent 86%);
        mask-image: radial-gradient(ellipse at center, #000 42%, rgba(0, 0, 0, 0.72) 62%, transparent 86%);
      }

      body.heaven-theme .archive-thoth-img {
        filter: saturate(0.9) contrast(0.92) brightness(1.03)
          drop-shadow(0 16px 22px rgba(120, 154, 174, 0.12));
      }

      body.myth-norse .archive-thoth {
        width: min(30vw, 300px);
      }

      body.myth-norse .archive-thoth-hell,
      body.myth-norse .archive-thoth-heaven {
        display: none;
      }

      body.myth-norse:not(.heaven-theme) .archive-thoth {
        opacity: 0.34;
        mix-blend-mode: screen;
      }

      body.myth-norse:not(.heaven-theme) .archive-raven-norse {
        display: block;
      }

      body.myth-norse.heaven-theme .archive-thoth {
        opacity: 0.58;
        mix-blend-mode: multiply;
      }

      body.myth-norse.heaven-theme .archive-raven-norse-heaven {
        display: block;
        -webkit-mask-image: radial-gradient(ellipse at center, #000 48%, rgba(0, 0, 0, 0.74) 70%, transparent 92%);
        mask-image: radial-gradient(ellipse at center, #000 48%, rgba(0, 0, 0, 0.74) 70%, transparent 92%);
      }

      body.myth-norse .archive-raven-norse,
      body.myth-norse .archive-raven-norse-heaven {
        filter: drop-shadow(0 18px 28px rgba(0, 0, 0, 0.36));
      }

      .divine-seal span {
        font-size: 21px;
        font-weight: 900;
      }

      .divine-seal small {
        color: var(--muted);
        font-size: 11px;
        font-weight: 800;
        line-height: 1.25;
        transition: color var(--theme-duration) var(--theme-ease);
      }

      .seal-isis {
        border-color: #f0c34f;
      }

      .seal-set {
        border-color: #e01824;
      }

      .seal-anubis {
        border-color: #8ecce3;
      }

      .set-title-date {
        color: var(--muted);
        font-size: 14px;
        font-weight: 800;
        transition: color var(--theme-duration) var(--theme-ease);
      }

      body.heaven-theme .set-report-section {
        border-top-color: rgba(125, 203, 235, 0.48);
      }

      .set-report-title {
        margin: 0;
        color: var(--ink);
        font-size: 20px;
        font-weight: 900;
        transition: color var(--theme-duration) var(--theme-ease);
      }

      .set-report-text,
      .set-empty {
        margin: 0;
        color: var(--muted);
        font-size: 14px;
        line-height: 1.45;
        transition: color var(--theme-duration) var(--theme-ease);
      }

      .set-report-table-wrap {
        width: 100%;
        overflow-x: auto;
        border: 1px solid var(--line);
        border-radius: 8px;
        transition: border-color var(--theme-duration) var(--theme-ease);
      }

      .set-report-table {
        width: 100%;
        min-width: 640px;
        border-collapse: collapse;
        table-layout: fixed;
        font-size: 14px;
      }

      .set-report-table th,
      .set-report-table td {
        padding: 8px;
        border: 1px solid var(--line);
        color: var(--ink);
        vertical-align: top;
        white-space: normal;
        word-break: normal;
        overflow-wrap: break-word;
        transition: border-color var(--theme-duration) var(--theme-ease),
          color var(--theme-duration) var(--theme-ease);
      }

      .set-report-table th {
        color: var(--accent);
        font-weight: 900;
        white-space: normal;
      }

      .set-sort-button {
        display: inline-flex;
        align-items: flex-start;
        justify-content: space-between;
        gap: 6px;
        width: 100%;
        min-height: 100%;
        padding: 0;
        border: 0;
        background: transparent;
        color: inherit;
        font: inherit;
        line-height: 1.25;
        text-align: left;
        cursor: pointer;
      }

      .set-sort-button:hover,
      .set-sort-button:focus {
        color: var(--accent-hover);
        outline: none;
      }

      .set-sort-indicator {
        flex: 0 0 auto;
        color: var(--muted);
        font-size: 12px;
        line-height: 1.2;
      }

      .set-report-table th.sorted-asc,
      .set-report-table th.sorted-desc {
        background: rgba(224, 24, 36, 0.12);
      .epss-header-cell {
        display: inline-flex;
        align-items: center;
        gap: 6px;
      }

      .epss-fetch-all-button {
        display: inline-flex;
        align-items: center;
        justify-content: center;
        width: 24px;
        height: 24px;
        padding: 0;
        border: 1px solid var(--line);
        border-radius: 6px;
        color: var(--accent);
        background: rgba(224, 24, 36, 0.08);
        font-size: 14px;
        line-height: 1;
        cursor: pointer;
      }

      .epss-fetch-all-button:hover,
      .epss-fetch-all-button:focus {
        border-color: var(--accent);
        box-shadow: 0 0 0 2px rgba(224, 24, 36, 0.12);
      }

      .epss-fetch-all-button:disabled {
        cursor: wait;
        opacity: 0.65;
      }

      .set-markdown table {
        width: 100%;
        margin: 10px 0;
        border-collapse: collapse;
        font-size: 14px;
      }

      .set-markdown th,
      .set-markdown td {
        padding: 6px 8px;
        border: 1px solid var(--line);
        color: var(--ink);
        vertical-align: top;
        transition: border-color var(--theme-duration) var(--theme-ease),
          color var(--theme-duration) var(--theme-ease);
      }

      .set-markdown th {
        color: var(--accent);
        font-weight: 900;
      }

      .commit-date-cell {
        display: block;
        max-width: 100%;
        overflow-wrap: anywhere;
      }

      .description-cell {
        display: grid;
        gap: 6px;
        max-width: 100%;
        overflow-wrap: anywhere;
      }

      .copy-commit-button {
        display: inline-flex;
        align-items: center;
        justify-content: center;
        width: 34px;
        height: 22px;
        margin-left: 4px;
        padding: 0;
        border: 1px solid var(--line);
        border-radius: 6px;
        color: var(--accent);
        background: rgba(224, 24, 36, 0.08);
        font-size: 12px;
        font-weight: 900;
        line-height: 1;
        cursor: pointer;
        vertical-align: middle;
        transition: border-color var(--theme-duration) var(--theme-ease),
          background-color var(--theme-duration) var(--theme-ease),
          color var(--theme-duration) var(--theme-ease),
          box-shadow var(--theme-duration) var(--theme-ease);
      }

      .copy-commit-button:hover,
      .copy-commit-button:focus {
        border-color: var(--accent);
        box-shadow: 0 0 0 2px rgba(224, 24, 36, 0.12);
      }

      .copy-commit-button.copied {
        color: var(--ink);
        background: rgba(120, 214, 166, 0.16);
      }

      body.heaven-theme .copy-commit-button {
        background: rgba(211, 29, 29, 0.07);
      }

      .epss-cell {
        display: inline-flex;
        align-items: center;
        gap: 6px;
        min-width: 118px;
      }

      .epss-value {
        font-variant-numeric: tabular-nums;
        white-space: nowrap;
      }

      .epss-percentile {
        display: block;
        margin-top: 2px;
        color: var(--muted);
        font-size: 11px;
        line-height: 1.2;
      }

      .epss-fetch-button {
        display: inline-flex;
        align-items: center;
        justify-content: center;
        min-width: 48px;
        height: 24px;
        padding: 0 8px;
        border: 1px solid var(--line);
        border-radius: 6px;
        color: var(--accent);
        background: rgba(224, 24, 36, 0.08);
        font-size: 12px;
        font-weight: 900;
        cursor: pointer;
      }

      .epss-fetch-button:hover,
      .epss-fetch-button:focus {
        border-color: var(--accent);
        box-shadow: 0 0 0 2px rgba(224, 24, 36, 0.12);
      }

      .epss-fetch-button:disabled {
        cursor: wait;
        opacity: 0.65;
      }

      .full-text-button {
        justify-self: start;
        min-height: 24px;
        padding: 3px 8px;
        border: 1px solid var(--line);
        border-radius: 6px;
        color: var(--accent);
        background: rgba(224, 24, 36, 0.08);
        font-size: 12px;
        font-weight: 800;
        line-height: 1;
        cursor: pointer;
        transition: border-color var(--theme-duration) var(--theme-ease),
          background-color var(--theme-duration) var(--theme-ease),
          color var(--theme-duration) var(--theme-ease),
          box-shadow var(--theme-duration) var(--theme-ease);
      }

      .full-text-button:hover,
      .full-text-button:focus {
        border-color: var(--accent);
        box-shadow: 0 0 0 2px rgba(224, 24, 36, 0.12);
      }

      body.heaven-theme .full-text-button {
        background: rgba(211, 29, 29, 0.07);
      }

      .full-text-modal-body {
        max-height: min(62vh, 620px);
        overflow-y: auto;
        display: grid;
        gap: 14px;
      }

      .full-text-modal-section {
        display: grid;
        gap: 6px;
        padding: 10px;
        border: 1px solid var(--line);
        border-radius: 8px;
        background: rgba(224, 24, 36, 0.06);
      }

      .full-text-modal-section-title {
        color: var(--accent);
        font-size: 13px;
        font-weight: 900;
        text-transform: uppercase;
      }

      .full-text-modal-section-content {
        white-space: normal;
        overflow-wrap: anywhere;
        line-height: 1.55;
      }

      .full-text-modal-plain {
        white-space: pre-wrap;
      }

      .full-text-modal-section-content ul {
        margin: 0;
        padding-left: 18px;
      }

      .full-text-modal-section-content li {
        margin: 0;
        padding: 0;
      }

      .full-text-modal-markdown p,
      .full-text-modal-markdown ul,
      .full-text-modal-markdown ol {
        margin-top: 0;
        margin-bottom: 8px;
      }

      .full-text-modal-markdown pre {
        max-width: 100%;
        overflow-x: auto;
        padding: 8px;
        border-radius: 8px;
        background: rgba(0, 0, 0, 0.26);
      }

      .full-text-modal-markdown code {
        font-size: 0.92em;
      }

      .full-text-modal-link {
        display: inline;
        color: var(--accent);
        overflow-wrap: anywhere;
      }

      body.heaven-theme .full-text-modal-section {
        background: rgba(211, 29, 29, 0.05);
      }

      .set-plot-grid {
        display: grid;
        grid-template-columns: repeat(2, minmax(0, 1fr));
        gap: 14px;
      }

      .set-plot-card {
        display: grid;
        gap: 8px;
      }

      .set-plot-button {
        display: block;
        padding: 0;
        border: 0;
        border-radius: 8px;
        background: transparent;
        overflow: hidden;
        cursor: pointer;
      }

      .set-plot-image {
        width: 100%;
        border: 1px solid var(--line);
        border-radius: 8px;
        background: rgba(255, 255, 255, 0.92);
        box-shadow: 0 14px 34px rgba(0, 0, 0, 0.22);
        transition: border-color var(--theme-duration) var(--theme-ease),
          box-shadow var(--theme-duration) var(--theme-ease),
          transform 180ms var(--theme-ease);
      }

      .set-plot-button:hover .set-plot-image,
      .set-plot-button:focus .set-plot-image {
        transform: scale(1.02);
        box-shadow: 0 0 0 2px rgba(255, 43, 52, 0.2),
          0 14px 34px rgba(0, 0, 0, 0.24);
      }

      .set-plot-caption {
        color: var(--muted);
        font-size: 13px;
        font-weight: 800;
        transition: color var(--theme-duration) var(--theme-ease);
      }

      .plot-modal-image {
        width: 100%;
        height: auto;
        border-radius: 8px;
        border: 1px solid var(--line);
        background: rgba(255, 255, 255, 0.98);
      }

      .account-portrait {
        display: grid;
        justify-items: center;
        gap: 12px;
        text-align: center;
      }

      .profile-photo,
      .avatar-thumb {
        background-size: 200% 200%;
        background-repeat: no-repeat;
      }

      .profile-photo {
        width: min(260px, 72vw);
        aspect-ratio: 1;
        border: 2px solid var(--line);
        border-radius: 8px;
        box-shadow: 0 12px 28px rgba(0, 0, 0, 0.28);
        transition: border-color var(--theme-duration) var(--theme-ease),
          box-shadow var(--theme-duration) var(--theme-ease);
      }

      .avatar-picker .form-group {
        margin: 0;
      }

      .avatar-picker .control-label {
        display: block;
        margin-bottom: 12px;
      }

      .avatar-picker .shiny-options-group {
        display: grid;
        grid-template-columns: 1fr;
        gap: 10px;
        max-height: min(520px, 58vh);
        overflow-y: auto;
        padding-right: 4px;
        animation: avatar-list-drop 180ms ease both;
      }

      .avatar-picker .radio {
        position: relative;
        margin: 0;
      }

      .avatar-picker input {
        position: absolute;
        opacity: 0;
      }

      .avatar-card {
        display: grid;
        grid-template-columns: 86px minmax(0, 1fr);
        align-items: center;
        gap: 14px;
        min-height: 96px;
        padding: 10px;
        border: 1px solid var(--line);
        border-radius: 8px;
        background: rgba(255, 255, 255, 0.05);
        cursor: pointer;
        transition: border-color var(--theme-duration) var(--theme-ease),
          background-color var(--theme-duration) var(--theme-ease),
          box-shadow var(--theme-duration) var(--theme-ease);
      }

      body.heaven-theme .avatar-card {
        background: rgba(255, 255, 255, 0.72);
      }

      .avatar-thumb {
        width: 86px;
        aspect-ratio: 1;
        border-radius: 8px;
      }

      .avatar-meta {
        display: grid;
        gap: 6px;
        min-width: 0;
      }

      .avatar-name {
        display: block;
        color: var(--ink);
        font-size: 15px;
        font-weight: 700;
        line-height: 1.2;
        text-align: left;
        transition: color var(--theme-duration) var(--theme-ease);
      }

      .avatar-desc {
        display: block;
        color: var(--muted);
        font-size: 13px;
        line-height: 1.35;
        text-align: left;
        transition: color var(--theme-duration) var(--theme-ease);
      }

      .avatar-picker input:checked + .avatar-card,
      .avatar-picker input:checked + span .avatar-card {
        border-color: var(--accent);
        box-shadow: 0 0 0 3px rgba(224, 24, 36, 0.22);
      }

      body.heaven-theme .avatar-picker input:checked + .avatar-card,
      body.heaven-theme .avatar-picker input:checked + span .avatar-card {
        box-shadow: 0 0 0 3px rgba(125, 203, 235, 0.28);
      }

      .form-group {
        margin-bottom: 0;
      }

      label {
        margin-bottom: 8px;
        color: var(--muted);
        font-size: 14px;
        font-weight: 600;
        transition: color var(--theme-duration) var(--theme-ease);
      }

      .form-control {
        height: 48px;
        border: 1px solid var(--line);
        border-radius: 8px;
        background: var(--field-bg);
        box-shadow: inset 3px 0 0 rgba(224, 24, 36, 0.8);
        color: var(--ink);
        font-size: 16px;
        transition: border-color var(--theme-duration) var(--theme-ease),
          background-color var(--theme-duration) var(--theme-ease),
          box-shadow var(--theme-duration) var(--theme-ease),
          color var(--theme-duration) var(--theme-ease);
      }

      body.heaven-theme .form-control {
        box-shadow: inset 3px 0 0 rgba(125, 203, 235, 0.9);
      }

      .form-control:focus {
        border-color: var(--accent);
        box-shadow: 0 0 0 3px rgba(224, 24, 36, 0.22),
          0 0 22px rgba(224, 24, 36, 0.18),
          inset 3px 0 0 rgba(255, 59, 31, 0.9);
      }

      body.heaven-theme .form-control:focus {
        box-shadow: 0 0 0 3px rgba(125, 203, 235, 0.22),
          0 0 22px rgba(217, 165, 46, 0.18),
          inset 3px 0 0 rgba(217, 165, 46, 0.9);
      }

      .form-control::placeholder {
        color: #8f6767;
      }

      body.heaven-theme .form-control::placeholder {
        color: #8ba6b4;
      }

      .run-button,
      .menu-button {
        width: 100%;
        min-height: 50px;
        border: 1px solid #090a0f;
        border-radius: 8px;
        color: var(--button-ink);
        font-size: 16px;
        font-weight: 700;
        transition: border-color var(--theme-duration) var(--theme-ease),
          background var(--theme-duration) var(--theme-ease),
          color var(--theme-duration) var(--theme-ease),
          box-shadow var(--theme-duration) var(--theme-ease);
      }

      .mini-thoth {
        position: fixed;
        right: 18px;
        bottom: 18px;
        z-index: 4;
        display: grid;
        grid-template-columns: 48px minmax(0, 260px);
        gap: 10px;
        align-items: end;
        max-width: calc(100vw - 36px);
        text-align: left;
        animation: thoth-rise 360ms var(--theme-ease) both;
      }

      .mini-thoth-avatar {
        width: 48px;
        height: auto;
        align-self: end;
        filter: drop-shadow(0 12px 18px rgba(0, 0, 0, 0.4));
        transition: filter var(--theme-duration) var(--theme-ease),
          transform var(--theme-duration) var(--theme-ease);
      }

      .mini-thoth-avatar-norse,
      .mini-thoth-title-norse,
      .mini-thoth-text-norse {
        display: none;
      }

      body.myth-norse .mini-thoth-avatar-egypt,
      body.myth-norse .mini-thoth-title-egypt,
      body.myth-norse .mini-thoth-text-egypt {
        display: none;
      }

      body.myth-norse .mini-thoth-avatar-norse,
      body.myth-norse .mini-thoth-title-norse,
      body.myth-norse .mini-thoth-text-norse {
        display: block;
      }

      body.myth-norse .mini-thoth {
        grid-template-columns: 58px minmax(0, 260px);
      }

      body.myth-norse .mini-thoth-avatar {
        width: 58px;
      }

      body.myth-norse .mini-thoth-avatar-norse,
      body.myth-norse .logout-modal-avatar-norse {
        border-radius: 42%;
        clip-path: inset(3% 4% 4% 4% round 42%);
        background: transparent;
      }

      body.myth-norse.heaven-theme .mini-thoth-avatar-norse,
      body.myth-norse.heaven-theme .logout-modal-avatar-norse {
        mix-blend-mode: screen;
        filter: drop-shadow(0 10px 18px rgba(15, 67, 102, 0.28))
          drop-shadow(0 0 14px rgba(155, 220, 255, 0.58));
      }

      .mini-thoth-scroll {
        position: relative;
        padding: 12px 14px;
        border: 1px solid var(--line);
        border-radius: 8px;
        background: var(--panel);
        color: var(--ink);
        box-shadow: 0 18px 42px rgba(0, 0, 0, 0.34);
        transition: border-color var(--theme-duration) var(--theme-ease),
          background-color var(--theme-duration) var(--theme-ease),
          color var(--theme-duration) var(--theme-ease),
          box-shadow var(--theme-duration) var(--theme-ease);
      }

      .mini-thoth-title {
        margin-bottom: 4px;
        color: var(--accent);
        font-size: 13px;
        font-weight: 900;
        transition: color var(--theme-duration) var(--theme-ease);
      }

      .mini-thoth-text {
        color: var(--muted);
        font-size: 13px;
        line-height: 1.35;
        transition: color var(--theme-duration) var(--theme-ease);
      }

      .mini-thoth-close {
        position: absolute;
        top: -8px;
        right: -8px;
        z-index: 1;
        display: grid;
        place-items: center;
        width: 26px;
        height: 26px;
        border: 1px solid var(--line);
        border-radius: 50%;
        background: var(--panel);
        color: var(--ink);
        font-size: 14px;
        font-weight: 900;
        line-height: 1;
        box-shadow: 0 8px 18px rgba(0, 0, 0, 0.28);
        transition: border-color var(--theme-duration) var(--theme-ease),
          background-color var(--theme-duration) var(--theme-ease),
          color var(--theme-duration) var(--theme-ease),
          box-shadow var(--theme-duration) var(--theme-ease);
      }

      .mini-thoth-close:hover,
      .mini-thoth-close:focus {
        border-color: var(--accent);
        color: var(--accent);
      }

      .logout-modal {
        display: grid;
        grid-template-columns: 64px minmax(0, 1fr);
        gap: 14px;
        align-items: center;
      }

      .logout-modal-avatar {
        width: 64px;
        height: auto;
        filter: drop-shadow(0 10px 18px rgba(0, 0, 0, 0.35));
      }

      .logout-modal-avatar-norse,
      .logout-modal-title-norse {
        display: none;
      }

      body.myth-norse .logout-modal-avatar-egypt,
      body.myth-norse .logout-modal-title-egypt {
        display: none;
      }

      body.myth-norse .logout-modal-avatar-norse,
      body.myth-norse .logout-modal-title-norse {
        display: block;
      }

      .logout-modal-copy {
        display: grid;
        gap: 6px;
      }

      .logout-modal-title {
        color: var(--ink);
        font-size: 20px;
        font-weight: 900;
      }

      .logout-modal-text {
        color: var(--muted);
        font-size: 15px;
        line-height: 1.4;
      }

      .modal-content {
        background: var(--panel);
        border: 1px solid var(--line);
        border-radius: 8px;
        color: var(--ink);
        box-shadow: 0 20px 48px rgba(0, 0, 0, 0.42);
      }

      .modal-header,
      .modal-footer {
        border-color: rgba(224, 24, 36, 0.22);
      }

      .modal-body {
        color: var(--ink);
      }

      .logout-modal-footer {
        display: grid;
        grid-template-columns: repeat(2, minmax(0, 1fr));
        gap: 12px;
        width: 100%;
      }

      .logout-modal-footer .run-button,
      .logout-modal-footer .secondary-button {
        min-height: 48px;
        margin: 0;
      }

      .run-button,
      .primary-button {
        background: var(--accent);
        box-shadow: 5px 5px 0 var(--button-shadow), -5px -5px 0 rgba(255, 59, 31, 0.55),
          0 0 28px rgba(224, 24, 36, 0.22);
      }

      .secondary-button {
        background: var(--secondary-bg);
        border-color: var(--secondary-line);
        color: var(--secondary-ink);
        box-shadow: 5px 5px 0 var(--button-shadow), -5px -5px 0 rgba(143, 0, 8, 0.64);
      }

      body.heaven-theme .run-button,
      body.heaven-theme .primary-button {
        background: linear-gradient(135deg, #fff1a8 0%, #e1ad31 45%, #8fd7ee 100%);
        border-color: #d1a036;
        box-shadow: 5px 5px 0 rgba(120, 154, 174, 0.52),
          -5px -5px 0 rgba(255, 255, 255, 0.92),
          0 0 28px rgba(217, 165, 46, 0.22);
      }

      body.heaven-theme .secondary-button {
        background: linear-gradient(135deg, #ffffff 0%, #e9f8ff 100%);
        border-color: #8ecce3;
        color: #1f5d78;
        box-shadow: 5px 5px 0 rgba(120, 154, 174, 0.42),
          -5px -5px 0 rgba(255, 255, 255, 0.95);
      }

      .run-button:hover,
      .run-button:focus,
      .menu-button:hover,
      .menu-button:focus {
        background: var(--accent-hover);
        color: var(--button-ink);
      }

      body.heaven-theme .run-button:hover,
      body.heaven-theme .run-button:focus,
      body.heaven-theme .primary-button:hover,
      body.heaven-theme .primary-button:focus {
        background: linear-gradient(135deg, #fff6c8 0%, #f0bd45 48%, #a8e6f8 100%);
        color: #1d2024;
      }

      body.heaven-theme .secondary-button:hover,
      body.heaven-theme .secondary-button:focus {
        background: #ffffff;
        color: #174e65;
      }

      body.myth-norse.heaven-theme .run-button,
      body.myth-norse.heaven-theme .primary-button {
        background: linear-gradient(135deg, #eefcff 0%, #8fd6ff 48%, #1b5c91 100%);
        border-color: #b9ecff;
        color: #102334;
        box-shadow: 5px 5px 0 rgba(4, 24, 48, 0.46),
          -5px -5px 0 rgba(255, 255, 255, 0.72),
          0 0 32px rgba(190, 241, 255, 0.58);
      }

      body.myth-norse.heaven-theme .secondary-button,
      body.myth-norse.heaven-theme .menu-button {
        border-color: #a9e8ff;
        background: linear-gradient(135deg, rgba(242, 252, 255, 0.96), rgba(98, 170, 219, 0.74));
        color: #102334;
        box-shadow: 5px 5px 0 rgba(4, 24, 48, 0.4),
          0 0 24px rgba(190, 241, 255, 0.38);
      }

      body.myth-norse.heaven-theme .run-button:hover,
      body.myth-norse.heaven-theme .run-button:focus,
      body.myth-norse.heaven-theme .primary-button:hover,
      body.myth-norse.heaven-theme .primary-button:focus,
      body.myth-norse.heaven-theme .secondary-button:hover,
      body.myth-norse.heaven-theme .secondary-button:focus,
      body.myth-norse.heaven-theme .menu-button:hover,
      body.myth-norse.heaven-theme .menu-button:focus {
        background: linear-gradient(135deg, #ffffff 0%, #bdf2ff 52%, #3f91c4 100%);
        color: #081d2d;
      }

      .home::before,
      .home::after {
        content: '';
        position: fixed;
        z-index: -1;
        left: 0;
        right: 0;
        height: 1px;
        background: rgba(255, 43, 52, 0.52);
        opacity: 0;
        pointer-events: none;
        animation: scan-glitch 5.4s infinite steps(1, end);
      }

      body.heaven-theme .home::before,
      body.heaven-theme .home::after {
        background: rgba(125, 203, 235, 0.5);
      }

      .home::before {
        top: 28%;
      }

      .home::after {
        top: 72%;
        animation-delay: 1.4s;
      }

      @media (max-width: 640px) {
        .container-fluid {
          padding-top: 156px;
        }

        .account-menu {
          left: 12px;
          width: 190px;
        }

        .theme-switch {
          right: 12px;
        }

        .github-rate-card {
          right: 12px;
          width: min(190px, calc(100vw - 24px));
        }

        .avatar-card {
          grid-template-columns: 76px minmax(0, 1fr);
          gap: 12px;
        }

        .avatar-thumb {
          width: 76px;
        }

        .protocol-grid {
          grid-template-columns: 1fr;
        }

        .set-title-page {
          min-height: 520px;
          padding: 40px 18px;
        }

        .archive-thoth {
          position: static;
          width: min(100%, 220px);
          margin-top: 16px;
          margin-left: auto;
          opacity: 0.18;
        }

        .archive-actions {
          grid-template-columns: 1fr;
        }

        .divine-seals {
          grid-template-columns: 1fr;
          width: min(100%, 220px);
        }

        .set-plot-grid {
          grid-template-columns: 1fr;
        }

        .account-portrait {
          grid-template-columns: 1fr;
          justify-items: center;
          text-align: center;
        }

        .mini-thoth {
          right: 12px;
          bottom: 12px;
          grid-template-columns: 42px minmax(0, 1fr);
        }

        .mini-thoth-avatar {
          width: 42px;
          font-size: 21px;
        }

        .mini-thoth-scroll {
          padding: 10px 12px;
        }
      }

      @keyframes avatar-list-drop {
        from {
          opacity: 0;
          transform: translateY(-8px);
        }
        to {
          opacity: 1;
          transform: translateY(0);
        }
      }

      @keyframes site-glitch-blocks {
        0%, 72%, 76%, 100% {
          opacity: 0;
          transform: translate(0, 0);
        }
        73% {
          opacity: 1;
          transform: translate(14px, -6px);
        }
        74% {
          opacity: 0.45;
          transform: translate(-18px, 10px);
        }
        75% {
          opacity: 0.9;
          transform: translate(6px, 16px);
        }
      }

      @keyframes scan-glitch {
        0%, 84%, 88%, 100% {
          opacity: 0;
          transform: translateX(0);
        }
        85% {
          opacity: 1;
          transform: translateX(-18px);
        }
        86% {
          opacity: 0.5;
          transform: translateX(22px);
        }
        87% {
          opacity: 0.9;
          transform: translateX(-8px);
        }
      }

      @keyframes norse-lava-pulse {
        0%, 100% {
          filter: brightness(0.85) saturate(1.1);
          opacity: 0.52;
        }
        50% {
          filter: brightness(1.35) saturate(1.7);
          opacity: 1;
        }
      }

      @keyframes norse-runes-glow {
        0%, 100% {
          opacity: 0.52;
          filter: drop-shadow(0 0 12px rgba(255, 255, 255, 0.72))
            drop-shadow(0 0 28px rgba(143, 216, 255, 0.55));
        }
        48%, 56% {
          opacity: 1;
          filter: drop-shadow(0 0 22px rgba(255, 255, 255, 1))
            drop-shadow(0 0 58px rgba(172, 235, 255, 0.98));
        }
      }

      @keyframes logo-glitch-a {
        0%, 82%, 100% {
          clip-path: inset(0 0 0 0);
        }
        83% {
          clip-path: inset(10% 0 70% 0);
        }
        84% {
          clip-path: inset(58% 0 18% 0);
        }
      }

      @keyframes logo-glitch-b {
        0%, 76%, 100% {
          clip-path: inset(0 0 0 0);
        }
        77% {
          clip-path: inset(42% 0 36% 0);
        }
        78% {
          clip-path: inset(18% 0 62% 0);
        }
      }

      @keyframes thoth-rise {
        from {
          opacity: 0;
          transform: translateY(10px);
        }
        to {
          opacity: 1;
          transform: translateY(0);
        }
      }

      @keyframes progress-shine {
        to {
          transform: translateX(100%);
        }
      }
    "))
  ),
  div(
    class = "theme-switch",
    tags$button(
      id = "theme_heaven",
      type = "button",
      class = "theme-button",
      `aria-pressed` = "false",
      "Рай"
    ),
    tags$button(
      id = "theme_hell",
      type = "button",
      class = "theme-button active",
      `aria-pressed` = "true",
      "Ад"
    )
  ),
  uiOutput("github_rate_limit_ui"),
  uiOutput("account_menu"),
  uiOutput("page")
)

server <- function(input, output, session) {
  notify_user <- function(text, type = "message", duration = 5) {
    tryCatch({
      if (is.function(session$sendNotification)) {
        shiny::showNotification(text, type = type, duration = duration, session = session)
      } else {
        base::message(text)
      }
    }, error = function(e) {
      base::message(text)
    })
  }

  current_page <- reactiveVal("boot")
  avatars_open <- reactiveVal(FALSE)
  analysis_target <- reactiveVal(NULL)
  theme_mode <- reactiveVal("hell")
  mythology_style <- reactiveVal("egypt")
  set_report <- reactiveVal(NULL)
  active_protocol <- reactiveVal("set")
  report_archive <- reactiveVal(list())
  archive_page <- reactiveVal(1L)
  menu_open <- reactiveVal(FALSE)
  remember_token <- reactiveVal(NULL)
  remember_attempted <- reactiveVal(FALSE)
  remember_input_seen <- reactiveVal(FALSE)
  github_oauth_processed <- reactiveVal(FALSE)
  set_progress <- reactiveValues(value = 0, label = "Ожидание запуска")
  account <- reactiveValues(
    email = NULL,
    nickname = NULL,
    avatar_id = "egypt_1",
    github_token = "",
    github_token_name = "",
    github_token_expires_at = NA_character_,
    mythology_style = "egypt"
  )
  parse_github_token_expires_at <- function(value) {
    value <- value %||% NA_character_
    if (inherits(value, "POSIXt")) {
      return(as.POSIXct(value, tz = "UTC"))
    }
    value <- trimws(as.character(value))
    if (!nzchar(value) || is.na(value)) {
      return(as.POSIXct(NA))
    }
    suppressWarnings(as.POSIXct(value, tz = "UTC"))
  }
  account_github_token_expired <- function() {
    token <- trimws(account$github_token %||% "")
    if (!nzchar(token)) {
      return(FALSE)
    }
    expires_at <- parse_github_token_expires_at(account$github_token_expires_at)
    !is.na(expires_at) && Sys.time() >= expires_at
  }
  account_github_token <- function() {
    if (isTRUE(account_github_token_expired())) {
      return("")
    }
    trimws(account$github_token %||% "")
  }
  apply_account_row <- function(row) {
    account$email <- row$email[[1]]
    account$nickname <- row$nickname[[1]]
    account$avatar_id <- row$avatar_id[[1]]
    account$github_token <- row$github_token[[1]] %||% ""
    account$github_token_name <- if ("github_token_name" %in% names(row)) row$github_token_name[[1]] %||% "" else ""
    account$github_token_expires_at <- if ("github_token_expires_at" %in% names(row)) {
      row$github_token_expires_at[[1]] %||% NA_character_
    } else {
      NA_character_
    }
    style <- if ("mythology_style" %in% names(row)) row$mythology_style[[1]] %||% "egypt" else "egypt"
    style <- tolower(trimws(as.character(style)))
    if (!style %in% c("egypt", "norse", "greece")) {
      style <- "egypt"
    }
    if (identical(style, "greece")) {
      style <- "egypt"
    }
    account$mythology_style <- style
    mythology_style(style)
    invisible(TRUE)
  }
  github_rate_limit <- reactiveVal(list(
    status = "loading",
    remaining = NA_integer_,
    limit = NA_integer_,
    reset = NA_real_,
    updated_at = Sys.time(),
    source = "none",
    error = NULL
  ))

  fetch_github_rate_limit <- function(token = "") {
    token <- trimws(as.character(token %||% ""))
    headers <- c(
      Accept = "application/vnd.github+json",
      `User-Agent` = "GitHound-Shiny",
      `X-GitHub-Api-Version` = "2022-11-28",
      `Cache-Control` = "no-cache",
      Pragma = "no-cache"
    )
    if (nzchar(token)) {
      headers <- c(headers, Authorization = paste("Bearer", token))
    }

    response <- httr::GET(
      "https://api.github.com/rate_limit",
      do.call(httr::add_headers, as.list(headers)),
      httr::timeout(8)
    )
    text <- httr::content(response, as = "text", encoding = "UTF-8")
    if (httr::http_error(response)) {
      stop("GitHub /rate_limit returned HTTP ", httr::status_code(response), call. = FALSE)
    }

    payload <- jsonlite::fromJSON(text, simplifyVector = FALSE)
    core <- payload$resources$core %||% payload$rate %||% list()
    response_headers <- httr::headers(response)
    header_value <- function(name) {
      value <- response_headers[[name]] %||% response_headers[[tolower(name)]]
      if (is.null(value) || length(value) == 0L) NA_character_ else as.character(value[[1L]])
    }
    header_int <- function(name) suppressWarnings(as.integer(header_value(name)))
    header_num <- function(name) suppressWarnings(as.numeric(header_value(name)))

    remaining <- suppressWarnings(as.integer(core$remaining %||% header_int("x-ratelimit-remaining")))
    limit <- suppressWarnings(as.integer(core$limit %||% header_int("x-ratelimit-limit")))
    reset <- suppressWarnings(as.numeric(core$reset %||% header_num("x-ratelimit-reset")))
    resource <- header_value("x-ratelimit-resource")
    if (is.na(resource) || !nzchar(trimws(resource))) {
      resource <- "core"
    }

    list(
      status = "ok",
      remaining = remaining,
      limit = limit,
      reset = reset,
      used = suppressWarnings(as.integer(core$used %||% header_int("x-ratelimit-used"))),
      resource = as.character(resource),
      updated_at = Sys.time(),
      source = if (nzchar(token)) "account" else "anonymous",
      error = NULL
    )
  }
  protocol_queue <- reactiveVal(list())
  protocol_active_job <- reactiveVal(NULL)
  protocol_worker <- reactiveVal(NULL)
  protocol_job_dir <- file.path(tempdir(), "githound_protocol_jobs")
  dir.create(protocol_job_dir, recursive = TRUE, showWarnings = FALSE)

  normalize_mythology_style <- function(value) {
    value <- tolower(trimws(as.character(value %||% "egypt")))
    if (!value %in% c("egypt", "norse", "greece")) {
      value <- "egypt"
    }
    value
  }

  apply_account_mythology_style <- function(value) {
    value <- normalize_mythology_style(value)
    if (identical(value, "greece")) {
      value <- "egypt"
    }
    account$mythology_style <- value
    mythology_style(value)
    invisible(value)
  }

  protocol_new_job_id <- function() {
    paste0(
      format(Sys.time(), "%Y%m%d%H%M%OS3"),
      "_",
      paste(sample(c(letters, 0:9), 8L, replace = TRUE), collapse = "")
    )
  }

  protocol_progress_path <- function(job_id) {
    file.path(protocol_job_dir, paste0(job_id, ".rds"))
  }

  protocol_input_path <- function(job_id) {
    file.path(protocol_job_dir, paste0(job_id, "_input.rds"))
  }

  protocol_result_path <- function(job_id) {
    file.path(protocol_job_dir, paste0(job_id, "_result.rds"))
  }

  protocol_log_path <- function(job_id) {
    file.path(protocol_job_dir, paste0(job_id, ".log"))
  }

  github_oauth_callback_present <- function() {
    query <- parseQueryString(session$clientData$url_search %||% "")
    nzchar(trimws(query$code %||% ""))
  }

  oauth_callback_pending <- function() {
    isTRUE(github_oauth_callback_present()) &&
      !isTRUE(github_oauth_processed())
  }

  refresh_github_rate_limit <- function() {
    token <- isolate(account_github_token())
    github_rate_limit(tryCatch(
      fetch_github_rate_limit(token),
      error = function(e) list(
        status = "error",
        remaining = NA_integer_,
        limit = NA_integer_,
        reset = NA_real_,
        updated_at = Sys.time(),
        source = if (nzchar(trimws(token))) "account" else "anonymous",
        error = conditionMessage(e)
      )
    ))
  }

  observe({
    active_job <- protocol_active_job()
    invalidateLater(if (is.null(active_job)) 60000 else 10000, session)
    refresh_github_rate_limit()
  })

  protocol_write_progress <- function(path, value = 0, label = NULL) {
    dir.create(dirname(path), recursive = TRUE, showWarnings = FALSE)
    saveRDS(list(value = value, label = label, updated_at = Sys.time()), path)
  }

  protocol_read_progress <- function(path) {
    if (!file.exists(path)) return(NULL)
    tryCatch(readRDS(path), error = function(e) NULL)
  }

  update_protocol_progress <- function(value, label = NULL) {
    value <- max(0, min(100, as.integer(value %||% 0)))
    if (is.null(label) || !nzchar(label)) {
      label <- set_progress$label %||% ""
    }
    set_progress$value <- value
    set_progress$label <- label
    session$sendCustomMessage("setSetProgress", list(value = value, label = label))
  }

  sync_protocol_progress <- function(job) {
    progress <- protocol_read_progress(job$progress_path)
    if (is.list(progress)) {
      update_protocol_progress(progress$value %||% 0, progress$label %||% job$label)
    }
  }

  complete_protocol_job <- function(job, result) {
    sync_protocol_progress(job)
    protocol_active_job(NULL)
    protocol_worker(NULL)
    try(unlink(c(job$progress_path, job$input_path)), silent = TRUE)
    refresh_github_rate_limit()

    if (is.list(result) && isTRUE(result$ok)) {
      job$on_success(result$result)
    } else {
      job$on_error(result$error %||% "Protocol job failed.")
    }
    start_next_protocol_job()
  }

  poll_protocol_job <- function() {
    job <- isolate(protocol_active_job())
    if (is.null(job)) return(invisible(FALSE))

    sync_protocol_progress(job)
    if (!file.exists(job$result_path)) {
      later::later(poll_protocol_job, delay = 0.5)
      return(invisible(TRUE))
    }

    result <- tryCatch(
      readRDS(job$result_path),
      error = function(e) list(ok = FALSE, error = conditionMessage(e))
    )
    complete_protocol_job(job, result)
    invisible(TRUE)
  }

  start_next_protocol_job <- function() {
    if (!is.null(isolate(protocol_active_job()))) return(invisible(FALSE))

    queue <- isolate(protocol_queue())
    if (length(queue) == 0L) return(invisible(FALSE))

    job <- queue[[1L]]
    protocol_queue(queue[-1L])
    protocol_active_job(job)
    analysis_target(job$target)
    active_protocol(job$protocol_type %||% "set")
    current_page("set_loading")
    update_protocol_progress(0, job$label)
    protocol_write_progress(job$progress_path, 0, job$label)

    worker_script <- file.path(app_dir, "protocol_worker.R")
    rscript <- file.path(R.home("bin"), if (identical(.Platform$OS.type, "windows")) "Rscript.exe" else "Rscript")
    if (!file.exists(rscript)) {
      rscript <- "Rscript"
    }

    try(unlink(job$result_path), silent = TRUE)
    saveRDS(job$spec, job$input_path)
    started <- tryCatch({
      system2(
        rscript,
        args = c(worker_script, job$input_path),
        stdout = job$log_path,
        stderr = job$log_path,
        wait = FALSE
      )
      TRUE
    }, error = function(e) {
      complete_protocol_job(job, list(ok = FALSE, error = conditionMessage(e)))
      FALSE
    })

    if (isTRUE(started)) {
      protocol_worker(list(started_at = Sys.time(), log_path = job$log_path))
      later::later(poll_protocol_job, delay = 0.5)
    }

    invisible(TRUE)
  }

  enqueue_protocol_job <- function(job) {
    queue <- isolate(protocol_queue())
    protocol_queue(c(queue, list(job)))
    start_next_protocol_job()
    invisible(TRUE)
  }

  session$onSessionEnded(function() {
    protocol_queue(list())
    protocol_active_job(NULL)
    protocol_worker(NULL)
  })

  launch_protocol <- function(protocol_type, target = trimws(analysis_target() %||% "")) {
    target <- trimws(target %||% "")
    protocol_type <- tolower(protocol_type %||% "")
    token <- account_github_token()

    if (!nzchar(target)) {
      notify_user("Не указана цель.", type = "error", duration = 6)
      current_page("analysis")
      return(invisible(FALSE))
    }

    if (protocol_type %in% c("set", "quality")) {
      if (!nzchar(token)) {
        if (isTRUE(account_github_token_expired())) {
          notify_user("GitHoundToken истек. Войдите через GitHub еще раз, чтобы выпустить новый токен.", type = "error", duration = 8)
          current_page("account")
          return(invisible(FALSE))
        }
        notify_user("В личном кабинете не указан GitHub токен.", type = "error", duration = 7)
        current_page("account")
        return(invisible(FALSE))
      }
    }

    analysis_target(target)
    set_report(NULL)
    active_protocol(protocol_type)
    launch_theme <- theme_mode()
    launch_mythology <- mythology_style()
    load_user_archive()
    archive_same_target(target, protocol_type)
    current_page("set_loading")

    if (identical(protocol_type, "isis")) {
      set_progress$value <- 0
      set_progress$label <- "Запуск протокола Исиды"
      try(session$flushReact(), silent = TRUE)
      session$sendCustomMessage("setSetProgress", list(value = 0, label = "Запуск протокола Исиды"))
      progress_label <- "Запуск протокола Исиды"

      run_job <- function() {
        tryCatch({
          result <- run_isis_protocol(
            profile = target,
            conn = conn,
            progress = function(value, label = NULL) {
              set_progress$value <- value
              if (!is.null(label) && nzchar(label)) {
                progress_label <<- label
                set_progress$label <- label
              }
              session$sendCustomMessage("setSetProgress", list(value = value, label = progress_label))
              try(session$flushReact(), silent = TRUE)
            }
          )

          report <- result$report
          report$theme <- launch_theme
          report$view_theme <- launch_theme
          report$mythology_style <- launch_mythology
          report$view_mythology <- launch_mythology
          set_report(report)
          add_report_to_archive(report, target)
          current_page("set_report")
        }, error = function(e) {
          set_progress$value <- 0
          set_progress$label <- "Протокол остановлен"
          session$sendCustomMessage("setSetProgress", list(value = 0, label = "Протокол остановлен"))
          notify_user(conditionMessage(e), type = "error", duration = 10)
          current_page("protocol")
        })
      }
    } else {
      token <- account_github_token()
      protocol_start_label <- paste("Запуск протокола", archive_protocol_label(protocol_type))
      set_progress$value <- 0
      set_progress$label <- protocol_start_label
      try(session$flushReact(), silent = TRUE)
      session$sendCustomMessage("setSetProgress", list(value = 0, label = protocol_start_label))
      progress_label <- protocol_start_label

      run_job <- function() {
        tryCatch({
          result <- run_set_protocol(
            profile = target,
            token = token,
            conn = conn,
            progress = function(value, label = NULL) {
              set_progress$value <- value
              if (!is.null(label) && nzchar(label)) {
                progress_label <<- label
                set_progress$label <- label
              }
              session$sendCustomMessage("setSetProgress", list(value = value, label = progress_label))
              try(session$flushReact(), silent = TRUE)
            }
          )

          report <- result$report
          report$theme <- launch_theme
          report$view_theme <- launch_theme
          report$mythology_style <- launch_mythology
          report$view_mythology <- launch_mythology
          set_report(report)
          add_report_to_archive(report, target)
          current_page("set_report")
        }, error = function(e) {
          set_progress$value <- 0
          set_progress$label <- "Протокол остановлен"
          session$sendCustomMessage("setSetProgress", list(value = 0, label = "Протокол остановлен"))
          notify_user(conditionMessage(e), type = "error", duration = 10)
          current_page("protocol")
        })
      }
    }

    job_id <- protocol_new_job_id()
    job <- list(
      id = job_id,
      target = target,
      protocol_type = protocol_type,
      label = progress_label,
      progress_path = protocol_progress_path(job_id),
      input_path = protocol_input_path(job_id),
      result_path = protocol_result_path(job_id),
      log_path = protocol_log_path(job_id),
      spec = list(
        protocol_type = protocol_type,
        target = target,
        token = token,
        conn = conn,
        label = progress_label,
        progress_path = protocol_progress_path(job_id),
        result_path = protocol_result_path(job_id),
        project_root = project_root,
        account_storage_path = source_path,
        set_protocol_path = set_protocol_path
      ),
      on_success = function(result) {
        report <- result$report
        report$theme <- launch_theme
        report$view_theme <- launch_theme
        report$mythology_style <- launch_mythology
        report$view_mythology <- launch_mythology
        set_report(report)
        add_report_to_archive(report, target)
        current_page("set_report")
      },
      on_error = function(message) {
        update_protocol_progress(0, "Protocol stopped")
        notify_user(message, type = "error", duration = 10)
        current_page("protocol")
      }
    )

    enqueue_protocol_job(job)

    invisible(TRUE)
  }

  ensure_report_archive_table <- function() {
    sql <- paste0(
      "CREATE TABLE IF NOT EXISTS ",
      quote_table_ident("githound_report_archive", conn$dbname),
      " (",
      "report_id String, ",
      "email String, ",
      "target String, ",
      "status String, ",
      "created_at String, ",
      "expires_at String, ",
      "archived_at String, ",
      "report_blob String, ",
      "version UInt64",
      ") ENGINE = ReplacingMergeTree(version) ",
      "ORDER BY (email, report_id)"
    )
    clickhouse_request(conn, sql, parse_json = FALSE)
    invisible(TRUE)
  }

  report_to_blob <- function(report) {
    if (!requireNamespace("openssl", quietly = TRUE)) {
      stop("Для архива нужен R-пакет openssl.", call. = FALSE)
    }
    openssl::base64_encode(serialize(report, NULL))
  }

  blob_to_report <- function(blob) {
    unserialize(openssl::base64_decode(blob))
  }

  archive_time_label <- function(x) {
    format(as.POSIXct(x, origin = "1970-01-01", tz = "UTC"), "%Y-%m-%d %H:%M:%S", tz = "UTC")
  }

  archive_row_to_item <- function(row) {
    created_at <- as.POSIXct(row$created_at[[1]], tz = "UTC")
    expires_at <- as.POSIXct(row$expires_at[[1]], tz = "UTC")
    report <- blob_to_report(row$report_blob[[1]])
    list(
      id = row$report_id[[1]],
      target = row$target[[1]],
      protocol_type = report$protocol_type %||% "set",
      status = row$status[[1]],
      created_at = created_at,
      expires_at = expires_at,
      created_at_label = archive_time_label(created_at),
      expires_at_label = archive_time_label(expires_at),
      archived_at = if (nzchar(row$archived_at[[1]])) as.POSIXct(row$archived_at[[1]], tz = "UTC") else NULL,
      theme = report_theme_value(report, fallback = "hell"),
      protocol_label = report$protocol_label %||% archive_protocol_label(report$protocol_type %||% "set"),
      report = report
    )
  }

  save_archive_item <- function(item, email = isolate(account$email)) {
    if (!nzchar(email %||% "")) {
      return(invisible(FALSE))
    }
    ensure_report_archive_table()
    row <- data.frame(
      report_id = item$id,
      email = normalize_githound_email(email),
      target = item$target,
      status = item$status,
      created_at = archive_time_label(item$created_at),
      expires_at = archive_time_label(item$expires_at),
      archived_at = if (is.null(item$archived_at)) "" else archive_time_label(item$archived_at),
      report_blob = report_to_blob(item$report),
      version = as.integer(as.numeric(Sys.time())),
      stringsAsFactors = FALSE
    )
    load_df_to_clickhouse(row, table_name = "githound_report_archive", conn = conn, append = TRUE)
    invisible(TRUE)
  }

  persist_archive_status <- function(item, email = isolate(account$email)) {
    save_archive_item(item, email = email)
  }

  load_user_archive <- function(email = isolate(account$email)) {
    if (!nzchar(email %||% "")) {
      report_archive(list())
      return(list())
    }
    ensure_report_archive_table()
    sql <- paste0(
      "SELECT * FROM ",
      quote_table_ident("githound_report_archive", conn$dbname),
      " FINAL WHERE lower(email) = ",
      githound_sql_string(normalize_githound_email(email)),
      " AND status != 'deleted'",
      " ORDER BY created_at DESC"
    )
    rows <- query_clickhouse(conn, sql)
    if (!is.data.frame(rows) || nrow(rows) == 0L) {
      report_archive(list())
      return(list())
    }
    records <- lapply(seq_len(nrow(rows)), function(i) archive_row_to_item(rows[i, , drop = FALSE]))
    refresh_archive_status(records)
  }

  refresh_archive_status <- function(records = isolate(report_archive())) {
    now <- Sys.time()
    updated <- lapply(records, function(item) {
      expires_at <- item$expires_at %||% (item$created_at + 7 * 24 * 60 * 60)
      if (identical(item$status %||% "active", "active") && now >= expires_at) {
        item$status <- "archived"
        item$archived_at <- now
        try(persist_archive_status(item), silent = TRUE)
      }
      item
    })
    report_archive(updated)
    updated
  }

  archive_same_target <- function(target, protocol_type = NULL) {
    records <- refresh_archive_status()
    normalized_target <- tolower(trimws(target %||% ""))
    normalized_protocol <- tolower(trimws(protocol_type %||% ""))
    matching_idx <- which(vapply(records, function(item) {
      item_protocol <- tolower(trimws(item$protocol_type %||% item$report$protocol_type %||% "set"))
      identical(item$status %||% "active", "active") &&
        identical(tolower(trimws(item$target %||% "")), normalized_target) &&
        (!nzchar(normalized_protocol) || identical(item_protocol, normalized_protocol))
    }, logical(1)))

    if (length(matching_idx) == 0L) {
      return(invisible(records))
    }

    updated <- records
    archived_at <- Sys.time()
    for (idx in matching_idx) {
      updated[[idx]]$status <- "archived"
      updated[[idx]]$archived_at <- archived_at
      try(persist_archive_status(updated[[idx]]), silent = TRUE)
    }

    report_archive(updated)
    invisible(updated)
  }

  add_report_to_archive <- function(report, target) {
    report$theme <- report_theme_value(report, fallback = theme_mode())
    report$protocol_type <- tolower(report$protocol_type %||% "set")
    report$protocol_label <- report$protocol_label %||% archive_protocol_label(report$protocol_type)
    records <- archive_same_target(target, report$protocol_type)
    created_at <- Sys.time()
    expires_at <- created_at + 7 * 24 * 60 * 60
    item <- list(
      id = paste0(format(created_at, "%Y%m%d%H%M%S"), "-", sample(1000:9999, 1)),
      target = target,
      protocol_type = report$protocol_type,
      protocol_label = report$protocol_label,
      theme = report$theme,
      status = "active",
      created_at = created_at,
      expires_at = expires_at,
      created_at_label = format(created_at, "%Y-%m-%d %H:%M:%S"),
      expires_at_label = format(expires_at, "%Y-%m-%d %H:%M:%S"),
      report = report
    )
    report_archive(c(list(item), records))
    save_archive_item(item)
    invisible(item)
  }

  observeEvent(input$register, {
    current_page("registration")
  })

  observeEvent(input$login, {
    current_page("login")
  })

  observeEvent(input$back_to_landing, {
    current_page("landing")
  })

  observeEvent(input$back_to_landing_login, {
    current_page("landing")
  })

  observeEvent(input$go_analysis, {
    menu_open(FALSE)
    current_page("analysis")
  })

  observeEvent(input$toggle_account_menu, {
    menu_open(!isTRUE(menu_open()))
  }, ignoreInit = TRUE)

  observeEvent(input$menu_go_analysis, {
    req(account$email)
    menu_open(FALSE)
    current_page("analysis")
  }, ignoreInit = TRUE)

  observeEvent(input$open_account, {
    req(account$email)
    menu_open(FALSE)
    current_page("account")
  })

  observeEvent(input$open_archive, {
    menu_open(FALSE)
    load_user_archive()
    archive_page(1L)
    current_page("archive")
  })

  observeEvent(input$logout_request, {
    menu_open(FALSE)
    showModal(modalDialog(
      easyClose = TRUE,
      title = NULL,
      footer = div(
        class = "logout-modal-footer",
        actionButton("logout_cancel", "Нет", class = "menu-button secondary-button"),
        actionButton("logout_confirm", "Да", class = "run-button")
      ),
      div(
        class = "logout-modal",
        img(src = "mini-thoth.svg", class = "logout-modal-avatar logout-modal-avatar-egypt", alt = "Мини-Тот"),
        img(src = "mini-raven.png", class = "logout-modal-avatar logout-modal-avatar-norse", alt = "Мини-ворон"),
        div(
          class = "logout-modal-copy",
          div(class = "logout-modal-title logout-modal-title-egypt", "Мини-Тот"),
          div(class = "logout-modal-title logout-modal-title-norse", "Мини-ворон"),
          div(class = "logout-modal-text", "Вы уверены, что хотите выйти из аккаунта?")
        )
      )
    ))
  })

  observeEvent(input$logout_cancel, {
    removeModal()
  })

  observeEvent(input$logout_confirm, {
    if (nzchar(remember_token() %||% "")) {
      try(revoke_githound_remember_tokens(conn, token = remember_token()), silent = TRUE)
    }
    session$sendCustomMessage("rememberToken", list(token = ""))
    removeModal()
    account$email <- NULL
    account$nickname <- NULL
    account$avatar_id <- "egypt_1"
    account$github_token <- ""
    account$github_token_name <- ""
    account$github_token_expires_at <- NA_character_
    apply_account_mythology_style("egypt")
    remember_token(NULL)
    remember_attempted(TRUE)
    report_archive(list())
    archive_page(1L)
    analysis_target(NULL)
    set_report(NULL)
    avatars_open(FALSE)
    current_page("landing")
    notify_user("Вы вышли из аккаунта.", type = "message", duration = 5)
  })

  observeEvent(input$archive_prev, {
    archive_page(max(1L, archive_page() - 1L))
  })

  observeEvent(input$archive_next, {
    records <- refresh_archive_status()
    max_page <- max(1L, ceiling(length(records) / 15L))
    archive_page(min(max_page, archive_page() + 1L))
  })

  observeEvent(input$archive_open_id, {
    req(input$archive_open_id)
    records <- refresh_archive_status()
    match <- Filter(function(item) identical(item$id, input$archive_open_id), records)
    if (length(match) == 0L) {
      notify_user("Отчет не найден в архиве.", type = "error", duration = 6)
      return()
    }
    report <- match[[1]]$report
    if (!is.list(report)) {
      notify_user("Структура отчета повреждена и не может быть открыта.", type = "error", duration = 7)
      return()
    }
    report$theme <- match[[1]]$theme %||% report_theme_value(report, fallback = "hell")
    report$view_theme <- theme_mode()
    set_report(NULL)
    set_report(report)
    current_page("set_report")
  })

  observeEvent(input$plot_modal, {
    req(is.list(input$plot_modal), input$plot_modal$src)
    showModal(modalDialog(
      title = input$plot_modal$label %||% "График",
      easyClose = TRUE,
      size = "l",
      footer = modalButton("Закрыть"),
      img(src = input$plot_modal$src, class = "plot-modal-image", alt = input$plot_modal$label %||% "График")
    ))
  })

  observeEvent(input$show_full_text, {
    req(is.list(input$show_full_text))
    block_value <- function(name, fallback = "") {
      trimws(as.character(input$show_full_text[[name]] %||% fallback))
    }
    summary_text <- block_value("summary", input$show_full_text$text %||% "")
    details_text <- block_value("details")
    aliases_text <- block_value("aliases")
    references_text <- block_value("references")
    if (!any(nzchar(c(summary_text, details_text, aliases_text, references_text)))) {
      return()
    }
    title <- trimws(as.character(input$show_full_text$title %||% "Описание уязвимости"))
    if (!nzchar(title)) {
      title <- "Описание уязвимости"
    }
    render_modal_markdown <- function(value) {
      value <- paste(as.character(value %||% ""), collapse = "\n")
      value <- gsub("\r\n?", "\n", value)
      value <- gsub("\n{3,}", "\n\n", value)
      if (requireNamespace("commonmark", quietly = TRUE)) {
        return(HTML(commonmark::markdown_html(
          value,
          hardbreaks = TRUE,
          extensions = c("table", "strikethrough", "autolink")
        )))
      }
      if (requireNamespace("markdown", quietly = TRUE)) {
        return(HTML(markdown::markdownToHTML(
          text = value,
          fragment.only = TRUE,
          options = c("tables", "fenced_code_blocks", "autolink")
        )))
      }
      value
    }
    text_block <- function(label, value, markdown = FALSE) {
      value <- trimws(as.character(value %||% ""))
      if (!nzchar(value)) {
        return(NULL)
      }
      div(
        class = "full-text-modal-section",
        div(class = "full-text-modal-section-title", label),
        div(
          class = paste("full-text-modal-section-content", if (isTRUE(markdown)) "full-text-modal-markdown" else "full-text-modal-plain"),
          if (isTRUE(markdown)) render_modal_markdown(value) else value
        )
      )
    }
    list_block <- function(label, value, links = FALSE) {
      value <- trimws(as.character(value %||% ""))
      if (!nzchar(value)) {
        return(NULL)
      }
      items <- trimws(unlist(strsplit(value, ",", fixed = TRUE)))
      items <- items[nzchar(items)]
      if (length(items) == 0L) {
        return(text_block(label, value))
      }
      content <- if (isTRUE(links)) {
        tags$ul(lapply(items, function(item) {
          tags$li(tags$a(class = "full-text-modal-link", href = item, target = "_blank", rel = "noopener noreferrer", item))
        }))
      } else {
        tags$ul(lapply(items, tags$li))
      }
      div(
        class = "full-text-modal-section",
        div(class = "full-text-modal-section-title", label),
        div(class = "full-text-modal-section-content", content)
      )
    }
    showModal(modalDialog(
      title = title,
      easyClose = TRUE,
      size = "l",
      footer = modalButton("Закрыть"),
      div(
        class = "full-text-modal-body",
        text_block("Сводка", summary_text),
        text_block("Подробное описание", details_text, markdown = TRUE),
        list_block("Дополнительные индексы", aliases_text),
        list_block("Ссылки", references_text, links = TRUE)
      )
    ))
  })

  observeEvent(input$delete_archive_reports, {
    selected <- input$archive_selected_ids %||% character()
    if (length(selected) == 0L) {
      notify_user("Выберите отчеты для удаления.", type = "error", duration = 5)
      return()
    }
    records <- refresh_archive_status()
    now <- Sys.time()
    updated <- lapply(records, function(item) {
      if (item$id %in% selected) {
        item$status <- "deleted"
        item$archived_at <- now
        try(persist_archive_status(item), silent = TRUE)
      }
      item
    })
    visible_records <- Filter(function(item) !identical(item$status, "deleted"), updated)
    report_archive(visible_records)
    archive_page(min(archive_page(), max(1L, ceiling(length(visible_records) / 15L))))
    session$sendCustomMessage("clearArchiveSelection", list())
    notify_user("Выбранные отчеты удалены.", type = "message", duration = 5)
  })

  output$download_archive_reports <- downloadHandler(
    filename = function() {
      selected <- isolate(input$archive_selected_ids %||% character())
      records <- isolate(refresh_archive_status())
      chosen <- Filter(function(item) item$id %in% selected, records)
      if (length(chosen) == 1L) {
        safe_report_filename(chosen[[1]])
      } else {
        paste0("githound_reports_", format(Sys.time(), "%Y%m%d_%H%M%S"), ".zip")
      }
    },
    content = function(file) {
      selected <- isolate(input$archive_selected_ids %||% character())
      if (length(selected) == 0L) {
        stop("Выберите отчеты для скачивания.", call. = FALSE)
      }
      records <- refresh_archive_status()
      chosen <- Filter(function(item) item$id %in% selected, records)
      if (length(chosen) == 1L) {
        write_single_report_pdf(chosen[[1]], file)
      } else {
        write_reports_zip(chosen, file)
      }
    },
    contentType = "application/octet-stream"
  )

  observeEvent(input$toggle_avatars, {
    avatars_open(!isTRUE(avatars_open()))
  })

  observeEvent(input$theme_mode, {
    if (input$theme_mode %in% c("hell", "heaven")) {
      theme_mode(input$theme_mode)
    }
  }, ignoreInit = FALSE)

  observe({
    session$sendCustomMessage("applyMythologyStyle", list(style = mythology_style()))
  })

  observeEvent(input$style_egypt, {
    apply_account_mythology_style("egypt")
    if (nzchar(account$email %||% "")) {
      try(update_githound_account_mythology_style(conn, account$email, "egypt"), silent = TRUE)
    }
  }, ignoreInit = TRUE)

  observeEvent(input$style_norse, {
    apply_account_mythology_style("norse")
    if (nzchar(account$email %||% "")) {
      try(update_githound_account_mythology_style(conn, account$email, "norse"), silent = TRUE)
    }
  }, ignoreInit = TRUE)

  observeEvent(input$style_greece, {
    notify_user("Греческий стиль пока недоступен.", type = "message", duration = 4)
  }, ignoreInit = TRUE)

  observeEvent(input$submit_registration, {
    tryCatch({
      created <- register_githound_account(
        conn = conn,
        email = input$register_email,
        password = input$register_password,
        nickname = input$register_nickname,
        avatar_id = account$avatar_id
      )

      apply_account_row(created)
      load_user_archive()
      current_page("analysis")
      notify_user("Аккаунт сохранён в ClickHouse.", type = "message")
    }, error = function(e) {
      notify_user(conditionMessage(e), type = "error", duration = 7)
    })
  })

  observeEvent(input$submit_login, {
    tryCatch({
      logged_in <- login_githound_account(
        conn = conn,
        email = input$login_email,
        password = input$login_password
      )

      apply_account_row(logged_in)
      load_user_archive()
      if (isTRUE(input$remember_me)) {
        token_info <- issue_githound_remember_token(conn, logged_in$email[[1]], days = 30L)
        remember_token(token_info$token)
        session$sendCustomMessage("rememberToken", list(
          token = token_info$token,
          email = logged_in$email[[1]],
          maxAgeSeconds = 30L * 24L * 60L * 60L
        ))
      } else {
        if (nzchar(remember_token() %||% "")) {
          try(revoke_githound_remember_tokens(conn, token = remember_token()), silent = TRUE)
        }
        remember_token(NULL)
        session$sendCustomMessage("rememberToken", list(token = ""))
      }
      remember_attempted(TRUE)
      current_page("analysis")
      notify_user("Вход выполнен.", type = "message")
    }, error = function(e) {
      notify_user(conditionMessage(e), type = "error", duration = 7)
    })
  })

  observeEvent(input$github_login, {
    tryCatch({
      state <- githound_random_token(16L)
      redirect_uri <- github_oauth_redirect_uri(session)
      session$sendCustomMessage("startGithubOAuth", list(
        url = github_oauth_authorize_url(state, redirect_uri),
        state = state
      ))
    }, error = function(e) {
      notify_user(conditionMessage(e), type = "error", duration = 8)
    })
  }, ignoreInit = TRUE)

  observe({
    if (isTRUE(github_oauth_processed())) {
      return()
    }
    query <- parseQueryString(session$clientData$url_search %||% "")
    code <- trimws(query$code %||% "")
    state <- trimws(query$state %||% "")
    if (!nzchar(code)) {
      return()
    }
    if (!nzchar(account$email %||% "")) {
      current_page("boot")
    }

    if (is.null(input$github_oauth_state_cookie)) {
      return()
    }
    expected_state <- trimws(input$github_oauth_state_cookie %||% "")
    if (!nzchar(expected_state)) {
      return()
    }
    github_oauth_processed(TRUE)

    tryCatch({
      if (!identical(state, expected_state)) {
        stop("GitHub OAuth state не совпал. Попробуйте войти еще раз.", call. = FALSE)
      }

      github_user <- complete_github_oauth(code, github_oauth_redirect_uri(session))
      logged_in <- login_or_register_github_account(
        conn = conn,
        github_id = github_user$id,
        github_login = github_user$login,
        github_name = github_user$name,
        github_email = github_user$email,
        github_token = github_user$access_token,
        github_token_name = github_user$token_name,
        github_token_expires_at = github_user$token_expires_at,
        avatar_id = account$avatar_id
      )

      apply_account_row(logged_in)
      remember_attempted(TRUE)
      load_user_archive()
      current_page("analysis")
      session$sendCustomMessage("finishGithubOAuth", list())
      notify_user("Вход через GitHub выполнен.", type = "message")
    }, error = function(e) {
      session$sendCustomMessage("finishGithubOAuth", list())
      if (identical(current_page(), "boot")) {
        current_page("login")
      }
      notify_user(conditionMessage(e), type = "error", duration = 9)
    })
  })

  observeEvent(input$remember_token, {
    if (isTRUE(oauth_callback_pending())) {
      return()
    }
    if (!isTRUE(remember_input_seen())) {
      remember_input_seen(TRUE)
    }
    token <- trimws(input$remember_token %||% "")
    if (nzchar(account$email %||% "")) {
      if (nzchar(token)) remember_token(token)
      return()
    }
    if (isTRUE(remember_attempted()) && !identical(current_page(), "boot")) {
      if (nzchar(token)) remember_token(token) else remember_token(NULL)
      return()
    }
    if (!nzchar(token)) {
      remember_token(NULL)
      remember_attempted(TRUE)
      if (identical(current_page(), "boot")) {
        current_page("landing")
      }
      return()
    }
    remember_attempted(TRUE)

    tryCatch({
      remembered <- login_githound_account_by_remember_token(conn, token)
      apply_account_row(remembered)
      remember_token(token)
      load_user_archive()
      current_page("analysis")
    }, error = function(e) {
      remember_token(NULL)
      session$sendCustomMessage("rememberToken", list(token = ""))
      if (identical(current_page(), "boot")) {
        current_page("landing")
      }
    })
  }, ignoreInit = FALSE)

  observe({
    if (!identical(current_page(), "boot")) {
      return()
    }
    if (isTRUE(oauth_callback_pending())) {
      return()
    }
    invalidateLater(1800, session)
    if (isTRUE(remember_input_seen()) || isTRUE(remember_attempted()) || nzchar(account$email %||% "")) {
      return()
    }
    current_page("landing")
  })

  observeEvent(input$avatar_id, {
    account$avatar_id <- input$avatar_id
    avatars_open(FALSE)
  })

  observeEvent(input$run_analysis, {
    target <- trimws(input$analysis_user %||% "")
    if (!nzchar(target)) {
      notify_user("Не указана цель.", type = "error", duration = 6)
      return()
    }

    analysis_target(target)
    current_page("protocol")
  })

  observeEvent(input$run_isis, {
    launch_protocol("isis")
  })

  observeEvent(input$run_set, {
    launch_protocol("set")
  })

  observeEvent(input$run_quality, {
    launch_protocol("quality")
  })

  observeEvent(input$run_isis_after_report, {
    req(is.list(set_report()))
    launch_protocol("isis", target = set_report()$profile %||% analysis_target())
  })

  observeEvent(input$run_set_after_report, {
    req(is.list(set_report()))
    launch_protocol("set", target = set_report()$profile %||% analysis_target())
  })

  observeEvent(input$save_account, {
    req(account$email)
    tryCatch({
      submitted_token <- trimws(input$account_token %||% "")
      current_token <- trimws(account$github_token %||% "")
      if (isTRUE(account_github_token_expired()) && identical(submitted_token, current_token)) {
        notify_user("GitHoundToken истек. Войдите через GitHub еще раз или замените токен вручную.", type = "error", duration = 8)
        return()
      }
      updated <- update_githound_account_profile(
        conn = conn,
        email = account$email,
        github_token = submitted_token,
        avatar_id = input$avatar_id %||% account$avatar_id
      )

      apply_account_row(updated)
      notify_user("Профиль обновлен.", type = "message")
    }, error = function(e) {
      notify_user(conditionMessage(e), type = "error", duration = 7)
    })
  })

  output$account_token_status <- renderUI({
    token <- trimws(account$github_token %||% "")
    if (!nzchar(token)) {
      return(p(
        class = "account-copy",
        "GitHub-токен не сохранен. Войдите через GitHub, чтобы GitHoundToken появился автоматически."
      ))
    }

    token_name <- trimws(account$github_token_name %||% "")
    if (!nzchar(token_name)) {
      token_name <- "GitHub token"
    }
    expires_at <- parse_github_token_expires_at(account$github_token_expires_at)
    if (isTRUE(account_github_token_expired())) {
      return(p(
        class = "account-copy",
        paste0(token_name, " истек. Войдите через GitHub еще раз, чтобы выпустить новый токен.")
      ))
    }
    if (!is.na(expires_at)) {
      display_tz <- trimws(Sys.getenv("GITHOUND_RATE_LIMIT_TZ", unset = ""))
      if (!nzchar(display_tz)) {
        display_tz <- Sys.timezone()
      }
      if (!nzchar(display_tz) || is.na(display_tz) || !display_tz %in% OlsonNames()) {
        display_tz <- "UTC"
      }
      return(p(
        class = "account-copy",
        paste0(token_name, " действует до ", format(expires_at, "%Y-%m-%d %H:%M %Z", tz = display_tz), ".")
      ))
    }

    p(class = "account-copy", paste0(token_name, " сохранен без срока истечения в GitHound."))
  })

  output$github_rate_limit_ui <- renderUI({
    rate <- github_rate_limit()
    remaining <- suppressWarnings(as.integer(rate$remaining %||% NA_integer_))
    limit <- suppressWarnings(as.integer(rate$limit %||% NA_integer_))
    percent <- if (!is.na(remaining) && !is.na(limit) && limit > 0L) {
      max(0, min(100, round(remaining / limit * 100)))
    } else {
      0L
    }
    value <- if (!is.na(remaining) && !is.na(limit)) {
      paste0(remaining, " / ", limit)
    } else {
      "—"
    }
    reset <- suppressWarnings(as.numeric(rate$reset %||% NA_real_))
    display_tz <- trimws(Sys.getenv("GITHOUND_RATE_LIMIT_TZ", unset = ""))
    if (!nzchar(display_tz)) {
      display_tz <- Sys.timezone()
    }
    if (!nzchar(display_tz) || is.na(display_tz) || !display_tz %in% OlsonNames()) {
      display_tz <- "UTC"
    }
    reset_label <- if (!is.na(reset) && is.finite(reset) && reset > 0) {
      paste("сброс", format(as.POSIXct(reset, origin = "1970-01-01", tz = display_tz), "%H:%M %Z"))
    } else if (identical(rate$status, "error")) {
      "лимит недоступен"
    } else {
      "загрузка /rate_limit"
    }
    source_label <- if (identical(rate$source, "account")) "токен аккаунта" else "анонимно"
    resource_label <- trimws(as.character(rate$resource %||% "core"))
    if (!nzchar(resource_label) || is.na(resource_label)) {
      resource_label <- "core"
    }
    updated_at <- rate$updated_at %||% Sys.time()
    updated_label <- format(as.POSIXct(updated_at, tz = display_tz), "%H:%M:%S")
    title <- if (identical(rate$status, "error") && nzchar(rate$error %||% "")) {
      rate$error
    } else {
      paste("GitHub /rate_limit:", source_label, "resource", resource_label, "updated", updated_label)
    }

    div(
      class = "github-rate-card",
      title = title,
      div(
        class = "github-rate-top",
        span(class = "github-rate-title", "GitHub API"),
        span(class = "github-rate-value", value)
      ),
      div(
        class = "github-rate-track",
        style = paste0("--github-rate-percent: ", percent, "%;"),
        div(class = "github-rate-fill")
      ),
      div(class = "github-rate-meta", paste(reset_label, "·", source_label))
    )
  })

  output$profile_photo <- renderUI({
    div(class = "profile-photo", style = avatar_css(account$avatar_id %||% "egypt_1"))
  })

  output$mythology_style_picker <- renderUI({
    current <- mythology_style()
    div(
      class = "myth-style-picker",
      div(class = "myth-style-title", "Мифологический стиль"),
      div(
        class = "myth-style-buttons",
        actionButton(
          "style_egypt",
          "Египетский",
          class = paste("myth-style-button", if (identical(current, "egypt")) "active" else "")
        ),
        actionButton(
          "style_norse",
          "Скандинавский",
          class = paste("myth-style-button", if (identical(current, "norse")) "active" else "")
        ),
        actionButton(
          "style_greece",
          "Греческий",
          class = "myth-style-button disabled",
          title = "Будет добавлен позже"
        )
      )
    )
  })

  output$set_progress_ui <- renderUI({
    value <- max(0, min(100, as.integer(set_progress$value %||% 0)))
    label <- as.character(set_progress$label %||% "Запуск протокола")
    label_parts <- strsplit(label, "\n", fixed = TRUE)[[1]]
    stage_label <- label_parts[[1]] %||% ""
    working_label <- if (length(label_parts) > 1L) {
      paste(label_parts[-1], collapse = "\n")
    } else {
      ""
    }
    div(
      class = "set-progress-wrap",
      div(
        id = "set_progress_label",
        class = "set-progress-label",
        span(class = "set-progress-stage", stage_label),
        if (nzchar(working_label)) span(class = "set-progress-working", working_label)
      ),
      div(
        id = "set_progress_track",
        class = "set-progress-track",
        style = paste0("--set-progress: ", value, "%;"),
        div(class = "set-progress-fill"),
        img(src = "mini-horus.svg", class = "mini-horus", alt = "Мини-Гор"),
        img(src = "mini-mjolnir.svg", class = "mini-mjolnir", alt = "Мини-Мьёльнир")
      ),
      div(id = "set_progress_percent", class = "set-progress-percent", paste0(value, "%"))
    )
  })

  output$account_avatar_picker <- renderUI({
    if (!isTRUE(avatars_open())) {
      return(NULL)
    }

    div(
      class = "avatar-picker",
      radioButtons(
        "avatar_id",
        "Стандартное фото профиля",
        choiceNames = unname(Map(
          avatar_choice,
          avatar_options$id,
          avatar_options$title,
          avatar_options$description
        )),
        choiceValues = unname(avatar_options$id),
        selected = account$avatar_id %||% "egypt_1"
      )
    )
  })

  output$account_menu <- renderUI({
    if (is.null(account$email)) {
      return(NULL)
    }

    menu_items <- tagList(
      if (!identical(current_page(), "analysis")) {
        actionButton("menu_go_analysis", "Главная", class = "menu-button secondary-button")
      },
      actionButton("open_account", "Личный кабинет", class = "menu-button secondary-button"),
      actionButton("open_archive", "Архив", class = "menu-button secondary-button"),
      actionButton("logout_request", "Выход", class = "menu-button secondary-button")
    )

    div(
      class = paste("account-menu", if (isTRUE(menu_open())) "account-menu-open" else ""),
      actionButton("toggle_account_menu", "Меню", class = "account-menu-trigger"),
      div(
        class = "account-menu-panel",
        menu_items
      )
    )
  })

  output$page <- renderUI({
    if (identical(current_page(), "boot")) {
      boot_screen()
    } else if (identical(current_page(), "registration")) {
      registration_screen()
    } else if (identical(current_page(), "login")) {
      login_screen()
    } else if (identical(current_page(), "account")) {
      account_screen(
        nickname = account$nickname %||% "Профиль",
        selected_avatar = account$avatar_id %||% "egypt_1",
        github_token = account$github_token %||% ""
      )
    } else if (identical(current_page(), "analysis")) {
      analysis_screen()
    } else if (identical(current_page(), "protocol")) {
      protocol_screen(analysis_target() %||% "")
    } else if (identical(current_page(), "set_loading")) {
      set_loading_screen(analysis_target() %||% "", active_protocol())
    } else if (identical(current_page(), "set_report")) {
      report <- set_report()
      if (is.list(report)) {
        report$view_theme <- theme_mode()
      }
      set_report_screen(report)
    } else if (identical(current_page(), "archive")) {
      records <- report_archive()
      max_page <- max(1L, ceiling(length(records) / 15L))
      page <- min(max(1L, archive_page()), max_page)
      archive_screen(records, page = page)
    } else {
      landing_screen()
    }
  })
}

shinyApp(ui = ui, server = server)
