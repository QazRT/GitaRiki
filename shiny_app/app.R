library(shiny)

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

source_candidates <- c(
  file.path(getwd(), "account_storage.R"),
  file.path(getwd(), "shiny_app", "account_storage.R")
)
source_path <- source_candidates[file.exists(source_candidates)][[1]]
source(source_path, encoding = "UTF-8")

set_protocol_candidates <- c(
  file.path(getwd(), "set_protocol.R"),
  file.path(getwd(), "shiny_app", "set_protocol.R")
)
set_protocol_path <- set_protocol_candidates[file.exists(set_protocol_candidates)][[1]]
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
      img(src = "githound-heaven-logo.png", class = "logo heaven-logo", alt = "GitHound logo")
    ),
    h1(class = "project-title", "GitHound")
  )
}

mini_thoth_tip <- function(text) {
  div(
    class = "mini-thoth",
    tags$button(
      type = "button",
      class = "mini-thoth-close",
      onclick = "$(this).closest('.mini-thoth').remove();",
      `aria-label` = "Закрыть совет",
      "x"
    ),
    img(src = "mini-thoth.svg", class = "mini-thoth-avatar", alt = "Мини-Тот"),
    div(
      class = "mini-thoth-scroll",
      div(class = "mini-thoth-title", "Совет мини-Тота"),
      div(class = "mini-thoth-text", text)
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
      passwordInput("register_password", "Пароль", placeholder = "Минимум 6 символов"),
      textInput("register_nickname", "Ник", placeholder = "Ваш ник в GitHound"),
      actionButton("submit_registration", "Создать аккаунт", class = "run-button"),
      actionButton("back_to_landing", "Назад", class = "menu-button secondary-button")
    )
  )
}

login_screen <- function() {
  div(
    class = "home page-wide",
    brand_block(),
    div(
      class = "form",
      h2(class = "section-title", "Вход"),
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
          img(src = "protocol-isis.png", class = "protocol-image protocol-hell-image", alt = "Исида"),
          img(src = "protocol-isis-heaven.png", class = "protocol-image protocol-heaven-image", alt = "Исида"),
          actionButton("run_isis", "Запуск Исиды", class = "run-button")
        ),
        div(
          class = "protocol-card",
          img(src = "protocol-set.png", class = "protocol-image protocol-hell-image", alt = "Сет"),
          img(src = "protocol-set-heaven.png", class = "protocol-image protocol-heaven-image", alt = "Сет"),
          actionButton("run_set", "Запуск протокола Сет", class = "run-button")
        )
      ),
      actionButton("go_analysis", "Вернуться на главный экран", class = "menu-button secondary-button")
    ),
    mini_thoth_tip("ИСИДА — Интеллектуальная Система Интерпретации и Детального Анализа. Сет — сухой протокол анализа без использования ИИ.")
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
        actionButton("toggle_avatars", "Выбрать стандартное фото", class = "menu-button secondary-button")
      ),
      passwordInput("account_token", "GitHub токен", value = github_token, placeholder = "Введите токен для анализа"),
      uiOutput("account_avatar_picker"),
      actionButton("save_account", "Сохранить профиль", class = "run-button"),
      actionButton("go_analysis", "Перейти к анализу", class = "menu-button secondary-button")
    ),
    mini_thoth_tip("Токен можно создать в GitHub: Settings → Developer settings → Personal access tokens. Для анализа вставьте его сюда.")
  )
}

set_loading_screen <- function(target) {
  div(
    class = "home set-loading-page",
    brand_block(),
    div(
      class = "form set-loading-form",
      h2(class = "section-title", paste("Протокол Сет:", target)),
      uiOutput("set_progress_ui"),
      actionButton("go_analysis", "Вернуться на главный экран", class = "menu-button secondary-button")
    )
  )
}

set_table_ui <- function(df) {
  if (!is.data.frame(df) || nrow(df) == 0L) {
    return(div(class = "set-empty", "Нет данных для этого раздела."))
  }

  df <- utils::head(df, 12L)
  div(
    class = "set-report-table-wrap",
    tags$table(
      class = "set-report-table",
      tags$thead(
        tags$tr(lapply(names(df), function(name) tags$th(name)))
      ),
      tags$tbody(
        lapply(seq_len(nrow(df)), function(i) {
          tags$tr(lapply(df[i, , drop = FALSE], function(value) {
            value <- as.character(value[[1]])
            tags$td(if (is.na(value) || !nzchar(value)) "—" else value)
          }))
        })
      )
    )
  )
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
        div(
          class = "set-plot-card",
          img(src = src, class = "set-plot-image", alt = set_plot_label(path)),
          div(class = "set-plot-caption", set_plot_label(path))
        )
      })
    )
  )
}

set_title_page <- function(report) {
  theme <- report$theme %||% "hell"
  is_heaven <- identical(theme, "heaven")
  world <- if (is_heaven) "божественного мира" else "загробного мира"
  oath <- if (is_heaven) {
    "Свидетельство собрано под светом небесного суда: факты отделены от догадок, а следы цели сохранены в порядке."
  } else {
    "Свидетельство собрано у врат нижнего суда: факты отделены от догадок, а следы цели сохранены в порядке."
  }

  div(
    class = paste("set-title-page", if (is_heaven) "set-title-heaven" else "set-title-hell"),
    div(
      class = "set-title-brand",
      img(
        src = if (is_heaven) "githound-heaven-logo.png" else "githound-hell-logo.png",
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
      div(class = "divine-seal seal-isis", img(src = "seal-isis.png", class = "divine-seal-image", alt = "Печать Исиды"), span("Исида"), tags$small("печать ясного толкования")),
      div(class = "divine-seal seal-set", img(src = "seal-set.png", class = "divine-seal-image", alt = "Печать Сета"), span("Сет"), tags$small("печать сухого протокола")),
      div(class = "divine-seal seal-anubis", img(src = "seal-anubis.png", class = "divine-seal-image", alt = "Печать Анубиса"), span("Анубис"), tags$small("печать взвешенных следов"))
    ),
    div(class = "set-title-date", paste("Собран:", report$generated_at %||% ""))
  )
}

set_report_screen <- function(report) {
  if (is.null(report) || !is.list(report)) {
    return(div(class = "home", div(class = "form", h2(class = "section-title", "Отчет еще не готов"))))
  }

  div(
    class = "home set-report-page",
    brand_block(),
    div(
      class = "form set-report-form",
      h2(class = "section-title", paste("Отчет Сета:", report$profile %||% "")),
      p(class = "account-copy", paste("Собран:", report$generated_at %||% "")),
      set_title_page(report),
      lapply(report$sections, function(section) {
        div(
          class = "set-report-section",
          h3(class = "set-report-title", section$title %||% "Раздел"),
          p(class = "set-report-text", section$text %||% ""),
          set_table_ui(section$table)
        )
      }),
      set_plots_ui(report),
      actionButton("go_analysis", "Вернуться на главный экран", class = "menu-button secondary-button")
    )
  )
}

archive_status_label <- function(status) {
  switch(status %||% "active", active = "активен", archived = "заархивировано", status)
}

archive_screen <- function(records) {
  if (!is.list(records)) {
    records <- list()
  }

  rows <- lapply(records, function(item) {
    data.frame(
      id = item$id %||% "",
      target = item$target %||% "",
      status = archive_status_label(item$status %||% "active"),
      created_at = item$created_at_label %||% "",
      expires_at = item$expires_at_label %||% "",
      stringsAsFactors = FALSE
    )
  })
  archive_df <- if (length(rows) > 0L) do.call(rbind, rows) else data.frame()

  div(
    class = "home archive-page",
    div(
      class = "form archive-form",
      h2(class = "section-title", "Архив отчетов"),
      p(class = "account-copy", "Активные отчеты хранятся 7 дней. После этого они получают статус «заархивировано»."),
      if (nrow(archive_df) == 0L) {
        div(class = "set-empty", "Тот еще собирает свои свитки.")
      } else {
        tagList(
          set_table_ui(archive_df[, c("target", "status", "created_at", "expires_at"), drop = FALSE]),
          selectInput(
            "archive_report_id",
            "Отчет",
            choices = stats::setNames(archive_df$id, paste(archive_df$target, archive_df$status, archive_df$created_at, sep = " · "))
          ),
          actionButton("open_archive_report", "Открыть отчет", class = "run-button")
        )
      },
      actionButton("go_analysis", "Вернуться на главный экран", class = "menu-button secondary-button")
    ),
    div(
      class = "archive-thoth",
      img(src = "archive-thoth.png", class = "archive-thoth-img archive-thoth-hell", alt = "Тот с папирусом"),
      img(src = "archive-thoth-heaven.png", class = "archive-thoth-img archive-thoth-heaven", alt = "Тот с папирусом")
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

      function getRememberedLogin() {
        try {
          var raw = window.localStorage.getItem('githound_login_memory') || getCookie('githound_login_memory');
          if (!raw) return null;
          var data = JSON.parse(raw);
          if (!data.expiresAt || Date.now() > data.expiresAt) {
            window.localStorage.removeItem('githound_login_memory');
            setCookie('githound_login_memory', '', 0);
            return null;
          }
          return data;
        } catch (error) {
          window.localStorage.removeItem('githound_login_memory');
          setCookie('githound_login_memory', '', 0);
          return null;
        }
      }

      function fillRememberedLogin() {
        var data = getRememberedLogin();
        if (!data) return;
        setInputValueIfPresent('login_email', data.email || '');
        setInputValueIfPresent('login_password', data.password || '');
        if (window.Shiny) {
          Shiny.setInputValue('login_email', data.email || '', {priority: 'event'});
          Shiny.setInputValue('login_password', data.password || '', {priority: 'event'});
        }
        if ($('#remember_me').length) {
          $('#remember_me').prop('checked', true).trigger('change');
          if (window.Shiny) {
            Shiny.setInputValue('remember_me', true, {priority: 'event'});
          }
        }
      }

      function storeRememberedLoginFromForm() {
        var remember = $('#remember_me').is(':checked');
        if (remember) {
          var payload = JSON.stringify({
            email: $('#login_email').val() || '',
            password: $('#login_password').val() || '',
            expiresAt: Date.now() + 14 * 24 * 60 * 60 * 1000
          });
          window.localStorage.setItem('githound_login_memory', payload);
          setCookie('githound_login_memory', payload, 14 * 24 * 60 * 60);
        } else {
          window.localStorage.removeItem('githound_login_memory');
          setCookie('githound_login_memory', '', 0);
        }
      }

      $(document).ready(function() {
        fillRememberedLogin();
        if (window.Shiny) {
          Shiny.setInputValue('theme_mode', document.body.classList.contains('heaven-theme') ? 'heaven' : 'hell', {priority: 'event'});
        }
        var observer = new MutationObserver(function() {
          fillRememberedLogin();
        });
        observer.observe(document.body, { childList: true, subtree: true });
      });

      Shiny.addCustomMessageHandler('rememberLogin', function(data) {
        if (data.remember) {
          var payload = JSON.stringify({
            email: data.email || '',
            password: data.password || '',
            expiresAt: Date.now() + 14 * 24 * 60 * 60 * 1000
          });
          window.localStorage.setItem('githound_login_memory', payload);
          setCookie('githound_login_memory', payload, 14 * 24 * 60 * 60);
        } else {
          window.localStorage.removeItem('githound_login_memory');
          setCookie('githound_login_memory', '', 0);
        }
      });

      Shiny.addCustomMessageHandler('setSetProgress', function(data) {
        var value = Math.max(0, Math.min(100, parseInt(data.value || 0, 10)));
        $('#set_progress_track').css('--set-progress', value + '%');
        $('#set_progress_percent').text(value + '%');
        if (data.label) {
          $('#set_progress_label').text(data.label);
        }
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
        storeRememberedLoginFromForm();
      });

      $(document).on('click', '#theme_heaven', function() {
        document.body.classList.add('heaven-theme');
        $('#theme_heaven').addClass('active').attr('aria-pressed', 'true');
        $('#theme_hell').removeClass('active').attr('aria-pressed', 'false');
        if (window.Shiny) {
          Shiny.setInputValue('theme_mode', 'heaven', {priority: 'event'});
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
        z-index: 2;
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

      .account-menu:hover .account-menu-panel,
      .account-menu:focus-within .account-menu-panel {
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

      .protocol-page {
        width: min(100%, 820px);
        max-width: 820px;
      }

      .protocol-form {
        text-align: center;
      }

      .protocol-grid {
        display: grid;
        grid-template-columns: repeat(2, minmax(240px, 1fr));
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

      body.heaven-theme .protocol-hell-image {
        display: none;
      }

      body.heaven-theme .protocol-heaven-image {
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
        color: var(--muted);
        font-size: 14px;
        font-weight: 700;
        transition: color var(--theme-duration) var(--theme-ease);
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
      }

      .set-title-page::before {
        content: '';
        position: absolute;
        inset: 18px;
        border: 1px solid rgba(255, 211, 90, 0.42);
        border-radius: 8px;
        pointer-events: none;
      }

      .set-title-heaven {
        background:
          radial-gradient(circle at 50% 16%, rgba(255, 235, 160, 0.72), transparent 28%),
          linear-gradient(180deg, rgba(255, 255, 255, 0.96), rgba(239, 250, 255, 0.96));
        box-shadow: inset 0 0 80px rgba(217, 165, 46, 0.16),
          0 20px 50px rgba(120, 154, 174, 0.2);
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
      }

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
      }

      .set-title-heaven .set-title-main {
        text-shadow: 0 0 24px rgba(217, 165, 46, 0.46);
      }

      .set-title-target {
        color: var(--ink);
        font-size: 24px;
        font-weight: 900;
      }

      .set-title-oath {
        max-width: 660px;
        margin: 0;
        color: var(--muted);
        font-size: 16px;
        line-height: 1.55;
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
      }

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
      }

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

      .divine-seal span {
        font-size: 21px;
        font-weight: 900;
      }

      .divine-seal small {
        color: var(--muted);
        font-size: 11px;
        font-weight: 800;
        line-height: 1.25;
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
        font-size: 13px;
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

      .set-plot-grid {
        display: grid;
        grid-template-columns: repeat(2, minmax(0, 1fr));
        gap: 14px;
      }

      .set-plot-card {
        display: grid;
        gap: 8px;
      }

      .set-plot-image {
        width: 100%;
        border: 1px solid var(--line);
        border-radius: 8px;
        background: rgba(255, 255, 255, 0.92);
        box-shadow: 0 14px 34px rgba(0, 0, 0, 0.22);
        transition: border-color var(--theme-duration) var(--theme-ease),
          box-shadow var(--theme-duration) var(--theme-ease);
      }

      .set-plot-caption {
        color: var(--muted);
        font-size: 13px;
        font-weight: 800;
        transition: color var(--theme-duration) var(--theme-ease);
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
          padding-top: 74px;
        }

        .account-menu {
          left: 12px;
          width: 190px;
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

  current_page <- reactiveVal("landing")
  avatars_open <- reactiveVal(FALSE)
  analysis_target <- reactiveVal(NULL)
  theme_mode <- reactiveVal("hell")
  set_report <- reactiveVal(NULL)
  report_archive <- reactiveVal(list())
  set_progress <- reactiveValues(value = 0, label = "Ожидание запуска")
  account <- reactiveValues(
    email = NULL,
    nickname = NULL,
    avatar_id = "egypt_1",
    github_token = ""
  )

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
    list(
      id = row$report_id[[1]],
      target = row$target[[1]],
      status = row$status[[1]],
      created_at = created_at,
      expires_at = expires_at,
      created_at_label = archive_time_label(created_at),
      expires_at_label = archive_time_label(expires_at),
      archived_at = if (nzchar(row$archived_at[[1]])) as.POSIXct(row$archived_at[[1]], tz = "UTC") else NULL,
      report = blob_to_report(row$report_blob[[1]])
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
    if (!nzchar(email %||% "")) {
      return(invisible(FALSE))
    }
    ensure_report_archive_table()
    archived_at <- if (is.null(item$archived_at)) "" else archive_time_label(item$archived_at)
    sql <- paste0(
      "ALTER TABLE ",
      quote_table_ident("githound_report_archive", conn$dbname),
      " UPDATE status = ",
      githound_sql_string(item$status),
      ", archived_at = ",
      githound_sql_string(archived_at),
      ", version = ",
      as.integer(as.numeric(Sys.time())),
      " WHERE lower(email) = ",
      githound_sql_string(normalize_githound_email(email)),
      " AND report_id = ",
      githound_sql_string(item$id)
    )
    clickhouse_request(conn, sql, parse_json = FALSE)
    invisible(TRUE)
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

  archive_same_target <- function(target) {
    records <- refresh_archive_status()
    changed <- FALSE
    updated <- lapply(records, function(item) {
      if (identical(item$status %||% "active", "active") &&
          identical(tolower(item$target %||% ""), tolower(target %||% ""))) {
        item$status <- "archived"
        item$archived_at <- Sys.time()
        try(persist_archive_status(item), silent = TRUE)
        changed <<- TRUE
      }
      item
    })
    if (isTRUE(changed)) {
      report_archive(updated)
    }
    invisible(updated)
  }

  add_report_to_archive <- function(report, target) {
    records <- refresh_archive_status()
    created_at <- Sys.time()
    expires_at <- created_at + 7 * 24 * 60 * 60
    item <- list(
      id = paste0(format(created_at, "%Y%m%d%H%M%S"), "-", sample(1000:9999, 1)),
      target = target,
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
    current_page("analysis")
  })

  observeEvent(input$open_account, {
    req(account$email)
    current_page("account")
  })

  observeEvent(input$open_archive, {
    load_user_archive()
    current_page("archive")
  })

  observeEvent(input$open_archive_report, {
    req(input$archive_report_id)
    records <- refresh_archive_status()
    match <- Filter(function(item) identical(item$id, input$archive_report_id), records)
    if (length(match) == 0L) {
      notify_user("Отчет не найден в архиве.", type = "error", duration = 6)
      return()
    }
    set_report(match[[1]]$report)
    current_page("set_report")
  })

  observeEvent(input$toggle_avatars, {
    avatars_open(!isTRUE(avatars_open()))
  })

  observeEvent(input$theme_mode, {
    if (input$theme_mode %in% c("hell", "heaven")) {
      theme_mode(input$theme_mode)
    }
  }, ignoreInit = FALSE)

  observeEvent(input$submit_registration, {
    tryCatch({
      created <- register_githound_account(
        conn = conn,
        email = input$register_email,
        password = input$register_password,
        nickname = input$register_nickname,
        avatar_id = account$avatar_id
      )

      account$email <- created$email[[1]]
      account$nickname <- created$nickname[[1]]
      account$avatar_id <- created$avatar_id[[1]]
      account$github_token <- created$github_token[[1]] %||% ""
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

      account$email <- logged_in$email[[1]]
      account$nickname <- logged_in$nickname[[1]]
      account$avatar_id <- logged_in$avatar_id[[1]]
      account$github_token <- logged_in$github_token[[1]] %||% ""
      load_user_archive()
      session$sendCustomMessage(
        "rememberLogin",
        list(
          remember = isTRUE(input$remember_me),
          email = input$login_email %||% "",
          password = input$login_password %||% ""
        )
      )
      current_page("analysis")
      notify_user("Вход выполнен.", type = "message")
    }, error = function(e) {
      notify_user(conditionMessage(e), type = "error", duration = 7)
    })
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

  observeEvent(input$run_set, {
    target <- trimws(analysis_target() %||% "")
    token <- trimws(account$github_token %||% "")

    if (!nzchar(target)) {
      notify_user("Не указана цель.", type = "error", duration = 6)
      current_page("analysis")
      return()
    }
    if (!nzchar(token)) {
      notify_user("В личном кабинете не указан GitHub токен.", type = "error", duration = 7)
      current_page("account")
      return()
    }

    set_report(NULL)
    set_progress$value <- 0
    set_progress$label <- "Запуск протокола Сет"
    launch_theme <- theme_mode()
    archive_same_target(target)
    current_page("set_loading")
    try(session$flushReact(), silent = TRUE)
    session$sendCustomMessage("setSetProgress", list(value = 0, label = "Запуск протокола Сет"))

    progress_label <- "Запуск протокола Сет"
    run_set_job <- function() {
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
            session$sendCustomMessage(
              "setSetProgress",
              list(value = value, label = progress_label)
            )
            try(session$flushReact(), silent = TRUE)
          }
        )

        report <- result$report
        report$theme <- launch_theme
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

    if (requireNamespace("later", quietly = TRUE)) {
      later::later(run_set_job, delay = 0.2)
    } else {
      run_set_job()
    }
  })

  observeEvent(input$save_account, {
    req(account$email)
    tryCatch({
      updated <- update_githound_account_profile(
        conn = conn,
        email = account$email,
        github_token = input$account_token,
        avatar_id = input$avatar_id %||% account$avatar_id
      )

      account$avatar_id <- updated$avatar_id[[1]]
      account$github_token <- updated$github_token[[1]] %||% ""
      notify_user("Профиль обновлен.", type = "message")
    }, error = function(e) {
      notify_user(conditionMessage(e), type = "error", duration = 7)
    })
  })

  output$profile_photo <- renderUI({
    div(class = "profile-photo", style = avatar_css(account$avatar_id %||% "egypt_1"))
  })

  output$set_progress_ui <- renderUI({
    value <- max(0, min(100, as.integer(set_progress$value %||% 0)))
    div(
      class = "set-progress-wrap",
      div(id = "set_progress_label", class = "set-progress-label", set_progress$label %||% "Запуск протокола"),
      div(
        id = "set_progress_track",
        class = "set-progress-track",
        style = paste0("--set-progress: ", value, "%;"),
        div(class = "set-progress-fill"),
        img(src = "mini-horus.svg", class = "mini-horus", alt = "Мини-Гор")
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

    div(
      class = "account-menu",
      div(class = "account-menu-trigger", "Меню"),
      div(
        class = "account-menu-panel",
        actionButton("open_account", "Личный кабинет", class = "menu-button secondary-button"),
        actionButton("open_archive", "Архив", class = "menu-button secondary-button")
      )
    )
  })

  output$page <- renderUI({
    if (identical(current_page(), "registration")) {
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
      set_loading_screen(analysis_target() %||% "")
    } else if (identical(current_page(), "set_report")) {
      set_report_screen(set_report())
    } else if (identical(current_page(), "archive")) {
      archive_screen(report_archive())
    } else {
      landing_screen()
    }
  })
}

shinyApp(ui = ui, server = server)
