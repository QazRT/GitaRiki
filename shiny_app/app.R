library(shiny)

source_candidates <- c(
  file.path(getwd(), "account_storage.R"),
  file.path(getwd(), "shiny_app", "account_storage.R")
)
source_path <- source_candidates[file.exists(source_candidates)][[1]]
source(source_path, encoding = "UTF-8")

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
      actionButton("submit_login", "Войти", class = "run-button"),
      actionButton("back_to_landing_login", "Назад", class = "menu-button secondary-button")
    )
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
    )
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
      )
    )
  )
}

account_screen <- function(nickname = "Профиль", selected_avatar = "egypt_1") {
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
      passwordInput("account_token", "GitHub токен", placeholder = "Введите токен для анализа"),
      uiOutput("account_avatar_picker"),
      actionButton("save_account", "Сохранить профиль", class = "run-button"),
      actionButton("go_analysis", "Перейти к анализу", class = "menu-button secondary-button")
    )
  )
}

ui <- fluidPage(
  tags$head(
    tags$title("GitHound"),
    tags$script(HTML("
      $(document).on('click', '#theme_hell', function() {
        document.body.classList.remove('heaven-theme');
        $('#theme_hell').addClass('active').attr('aria-pressed', 'true');
        $('#theme_heaven').removeClass('active').attr('aria-pressed', 'false');
      });

      $(document).on('click', '#theme_heaven', function() {
        document.body.classList.add('heaven-theme');
        $('#theme_heaven').addClass('active').attr('aria-pressed', 'true');
        $('#theme_hell').removeClass('active').attr('aria-pressed', 'false');
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
      }

      html,
      body {
        min-height: 100%;
        background:
          radial-gradient(circle at 50% 18%, rgba(255, 59, 31, 0.2), transparent 28%),
          radial-gradient(circle at 18% 78%, rgba(143, 0, 8, 0.24), transparent 31%),
          radial-gradient(circle at 86% 62%, rgba(92, 0, 6, 0.24), transparent 29%),
          linear-gradient(180deg, #180304 0%, #080101 48%, #020000 100%),
          var(--page-bg);
        color: var(--ink);
        font-family: Arial, Helvetica, sans-serif;
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
        background:
          radial-gradient(circle at 50% 12%, rgba(255, 229, 153, 0.64), transparent 30%),
          radial-gradient(circle at 16% 76%, rgba(255, 255, 255, 0.82), transparent 34%),
          radial-gradient(circle at 84% 66%, rgba(142, 204, 227, 0.28), transparent 32%),
          linear-gradient(180deg, #fffefe 0%, #eefaff 48%, #fff1bd 100%);
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
          linear-gradient(rgba(255, 43, 52, 0.05) 1px, transparent 1px),
          linear-gradient(90deg, rgba(255, 43, 52, 0.035) 1px, transparent 1px);
        background-size: 28px 28px;
        opacity: 0.38;
      }

      body.heaven-theme::before {
        background:
          linear-gradient(rgba(125, 203, 235, 0.12) 1px, transparent 1px),
          linear-gradient(90deg, rgba(217, 165, 46, 0.08) 1px, transparent 1px);
        background-size: 30px 30px;
        opacity: 0.28;
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
        transition: opacity 160ms ease, transform 160ms ease;
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
        margin: 0 auto 16px;
        filter: drop-shadow(7px 7px 0 rgba(143, 0, 8, 0.72))
          drop-shadow(-5px 4px 0 rgba(255, 59, 31, 0.32));
      }

      .logo-wrap::before,
      .logo-wrap::after {
        content: '';
        position: absolute;
        inset: 0;
        background: url('githound-hell-logo.png') center / cover no-repeat;
        mix-blend-mode: screen;
        pointer-events: none;
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
        position: relative;
        z-index: 1;
        display: block;
        width: 100%;
        height: auto;
        border: 2px solid rgba(255, 43, 52, 0.5);
        border-radius: 8px;
        box-shadow: 0 0 0 1px rgba(255, 43, 52, 0.42),
          0 18px 70px rgba(224, 24, 36, 0.28);
      }

      .heaven-logo {
        display: none;
      }

      body.heaven-theme .hell-logo {
        display: none;
      }

      body.heaven-theme .heaven-logo {
        display: block;
      }

      body.heaven-theme .logo {
        border-color: rgba(125, 203, 235, 0.62);
        box-shadow: 0 0 0 1px rgba(217, 165, 46, 0.36),
          0 20px 70px rgba(125, 203, 235, 0.26);
      }

      .project-title {
        position: relative;
        margin: 0 0 24px;
        font-family: Impact, 'Arial Black', sans-serif;
        font-size: clamp(36px, 6.4vw, 56px);
        line-height: 0.96;
        font-weight: 900;
        letter-spacing: 1px;
        text-transform: uppercase;
        color: var(--ink);
        text-shadow: 0 0 20px rgba(255, 43, 52, 0.62), 0 3px 0 var(--blood);
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
      }

      .account-copy {
        margin: 6px 0 0;
        color: var(--muted);
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
      }

      .protocol-image {
        width: min(100%, 340px);
        aspect-ratio: 1;
        object-fit: cover;
        border: 2px solid var(--line);
        border-radius: 8px;
        box-shadow: 0 16px 42px rgba(0, 0, 0, 0.34);
      }

      .protocol-card .run-button {
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
      }

      .avatar-desc {
        display: block;
        color: var(--muted);
        font-size: 13px;
        line-height: 1.35;
        text-align: left;
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
      }

      .form-control {
        height: 48px;
        border: 1px solid var(--line);
        border-radius: 8px;
        background: var(--field-bg);
        box-shadow: inset 3px 0 0 rgba(224, 24, 36, 0.8);
        color: var(--ink);
        font-size: 16px;
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

        .account-portrait {
          grid-template-columns: 1fr;
          justify-items: center;
          text-align: center;
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
  current_page <- reactiveVal("landing")
  avatars_open <- reactiveVal(FALSE)
  analysis_target <- reactiveVal(NULL)
  account <- reactiveValues(
    email = NULL,
    nickname = NULL,
    avatar_id = "egypt_1"
  )

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
    group <- avatar_options$group[match(account$avatar_id %||% "egypt_1", avatar_options$id)]
    archive_message <- switch(
      group %||% "egypt",
      egypt = "Тот еще собирает свои свитки",
      norse = "Вороны еще не успели вернуться к одину",
      greece = "Афина еще не готова вас просвещать",
      "Архив еще собирается"
    )
    showNotification(archive_message, type = "message", duration = 6)
  })

  observeEvent(input$toggle_avatars, {
    avatars_open(!isTRUE(avatars_open()))
  })

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
      current_page("analysis")
      showNotification("Аккаунт сохранён в ClickHouse.", type = "message")
    }, error = function(e) {
      showNotification(conditionMessage(e), type = "error", duration = 7)
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
      current_page("analysis")
      showNotification("Вход выполнен.", type = "message")
    }, error = function(e) {
      showNotification(conditionMessage(e), type = "error", duration = 7)
    })
  })

  observeEvent(input$avatar_id, {
    account$avatar_id <- input$avatar_id
    avatars_open(FALSE)
  })

  observeEvent(input$run_analysis, {
    target <- trimws(input$analysis_user %||% "")
    if (!nzchar(target)) {
      showNotification("Не указана цель.", type = "error", duration = 6)
      return()
    }

    analysis_target(target)
    current_page("protocol")
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
      showNotification("Профиль обновлен.", type = "message")
    }, error = function(e) {
      showNotification(conditionMessage(e), type = "error", duration = 7)
    })
  })

  output$profile_photo <- renderUI({
    div(class = "profile-photo", style = avatar_css(account$avatar_id %||% "egypt_1"))
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
        selected_avatar = account$avatar_id %||% "egypt_1"
      )
    } else if (identical(current_page(), "analysis")) {
      analysis_screen()
    } else if (identical(current_page(), "protocol")) {
      protocol_screen(analysis_target() %||% "")
    } else {
      landing_screen()
    }
  })
}

shinyApp(ui = ui, server = server)
