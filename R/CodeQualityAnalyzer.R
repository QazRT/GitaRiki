`%||%` <- function(x, y) {
  if (is.null(x) || length(x) == 0L) {
    return(y)
  }
  if (is.atomic(x) && length(x) == 1L && is.na(x)) {
    return(y)
  }
  x
}

quality_env_int <- function(name, default, min_value = 1L) {
  value <- suppressWarnings(as.integer(Sys.getenv(name, "")))
  if (is.na(value)) {
    value <- as.integer(default)
  }
  max(as.integer(min_value), value)
}

quality_env_flag <- function(name, default = FALSE) {
  raw <- tolower(trimws(Sys.getenv(name, if (isTRUE(default)) "true" else "false")))
  raw %in% c("1", "true", "yes", "y", "on")
}

quality_env_num <- function(name, default, min_value = 0, max_value = Inf) {
  value <- suppressWarnings(as.numeric(Sys.getenv(name, "")))
  if (is.na(value)) {
    value <- as.numeric(default)
  }
  max(min_value, min(max_value, value))
}

quality_extract_username <- function(input) {
  input <- trimws(as.character(input %||% ""))
  if (!nzchar(input)) {
    return(NULL)
  }
  if (exists("extract_github_username", mode = "function")) {
    out <- tryCatch(extract_github_username(input), error = function(e) NULL)
    if (!is.null(out) && nzchar(out)) {
      return(out)
    }
  }
  if (grepl("^[A-Za-z0-9_-]+$", input)) {
    return(input)
  }
  if (requireNamespace("httr", quietly = TRUE)) {
    parsed <- httr::parse_url(input)
    if (!is.null(parsed$path)) {
      parts <- strsplit(gsub("^/|/$", "", parsed$path), "/", fixed = FALSE)[[1]]
      if (length(parts) >= 1L && nzchar(parts[[1L]])) {
        return(parts[[1L]])
      }
    }
  }
  NULL
}

quality_repo_parts <- function(full_name) {
  parts <- strsplit(as.character(full_name %||% ""), "/", fixed = TRUE)[[1]]
  if (length(parts) < 2L) {
    return(NULL)
  }
  list(owner = parts[[1L]], repo = parts[[2L]])
}

quality_num <- function(x, default = 0) {
  out <- suppressWarnings(as.numeric(x))
  out[is.na(out)] <- default
  out
}

quality_chr <- function(x, default = "") {
  out <- as.character(x %||% default)
  out[is.na(out)] <- default
  out
}

quality_score <- function(value) {
  max(0, min(100, round(as.numeric(value), 1)))
}

quality_compact_text <- function(x, max_chars = 12000L) {
  text <- paste(as.character(x %||% ""), collapse = "\n")
  text <- gsub("\r\n?", "\n", text)
  text <- gsub("[\\x00-\\x08\\x0B\\x0C\\x0E-\\x1F]", " ", text, perl = TRUE)
  text <- trimws(text)
  if (nchar(text, type = "bytes") > max_chars) {
    text <- paste0(substr(text, 1L, max_chars), "\n...[truncated]")
  }
  text
}

quality_json <- function(x) {
  jsonlite::toJSON(
    x,
    auto_unbox = TRUE,
    null = "null",
    na = "null",
    dataframe = "rows",
    digits = NA
  )
}

quality_fetch_repositories <- function(username, token = NULL) {
  if (!requireNamespace("gh", quietly = TRUE)) {
    stop("Package 'gh' is required for code quality analysis.", call. = FALSE)
  }

  repo_to_rows <- function(repos) {
    if (length(repos) == 0L) {
      return(data.frame())
    }
    rows <- lapply(repos, function(repo) {
      topics <- repo$topics %||% character()
      data.frame(
        full_name = repo$full_name %||% NA_character_,
        owner_login = repo$owner$login %||% NA_character_,
        name = repo$name %||% NA_character_,
        html_url = repo$html_url %||% NA_character_,
        description = repo$description %||% NA_character_,
        private = repo$private %||% NA,
        fork = repo$fork %||% NA,
        archived = repo$archived %||% NA,
        disabled = repo$disabled %||% NA,
        language = repo$language %||% NA_character_,
        stargazers_count = repo$stargazers_count %||% 0,
        forks_count = repo$forks_count %||% 0,
        open_issues_count = repo$open_issues_count %||% 0,
        size = repo$size %||% 0,
        default_branch = repo$default_branch %||% "main",
        created_at = repo$created_at %||% NA_character_,
        updated_at = repo$updated_at %||% NA_character_,
        pushed_at = repo$pushed_at %||% NA_character_,
        topics = paste(as.character(topics), collapse = ", "),
        stringsAsFactors = FALSE
      )
    })
    do.call(rbind, rows)
  }

  public_repos <- gh::gh(
    "GET /users/{username}/repos",
    username = username,
    type = "owner",
    sort = "pushed",
    direction = "desc",
    .limit = Inf,
    .token = token,
    .progress = FALSE
  )
  repos_df <- repo_to_rows(public_repos)

  include_private <- quality_env_flag("GITHOUND_QUALITY_INCLUDE_PRIVATE", default = TRUE)
  if (include_private && !is.null(token) && nzchar(token)) {
    private_repos <- tryCatch(
      gh::gh(
        "GET /user/repos",
        visibility = "all",
        affiliation = "owner,collaborator,organization_member",
        sort = "pushed",
        direction = "desc",
        .limit = Inf,
        .token = token,
        .progress = FALSE
      ),
      error = function(e) list()
    )
    private_df <- repo_to_rows(private_repos)
    if (nrow(private_df) > 0L) {
      owner_match <- tolower(private_df$owner_login %||% "") == tolower(username)
      name_match <- startsWith(tolower(private_df$full_name %||% ""), paste0(tolower(username), "/"))
      private_df <- private_df[owner_match | name_match, , drop = FALSE]
      if (nrow(private_df) > 0L) {
        repos_df <- rbind(repos_df, private_df)
      }
    }
  }

  if (!is.data.frame(repos_df) || nrow(repos_df) == 0L) {
    return(data.frame())
  }
  repos_df <- repos_df[!duplicated(repos_df$full_name), , drop = FALSE]
  rownames(repos_df) <- NULL
  repos_df
}

quality_fetch_languages <- function(full_name, token = NULL) {
  parts <- quality_repo_parts(full_name)
  if (is.null(parts)) {
    return(data.frame())
  }
  raw <- tryCatch(
    gh::gh(
      "GET /repos/{owner}/{repo}/languages",
      owner = parts$owner,
      repo = parts$repo,
      .token = token,
      .progress = FALSE
    ),
    error = function(e) list()
  )
  if (length(raw) == 0L) {
    return(data.frame())
  }
  data.frame(
    repository = full_name,
    language = names(raw),
    bytes = as.numeric(unlist(raw, use.names = FALSE)),
    stringsAsFactors = FALSE
  )
}

quality_fetch_tree <- function(full_name, branch, token = NULL) {
  parts <- quality_repo_parts(full_name)
  if (is.null(parts)) {
    return(data.frame())
  }

  raw <- tryCatch(
    gh::gh(
      "GET /repos/{owner}/{repo}/git/trees/{tree_sha}",
      owner = parts$owner,
      repo = parts$repo,
      tree_sha = branch %||% "main",
      recursive = "1",
      .token = token,
      .progress = FALSE
    ),
    error = function(e) {
      out <- data.frame()
      attr(out, "quality_error") <- conditionMessage(e)
      out
    }
  )
  if (!is.list(raw) || length(raw$tree %||% list()) == 0L) {
    return(data.frame())
  }

  rows <- lapply(raw$tree, function(item) {
    data.frame(
      path = item$path %||% NA_character_,
      type = item$type %||% NA_character_,
      size = item$size %||% NA_real_,
      sha = item$sha %||% NA_character_,
      stringsAsFactors = FALSE
    )
  })
  out <- do.call(rbind, rows)
  out$repository <- full_name
  out$truncated <- isTRUE(raw$truncated %||% FALSE)
  out
}

quality_file_language <- function(path) {
  lower <- tolower(path)
  name <- basename(lower)
  ext <- sub("^.*\\.([^.]+)$", "\\1", lower)
  no_ext <- identical(ext, lower)

  if (name == "dockerfile" || grepl("(^|/)dockerfile", lower)) return("Dockerfile")
  if (no_ext) return(NA_character_)

  mapping <- c(
    js = "JavaScript", jsx = "JavaScript", mjs = "JavaScript", cjs = "JavaScript",
    ts = "TypeScript", tsx = "TypeScript",
    py = "Python", go = "Go", java = "Java", kt = "Kotlin", kts = "Kotlin",
    cs = "C#", php = "PHP", rb = "Ruby", rs = "Rust", sql = "SQL",
    tf = "Terraform", tfvars = "Terraform", yml = "YAML", yaml = "YAML",
    json = "JSON", c = "C/C++", h = "C/C++", cpp = "C/C++", hpp = "C/C++",
    cc = "C/C++", swift = "Swift", dart = "Dart", scala = "Scala",
    ex = "Elixir", exs = "Elixir", r = "R", lua = "Lua", sol = "Solidity",
    ipynb = "Notebook"
  )
  if (ext %in% names(mapping)) {
    return(unname(mapping[[ext]]))
  }
  NA_character_
}

quality_is_code_file <- function(path) {
  !is.na(quality_file_language(path))
}

quality_detect_context <- function(repo_row, files) {
  name <- tolower(quality_chr(repo_row$name))
  desc <- tolower(quality_chr(repo_row$description))
  topics <- tolower(quality_chr(repo_row$topics))
  paths <- tolower(files$path %||% character())

  if (isTRUE(repo_row$archived %||% FALSE)) return("archive")
  if (isTRUE(repo_row$fork %||% FALSE)) return("fork")
  if (any(grepl("\\.tf$|(^|/)terraform/", paths))) return("infrastructure")
  if (any(basename(paths) %in% c("package.json", "pyproject.toml", "setup.py", "pom.xml", "build.gradle", "go.mod", "cargo.toml", "composer.json", "gemspec"))) return("library_or_service")
  if (grepl("demo|example|sample|sandbox|pet", paste(name, desc, topics))) return("pet_or_demo")
  if (quality_num(repo_row$stargazers_count) >= 10 || quality_num(repo_row$forks_count) >= 3) return("public_project")
  "application"
}

quality_repo_weight <- function(repo_row, context) {
  if (context %in% c("archive", "fork", "pet_or_demo")) {
    return(0.5)
  }
  1 + min(2, log1p(quality_num(repo_row$stargazers_count) + quality_num(repo_row$forks_count)) / 4) +
    min(1, log1p(quality_num(repo_row$size)) / 10)
}

quality_has_any <- function(paths, patterns) {
  any(vapply(patterns, function(pattern) any(grepl(pattern, paths, perl = TRUE)), logical(1)))
}

quality_analyze_repository <- function(repo_row,
                                       token = NULL,
                                       max_tree_files = 5000L) {
  full_name <- quality_chr(repo_row$full_name)
  tree <- quality_fetch_tree(full_name, repo_row$default_branch %||% "main", token = token)
  languages <- quality_fetch_languages(full_name, token = token)

  files <- tree[tree$type == "blob" & !is.na(tree$path), , drop = FALSE]
  if (nrow(files) > max_tree_files) {
    files <- files[seq_len(max_tree_files), , drop = FALSE]
  }
  paths <- tolower(files$path %||% character())
  base_names <- basename(paths)

  code_mask <- vapply(files$path %||% character(), quality_is_code_file, logical(1))
  code_files <- files[code_mask, , drop = FALSE]
  test_mask <- grepl("(^|/)(test|tests|spec|specs|__tests__)(/|$)", paths) |
    grepl("(_test|\\.test|\\.spec)\\.[^.]+$", paths)
  docs_mask <- base_names %in% c("readme.md", "readme.rst", "readme.txt", "contributing.md", "changelog.md", "license", "license.md", "security.md") |
    grepl("(^|/)(docs|doc|adr)(/|$)", paths)
  ci_mask <- grepl("^\\.github/workflows/.*\\.(yml|yaml)$|(^|/)(\\.gitlab-ci\\.yml|jenkinsfile|circle\\.yml)$", paths)
  dep_mask <- base_names %in% c(
    "package.json", "pyproject.toml", "requirements.txt", "pom.xml", "build.gradle",
    "go.mod", "cargo.toml", "composer.json", "gemfile", "renv.lock", "description"
  )
  lock_mask <- base_names %in% c(
    "package-lock.json", "yarn.lock", "pnpm-lock.yaml", "poetry.lock", "requirements.lock",
    "go.sum", "cargo.lock", "composer.lock", "gemfile.lock", "renv.lock"
  )
  security_mask <- base_names %in% c("security.md") |
    grepl("^\\.github/(dependabot|codeql|renovate)", paths)
  large_code_mask <- code_mask & quality_num(files$size) > 200000
  vendor_mask <- grepl("(^|/)(vendor|node_modules|dist|build|target|coverage|\\.venv)(/|$)", paths)
  deep_mask <- vapply(strsplit(paths, "/", fixed = TRUE), length, integer(1)) > 6L

  context <- quality_detect_context(repo_row, files)
  active_context <- !(context %in% c("archive", "fork", "pet_or_demo"))
  code_count <- nrow(code_files)
  test_count <- sum(test_mask & code_mask, na.rm = TRUE)
  doc_count <- sum(docs_mask, na.rm = TRUE)
  ci_count <- sum(ci_mask, na.rm = TRUE)
  dep_count <- sum(dep_mask, na.rm = TRUE)
  lock_count <- sum(lock_mask, na.rm = TRUE)
  large_count <- sum(large_code_mask, na.rm = TRUE)
  vendor_count <- sum(vendor_mask, na.rm = TRUE)
  deep_count <- sum(deep_mask & code_mask, na.rm = TRUE)
  truncated <- any(isTRUE(tree$truncated), na.rm = TRUE)

  pushed_at <- suppressWarnings(as.POSIXct(repo_row$pushed_at %||% NA_character_, tz = "UTC", format = "%Y-%m-%dT%H:%M:%SZ"))
  stale_days <- if (is.na(pushed_at)) NA_real_ else as.numeric(difftime(Sys.time(), pushed_at, units = "days"))

  test_ratio <- if (code_count > 0L) test_count / code_count else NA_real_
  docs_score <- quality_score(40 * (doc_count > 0L) + 25 * any(base_names == "readme.md") + 20 * any(base_names %in% c("contributing.md", "changelog.md")) + 15 * any(base_names %in% c("license", "license.md")))
  test_score <- if (is.na(test_ratio)) 50 else quality_score(35 + min(65, test_ratio * 650))
  ci_score <- quality_score(if (ci_count > 0L) 90 else if (!active_context) 65 else 35)
  security_score <- quality_score(70 + 15 * (lock_count > 0L || dep_count == 0L) + 15 * (sum(security_mask, na.rm = TRUE) > 0L) - if (dep_count > 0L && lock_count == 0L) 25 else 0)
  maintainability_score <- quality_score(100 - min(35, large_count * 8) - min(20, deep_count * 1.5) - min(20, vendor_count / max(nrow(files), 1L) * 100) - if (truncated) 10 else 0)
  activity_score <- quality_score(if (is.na(stale_days)) 60 else 100 - min(65, stale_days / 10))

  final_score <- quality_score(
    maintainability_score * 0.25 +
      test_score * 0.2 +
      ci_score * 0.15 +
      docs_score * 0.15 +
      security_score * 0.15 +
      activity_score * 0.1
  )
  confidence <- quality_score(
    30 +
      25 * (nrow(files) > 0L) +
      15 * (nrow(languages) > 0L) +
      10 * !truncated +
      10 * (code_count > 0L) +
      10 * !is.na(stale_days)
  ) / 100

  metrics <- data.frame(
    repository = full_name,
    context = context,
    files_total = nrow(files),
    code_files = code_count,
    test_files = test_count,
    test_file_ratio = round(test_ratio %||% NA_real_, 4),
    docs_files = doc_count,
    ci_files = ci_count,
    dependency_files = dep_count,
    lock_files = lock_count,
    large_code_files = large_count,
    deep_code_files = deep_count,
    vendor_generated_files = vendor_count,
    primary_language = if (nrow(languages) > 0L) languages$language[[which.max(languages$bytes)]] else repo_row$language %||% NA_character_,
    maintainability_score = maintainability_score,
    test_score = test_score,
    ci_score = ci_score,
    docs_score = docs_score,
    security_score = security_score,
    activity_score = activity_score,
    quality_score = final_score,
    confidence = round(confidence, 2),
    weight = round(quality_repo_weight(repo_row, context), 3),
    truncated_tree = truncated,
    stringsAsFactors = FALSE
  )

  findings <- list()
  add_finding <- function(category, severity, title, evidence, penalty) {
    findings[[length(findings) + 1L]] <<- data.frame(
      repository = full_name,
      category = category,
      severity = severity,
      title = title,
      evidence = evidence,
      penalty = penalty,
      confidence = round(confidence, 2),
      stringsAsFactors = FALSE
    )
  }

  if (active_context && code_count > 10L && (is.na(test_ratio) || test_ratio < 0.05)) {
    add_finding("testability", "high", "Low or missing automated test footprint", paste0(test_count, " test files for ", code_count, " code files"), 18)
  }
  if (active_context && ci_count == 0L && code_count > 0L) {
    add_finding("ci_cd", "medium", "No CI workflow detected", "No GitHub/GitLab/Jenkins workflow files in repository tree", 12)
  }
  if (dep_count > 0L && lock_count == 0L) {
    add_finding("supply_chain", "medium", "Dependency manifests without lock files", paste0(dep_count, " dependency files, 0 lock files"), 12)
  }
  if (active_context && doc_count == 0L) {
    add_finding("documentation", "medium", "No basic documentation detected", "README/docs/license/security files were not found", 10)
  }
  if (large_count > 0L) {
    add_finding("maintainability", "medium", "Large source files detected", paste0(large_count, " code files are larger than 200 KB"), 8)
  }
  if (!is.na(stale_days) && stale_days > 365 && active_context) {
    add_finding("activity", "low", "Repository looks stale", paste0("Last push was ", round(stale_days), " days ago"), 6)
  }
  if (truncated) {
    add_finding("evidence", "low", "GitHub tree was truncated", "GitHub API returned a truncated recursive tree", 4)
  }

  list(
    metrics = metrics,
    findings = if (length(findings) > 0L) do.call(rbind, findings) else data.frame(),
    languages = languages,
    files = if (nrow(files) > 0L) {
      data.frame(
        repository = full_name,
        path = files$path,
        type = files$type,
        size = files$size,
        language = vapply(files$path, quality_file_language, character(1)),
        is_code = code_mask,
        is_test = test_mask,
        is_doc = docs_mask,
        is_ci = ci_mask,
        is_dependency = dep_mask,
        is_lock = lock_mask,
        stringsAsFactors = FALSE
      )
    } else {
      data.frame()
    }
  )
}

quality_analyze_repositories <- function(repos,
                                         token = NULL,
                                         progress = function(value, label = NULL) NULL,
                                         base_progress = 25,
                                         end_progress = 82) {
  if (!is.data.frame(repos) || nrow(repos) == 0L) {
    return(list(metrics = data.frame(), findings = data.frame(), languages = data.frame()))
  }

  max_tree_files <- quality_env_int("GITHOUND_QUALITY_MAX_TREE_FILES", 5000L, min_value = 100L)
  configured_workers <- quality_env_int("GITHOUND_QUALITY_WORKERS", 2L, min_value = 1L)
  workers <- min(configured_workers, nrow(repos))

  analyze_one <- function(i) {
    quality_analyze_repository(repos[i, , drop = FALSE], token = token, max_tree_files = max_tree_files)
  }

  results <- list()
  if (workers <= 1L || nrow(repos) <= 1L) {
    for (i in seq_len(nrow(repos))) {
      results[[length(results) + 1L]] <- analyze_one(i)
      value <- base_progress + (end_progress - base_progress) * length(results) / nrow(repos)
      progress(round(value), paste0("Code quality scan: ", length(results), "/", nrow(repos), " repositories"))
    }
  } else {
    index_chunks <- split(seq_len(nrow(repos)), ceiling(seq_len(nrow(repos)) / workers))
    for (chunk in index_chunks) {
      cl <- parallel::makeCluster(min(workers, length(chunk)))
      on.exit(try(parallel::stopCluster(cl), silent = TRUE), add = TRUE)
      parallel::clusterExport(
        cl,
        varlist = ls(envir = environment()),
        envir = environment()
      )
      helper_names <- c(
        "%||%", "quality_num", "quality_chr", "quality_score",
        "quality_repo_parts", "quality_fetch_languages", "quality_fetch_tree",
        "quality_file_language", "quality_is_code_file", "quality_detect_context",
        "quality_repo_weight", "quality_has_any", "quality_analyze_repository"
      )
      helper_names <- helper_names[vapply(helper_names, exists, logical(1), mode = "function")]
      parallel::clusterExport(cl, varlist = helper_names, envir = .GlobalEnv)
      parallel::clusterEvalQ(cl, {
        if (!requireNamespace("gh", quietly = TRUE)) stop("Package 'gh' is required.")
        NULL
      })
      chunk_results <- parallel::parLapply(cl, chunk, analyze_one)
      try(parallel::stopCluster(cl), silent = TRUE)
      results <- c(results, chunk_results)
      value <- base_progress + (end_progress - base_progress) * length(results) / nrow(repos)
      progress(round(value), paste0("Code quality scan: ", length(results), "/", nrow(repos), " repositories"))
    }
  }

  metric_parts <- lapply(results, `[[`, "metrics")
  finding_parts <- Filter(function(x) is.data.frame(x) && nrow(x) > 0L, lapply(results, `[[`, "findings"))
  language_parts <- Filter(function(x) is.data.frame(x) && nrow(x) > 0L, lapply(results, `[[`, "languages"))
  file_parts <- Filter(function(x) is.data.frame(x) && nrow(x) > 0L, lapply(results, `[[`, "files"))

  list(
    metrics = if (length(metric_parts) > 0L) do.call(rbind, metric_parts) else data.frame(),
    findings = if (length(finding_parts) > 0L) do.call(rbind, finding_parts) else data.frame(),
    languages = if (length(language_parts) > 0L) do.call(rbind, language_parts) else data.frame(),
    files = if (length(file_parts) > 0L) do.call(rbind, file_parts) else data.frame()
  )
}

quality_weighted_mean <- function(value, weight) {
  value <- quality_num(value, default = NA_real_)
  weight <- quality_num(weight, default = 1)
  ok <- !is.na(value) & !is.na(weight) & weight > 0
  if (!any(ok)) {
    return(NA_real_)
  }
  round(sum(value[ok] * weight[ok]) / sum(weight[ok]), 1)
}

quality_ai_prompt <- function() {
  root <- if (exists("find_githound_project_root", mode = "function")) {
    tryCatch(find_githound_project_root(), error = function(e) getwd())
  } else {
    getwd()
  }
  path <- file.path(root, "inst", "ai", "prompts", "code_quality.md")
  if (file.exists(path)) {
    return(paste(readLines(path, warn = FALSE, encoding = "UTF-8"), collapse = "\n"))
  }
  paste(
    "Ты эксперт по code quality. Всегда отвечай на русском.",
    "Оценивай только по переданным evidence и возвращай строгий JSON без markdown."
  )
}

quality_parse_ai_json <- function(text) {
  text <- quality_compact_text(text, max_chars = 200000L)
  text <- sub("^```(?:json)?\\s*", "", text, ignore.case = TRUE, perl = TRUE)
  text <- sub("\\s*```$", "", text, perl = TRUE)
  start <- regexpr("[\\[{]", text, perl = TRUE)[[1]]
  if (start > 1L) {
    text <- substring(text, start)
  }
  tryCatch(
    jsonlite::fromJSON(text, simplifyVector = FALSE),
    error = function(e) list(error = TRUE, message = conditionMessage(e), raw = text)
  )
}

quality_flatten_list <- function(x, prefix = "") {
  if (is.data.frame(x)) {
    return(list())
  }
  if (!is.list(x)) {
    return(setNames(list(x), prefix))
  }
  out <- list()
  nms <- names(x)
  if (is.null(nms)) {
    nms <- rep("", length(x))
  }
  for (i in seq_along(x)) {
    name <- nms[[i]]
    if (!nzchar(name)) {
      name <- as.character(i)
    }
    key <- if (nzchar(prefix)) paste(prefix, name, sep = ".") else name
    value <- x[[i]]
    if (is.list(value) && !is.data.frame(value)) {
      out <- c(out, quality_flatten_list(value, key))
    } else {
      out[[key]] <- value
    }
  }
  out
}

quality_ai_value <- function(parsed, aliases, default = NULL) {
  flat <- quality_flatten_list(parsed)
  if (length(flat) == 0L) {
    return(default)
  }
  flat_names <- tolower(names(flat))
  aliases <- tolower(aliases)
  for (alias in aliases) {
    idx <- which(flat_names == alias)
    if (length(idx) > 0L) {
      value <- flat[[idx[[1L]]]]
      if (!is.null(value) && length(value) > 0L && !(is.atomic(value) && length(value) == 1L && is.na(value))) {
        return(value)
      }
    }
  }
  for (alias in aliases) {
    idx <- which(endsWith(flat_names, paste0(".", alias)))
    if (length(idx) > 0L) {
      value <- flat[[idx[[1L]]]]
      if (!is.null(value) && length(value) > 0L && !(is.atomic(value) && length(value) == 1L && is.na(value))) {
        return(value)
      }
    }
  }
  default
}

quality_ai_numeric <- function(parsed, aliases, default = NA_real_, max_value = 100) {
  value <- quality_ai_value(parsed, aliases, default = default)
  value <- suppressWarnings(as.numeric(value[[1L]]))
  if (is.na(value)) {
    return(default)
  }
  max(0, min(max_value, value))
}

quality_normalize_ai_review <- function(parsed) {
  if (!is.list(parsed) || isTRUE(parsed$error %||% FALSE)) {
    return(parsed)
  }

  parsed$score <- quality_ai_numeric(parsed, c(
    "score", "overall_score", "quality_score", "final_score", "total_score",
    "scores.overall", "scores.total", "scores.quality", "rating"
  ))
  parsed$confidence <- quality_ai_numeric(parsed, c(
    "confidence", "ai_confidence", "scores.confidence"
  ), max_value = 1)
  if (!is.na(parsed$confidence) && parsed$confidence > 1) {
    parsed$confidence <- parsed$confidence / 100
  }
  parsed$readability_score <- quality_ai_numeric(parsed, c(
    "readability_score", "readability", "scores.readability", "scores.readability_score",
    "code_readability", "readabilityScore"
  ))
  parsed$best_practice_score <- quality_ai_numeric(parsed, c(
    "best_practice_score", "best_practices_score", "best_practices", "best_practice",
    "scores.best_practices", "scores.best_practice", "bestPracticeScore"
  ))
  parsed$structure_score <- quality_ai_numeric(parsed, c(
    "structure_score", "structure", "repository_structure_score", "repo_structure_score",
    "scores.structure", "scores.repository_structure", "structureScore"
  ))
  parsed$ai_generated_likelihood <- quality_ai_numeric(parsed, c(
    "ai_generated_likelihood", "ai_generated_score", "ai_likelihood", "generated_likelihood",
    "scores.ai_generated_likelihood", "scores.ai_generated", "aiGeneratedLikelihood"
  ))
  parsed$summary <- as.character(quality_ai_value(parsed, c(
    "summary", "conclusion", "overall_summary", "short_summary", "executive_summary"
  ), default = parsed$summary %||% ""))[[1L]]

  if (is.na(parsed$score)) {
    components <- c(parsed$readability_score, parsed$best_practice_score, parsed$structure_score)
    if (any(!is.na(components))) {
      parsed$score <- round(mean(components, na.rm = TRUE), 1)
    }
  }
  parsed
}

quality_ai_model <- function() {
  model <- trimws(Sys.getenv("GITHOUND_QUALITY_AI_MODEL", unset = ""))
  if (nzchar(model)) {
    return(model)
  }
  Sys.getenv("OPENAI_MODEL", unset = "gpt-4.1-mini")
}

quality_ai_enabled <- function() {
  quality_env_flag("GITHOUND_QUALITY_AI_ENABLED", default = TRUE) &&
    nzchar(trimws(Sys.getenv("OPENAI_API_KEY", unset = ""))) &&
    exists("mcp_openai_chat", mode = "function") &&
    exists("mcp_message", mode = "function")
}

quality_ai_chat <- function(messages,
                            system_prompt,
                            max_tokens = NULL,
                            timeout = NULL) {
  timeout <- timeout %||% quality_env_int("GITHOUND_QUALITY_AI_TIMEOUT_SEC", 180L, min_value = 10L)
  mcp_openai_chat(
    messages = messages,
    tools = list(),
    model = quality_ai_model(),
    api_key = Sys.getenv("OPENAI_API_KEY", unset = ""),
    base_url = Sys.getenv("OPENAI_BASE_URL", unset = "https://api.openai.com/v1"),
    system_prompt = system_prompt,
    temperature = quality_env_num("GITHOUND_QUALITY_AI_TEMPERATURE", 0, min_value = 0, max_value = 2),
    max_tokens = max_tokens,
    max_tool_rounds = 0,
    timeout = timeout,
    tool_choice = "none"
  )
}

quality_repo_file_inventory_for_ai <- function(files, repo, limit = 250L) {
  repo_files <- files[files$repository == repo, , drop = FALSE]
  if (nrow(repo_files) == 0L) {
    return(data.frame())
  }
  code <- as.logical(repo_files$is_code %||% FALSE); code[is.na(code)] <- FALSE
  test <- as.logical(repo_files$is_test %||% FALSE); test[is.na(test)] <- FALSE
  doc <- as.logical(repo_files$is_doc %||% FALSE); doc[is.na(doc)] <- FALSE
  ci <- as.logical(repo_files$is_ci %||% FALSE); ci[is.na(ci)] <- FALSE
  dep <- as.logical(repo_files$is_dependency %||% FALSE); dep[is.na(dep)] <- FALSE
  repo_files$rank <- ifelse(code, 1L, 5L)
  repo_files$rank[test] <- 2L
  repo_files$rank[doc] <- 3L
  repo_files$rank[ci | dep] <- 4L
  repo_files <- repo_files[order(repo_files$rank, repo_files$path), , drop = FALSE]
  repo_files <- utils::head(repo_files, limit)
  out <- repo_files[, c("path", "size", "language", "is_code", "is_test", "is_doc", "is_ci", "is_dependency"), drop = FALSE]
  for (col in c("is_code", "is_test", "is_doc", "is_ci", "is_dependency")) {
    flag <- as.logical(out[[col]] %||% FALSE)
    flag[is.na(flag)] <- FALSE
    out[[col]] <- as.integer(flag)
  }
  out
}

quality_file_structure_for_ai <- function(files) {
  if (!is.data.frame(files) || nrow(files) == 0L) {
    return(data.frame())
  }
  limit <- quality_env_int("GITHOUND_QUALITY_AI_STRUCTURE_LIMIT_PER_REPO", 60L, min_value = 10L)
  repos <- unique(files$repository)
  parts <- lapply(repos, function(repo) {
    out <- files[files$repository == repo, , drop = FALSE]
    code <- as.logical(out$is_code %||% FALSE); code[is.na(code)] <- FALSE
    test <- as.logical(out$is_test %||% FALSE); test[is.na(test)] <- FALSE
    doc <- as.logical(out$is_doc %||% FALSE); doc[is.na(doc)] <- FALSE
    ci <- as.logical(out$is_ci %||% FALSE); ci[is.na(ci)] <- FALSE
    dep <- as.logical(out$is_dependency %||% FALSE); dep[is.na(dep)] <- FALSE
    lock <- as.logical(out$is_lock %||% FALSE); lock[is.na(lock)] <- FALSE
    out$rank <- ifelse(code, 1L, 6L)
    out$rank[test] <- 2L
    out$rank[doc] <- 3L
    out$rank[ci] <- 4L
    out$rank[dep | lock] <- 5L
    out <- out[order(out$rank, out$path), , drop = FALSE]
    utils::head(out, limit)
  })
  out <- do.call(rbind, parts)
  out <- out[, intersect(c("repository", "path", "size", "language", "is_code", "is_test", "is_doc", "is_ci", "is_dependency", "is_lock"), names(out)), drop = FALSE]
  total_limit <- quality_env_int("GITHOUND_QUALITY_AI_STRUCTURE_TOTAL_LIMIT", 600L, min_value = 50L)
  if (nrow(out) > total_limit) {
    out <- utils::head(out, total_limit)
  }
  out
}

quality_select_files_with_ai <- function(scan_repos, files) {
  max_files_per_repo <- quality_env_int("GITHOUND_QUALITY_AI_FILES_PER_REPO", 4L, min_value = 1L)
  file_list_limit <- quality_env_int("GITHOUND_QUALITY_AI_FILE_LIST_LIMIT", 80L, min_value = 10L)
  batch_size <- quality_env_int("GITHOUND_QUALITY_AI_SELECT_REPO_BATCH", 3L, min_value = 1L)
  parse_selected <- function(parsed) {
    selected <- parsed$files %||% list()
    if (is.data.frame(selected)) {
      return(selected)
    }
    if (length(selected) > 0L) {
      return(do.call(rbind, lapply(selected, function(item) {
        data.frame(
          repository = item$repository %||% NA_character_,
          path = item$path %||% NA_character_,
          reason = item$reason %||% "",
          stringsAsFactors = FALSE
        )
      })))
    }
    data.frame(repository = character(), path = character(), reason = character(), stringsAsFactors = FALSE)
  }

  prompt <- paste(
    "Выбери до", max_files_per_repo, "файлов на каждый репозиторий для оценки читаемости, best practices и признаков AI-generated кода.",
    "Верни строгий JSON без markdown в формате:",
    '{"files":[{"repository":"owner/repo","path":"src/app.py","reason":"почему выбран"}]}',
    "Не выбирай lock/vendor/generated/build файлы, если есть более полезные source/test/config файлы.",
    sep = "\n"
  )
  chunks <- split(seq_len(nrow(scan_repos)), ceiling(seq_len(nrow(scan_repos)) / batch_size))
  selected_parts <- lapply(chunks, function(idx) {
    repos_payload <- lapply(idx, function(i) {
      repo <- scan_repos$full_name[[i]]
      list(
        repository = repo,
        primary_language = scan_repos$language[[i]] %||% NA_character_,
        description = scan_repos$description[[i]] %||% "",
        default_branch = scan_repos$default_branch[[i]] %||% "main",
        files = quality_repo_file_inventory_for_ai(files, repo, limit = file_list_limit)
      )
    })
    result <- quality_ai_chat(
      messages = list(mcp_message("user", paste(prompt, as.character(quality_json(repos_payload)), sep = "\n\n"))),
      system_prompt = quality_ai_prompt(),
      max_tokens = quality_env_int("GITHOUND_QUALITY_AI_SELECT_MAX_TOKENS", 1200L, min_value = 300L)
    )
    parse_selected(quality_parse_ai_json(result$final_message$content %||% ""))
  })

  selected_parts <- Filter(function(x) is.data.frame(x) && nrow(x) > 0L, selected_parts)
  if (length(selected_parts) > 0L) {
    selected_df <- do.call(rbind, selected_parts)
  } else {
    selected_df <- data.frame(repository = character(), path = character(), reason = character(), stringsAsFactors = FALSE)
  }
  selected_df <- selected_df[!is.na(selected_df$repository) & !is.na(selected_df$path), , drop = FALSE]
  valid <- paste(files$repository, files$path, sep = "\r")
  selected_df <- selected_df[paste(selected_df$repository, selected_df$path, sep = "\r") %in% valid, , drop = FALSE]
  if (nrow(selected_df) > 0L) {
    selected_df <- do.call(rbind, lapply(split(selected_df, selected_df$repository), function(df) {
      utils::head(df, max_files_per_repo)
    }))
  }
  selected_df <- selected_df[!duplicated(paste(selected_df$repository, selected_df$path, sep = "\r")), , drop = FALSE]
  rownames(selected_df) <- NULL
  selected_df
}

quality_decode_github_content <- function(content) {
  content <- gsub("\\s+", "", as.character(content %||% ""), perl = TRUE)
  if (!nzchar(content)) {
    return("")
  }
  raw <- jsonlite::base64_dec(content)
  rawToChar(raw, multiple = FALSE)
}

quality_fetch_file_text <- function(full_name, path, ref = NULL, token = NULL) {
  parts <- quality_repo_parts(full_name)
  if (is.null(parts)) {
    return(NA_character_)
  }
  item <- tryCatch(
    gh::gh(
      "GET /repos/{owner}/{repo}/contents/{path}",
      owner = parts$owner,
      repo = parts$repo,
      path = path,
      ref = ref %||% NULL,
      .token = token,
      .progress = FALSE
    ),
    error = function(e) NULL
  )
  if (is.null(item) || is.null(item$content)) {
    return(NA_character_)
  }
  tryCatch(quality_decode_github_content(item$content), error = function(e) NA_character_)
}

quality_text_fragment <- function(text, max_lines = 100L, max_chars = 30000L) {
  text <- quality_compact_text(text, max_chars = max_chars)
  lines <- strsplit(text, "\n", fixed = TRUE)[[1]]
  paste(utils::head(lines, max_lines), collapse = "\n")
}

quality_fetch_ai_fragments <- function(selected_files, scan_repos, files, token = NULL) {
  if (!is.data.frame(selected_files) || nrow(selected_files) == 0L) {
    return(data.frame())
  }
  max_lines <- quality_env_int("GITHOUND_QUALITY_AI_CODE_LINES", 100L, min_value = 10L)
  max_bytes <- quality_env_int("GITHOUND_QUALITY_AI_MAX_FILE_BYTES", 200000L, min_value = 1000L)

  max_fragments <- quality_env_int("GITHOUND_QUALITY_AI_MAX_CODE_FRAGMENTS", 24L, min_value = 1L)
  selected_files <- utils::head(selected_files, max_fragments)

  rows <- lapply(seq_len(nrow(selected_files)), function(i) {
    repo <- selected_files$repository[[i]]
    path <- selected_files$path[[i]]
    file_row <- files[files$repository == repo & files$path == path, , drop = FALSE]
    size <- if (nrow(file_row) > 0L) quality_num(file_row$size[[1]], default = NA_real_) else NA_real_
    language <- if (nrow(file_row) > 0L) file_row$language[[1]] else quality_file_language(path)
    ref <- scan_repos$default_branch[match(repo, scan_repos$full_name)] %||% "main"
    content <- if (!is.na(size) && size > max_bytes) {
      NA_character_
    } else {
      quality_fetch_file_text(repo, path, ref = ref, token = token)
    }
    data.frame(
      repository = repo,
      path = path,
      language = language %||% NA_character_,
      size = size,
      reason = selected_files$reason[[i]] %||% "",
      skipped = is.na(content),
      fragment = if (is.na(content)) "" else quality_text_fragment(content, max_lines = max_lines),
      stringsAsFactors = FALSE
    )
  })
  do.call(rbind, rows)
}

quality_fetch_readmes <- function(scan_repos, files, token = NULL) {
  readme_rows <- files[grepl("(^|/)readme\\.(md|rst|txt)$", tolower(files$path)), , drop = FALSE]
  if (nrow(readme_rows) == 0L) {
    return(data.frame())
  }
  readme_rows <- readme_rows[!duplicated(readme_rows$repository), , drop = FALSE]
  readme_rows <- utils::head(readme_rows, quality_env_int("GITHOUND_QUALITY_AI_MAX_READMES", 8L, min_value = 1L))
  max_lines <- quality_env_int("GITHOUND_QUALITY_AI_README_LINES", 40L, min_value = 5L)
  rows <- lapply(seq_len(nrow(readme_rows)), function(i) {
    repo <- readme_rows$repository[[i]]
    ref <- scan_repos$default_branch[match(repo, scan_repos$full_name)] %||% "main"
    content <- quality_fetch_file_text(repo, readme_rows$path[[i]], ref = ref, token = token)
    data.frame(
      repository = repo,
      path = readme_rows$path[[i]],
      fragment = if (is.na(content)) "" else quality_text_fragment(content, max_lines = max_lines, max_chars = 12000L),
      stringsAsFactors = FALSE
    )
  })
  do.call(rbind, rows)
}

quality_fetch_vulnerability_lifecycle <- function(username, conn = NULL) {
  if (is.null(conn) || !exists("query_clickhouse", mode = "function")) {
    return(data.frame())
  }
  table_name <- "github_commit_scan_lifecycle"
  if (exists("table_exists_custom", mode = "function") && !isTRUE(table_exists_custom(conn, table_name))) {
    return(data.frame())
  }
  table_sql <- if (exists("quote_table_ident", mode = "function")) quote_table_ident(table_name, conn$dbname) else table_name
  escaped <- if (exists("escape_sql_string", mode = "function")) escape_sql_string(username) else gsub("'", "''", username)
  limit <- quality_env_int("GITHOUND_QUALITY_AI_VULN_LIMIT", 50L, min_value = 1L)
  sql <- paste0(
    "SELECT * FROM ", table_sql,
    " WHERE repository LIKE '", escaped, "%'",
    " LIMIT ", limit
  )
  out <- tryCatch(query_clickhouse(conn, sql), error = function(e) data.frame())
  if (!is.data.frame(out) || nrow(out) == 0L) {
    return(data.frame())
  }
  keep <- intersect(
    c(
      "repository", "osv_id", "status", "component_name", "component_purl",
      "vulnerability_published", "vulnerability_modified",
      "introduced_at", "introduced_commit", "fixed_at", "fixed_commit",
      "first_seen_at", "last_seen_at"
    ),
    names(out)
  )
  if (length(keep) > 0L) {
    out <- out[, keep, drop = FALSE]
  }
  out
}

quality_ai_review_findings_df <- function(ai_review) {
  parsed <- ai_review$parsed %||% list()
  findings <- parsed$findings %||% list()
  if (is.data.frame(findings)) {
    out <- findings
  } else if (length(findings) > 0L) {
    out <- do.call(rbind, lapply(findings, function(item) {
      data.frame(
        repository = item$repository %||% NA_character_,
        severity = item$severity %||% "low",
        category = item$category %||% "ai_review",
        title = item$title %||% "",
        evidence = item$evidence %||% "",
        stringsAsFactors = FALSE
      )
    }))
  } else {
    return(data.frame())
  }
  out$penalty <- ifelse(out$severity == "high", 12, ifelse(out$severity == "medium", 7, 3))
  out$confidence <- quality_num(parsed$confidence %||% 0, default = 0)
  out$category <- paste0("ai_", out$category %||% "review")
  out
}

quality_limit_payload_text <- function(df, col, max_chars) {
  if (!is.data.frame(df) || nrow(df) == 0L || !col %in% names(df)) {
    return(df)
  }
  df[[col]] <- vapply(df[[col]], quality_compact_text, character(1), max_chars = max_chars)
  df
}

quality_compact_review_payload <- function(payload) {
  max_chars <- quality_env_int("GITHOUND_QUALITY_AI_REVIEW_MAX_INPUT_CHARS", 120000L, min_value = 20000L)
  payload$file_structure <- utils::head(payload$file_structure %||% data.frame(), quality_env_int("GITHOUND_QUALITY_AI_STRUCTURE_TOTAL_LIMIT", 600L, min_value = 50L))
  payload$selected_code_fragments <- utils::head(payload$selected_code_fragments %||% data.frame(), quality_env_int("GITHOUND_QUALITY_AI_MAX_CODE_FRAGMENTS", 24L, min_value = 1L))
  payload$readme_fragments <- utils::head(payload$readme_fragments %||% data.frame(), quality_env_int("GITHOUND_QUALITY_AI_MAX_READMES", 8L, min_value = 1L))
  payload$vulnerability_lifecycle <- utils::head(payload$vulnerability_lifecycle %||% data.frame(), quality_env_int("GITHOUND_QUALITY_AI_VULN_LIMIT", 50L, min_value = 1L))

  shrink_steps <- list(
    list(fragment_chars = 12000L, readme_chars = 8000L, structure_rows = 600L, vuln_rows = 50L),
    list(fragment_chars = 8000L, readme_chars = 5000L, structure_rows = 400L, vuln_rows = 35L),
    list(fragment_chars = 5000L, readme_chars = 3000L, structure_rows = 250L, vuln_rows = 20L),
    list(fragment_chars = 3000L, readme_chars = 1800L, structure_rows = 150L, vuln_rows = 12L),
    list(fragment_chars = 1800L, readme_chars = 1000L, structure_rows = 80L, vuln_rows = 8L)
  )

  for (step in shrink_steps) {
    candidate <- payload
    candidate$selected_code_fragments <- quality_limit_payload_text(
      utils::head(candidate$selected_code_fragments, quality_env_int("GITHOUND_QUALITY_AI_MAX_CODE_FRAGMENTS", 24L, min_value = 1L)),
      "fragment",
      step$fragment_chars
    )
    candidate$readme_fragments <- quality_limit_payload_text(
      utils::head(candidate$readme_fragments, quality_env_int("GITHOUND_QUALITY_AI_MAX_READMES", 8L, min_value = 1L)),
      "fragment",
      step$readme_chars
    )
    candidate$file_structure <- utils::head(candidate$file_structure, step$structure_rows)
    candidate$vulnerability_lifecycle <- utils::head(candidate$vulnerability_lifecycle, step$vuln_rows)
    if (nchar(as.character(quality_json(candidate)), type = "bytes") <= max_chars) {
      return(candidate)
    }
    payload <- candidate
  }

  for (i in seq_len(8L)) {
    if (nchar(as.character(quality_json(payload)), type = "bytes") <= max_chars) {
      return(payload)
    }
    fragment_chars <- max(300L, as.integer(1800L / i))
    readme_chars <- max(250L, as.integer(1000L / i))
    payload$selected_code_fragments <- quality_limit_payload_text(
      utils::head(payload$selected_code_fragments, max(4L, floor(nrow(payload$selected_code_fragments) / 2L))),
      "fragment",
      fragment_chars
    )
    payload$readme_fragments <- quality_limit_payload_text(
      utils::head(payload$readme_fragments, max(2L, floor(nrow(payload$readme_fragments) / 2L))),
      "fragment",
      readme_chars
    )
    payload$file_structure <- utils::head(payload$file_structure, max(30L, floor(nrow(payload$file_structure) / 2L)))
    payload$vulnerability_lifecycle <- utils::head(payload$vulnerability_lifecycle, max(3L, floor(nrow(payload$vulnerability_lifecycle) / 2L)))
  }

  payload$selected_code_fragments <- quality_limit_payload_text(utils::head(payload$selected_code_fragments, 4L), "fragment", 300L)
  payload$readme_fragments <- quality_limit_payload_text(utils::head(payload$readme_fragments, 2L), "fragment", 250L)
  payload$file_structure <- utils::head(payload$file_structure, 30L)
  payload$vulnerability_lifecycle <- utils::head(payload$vulnerability_lifecycle, 3L)
  payload
}

quality_run_ai_review <- function(username,
                                  scan_repos,
                                  scanned,
                                  conn = NULL,
                                  token = NULL,
                                  progress = function(value, label = NULL) NULL) {
  if (!quality_ai_enabled()) {
    return(list(status = "skipped", message = "AI review disabled or OPENAI_API_KEY is empty."))
  }
  if (!is.data.frame(scanned$files) || nrow(scanned$files) == 0L) {
    return(list(status = "skipped", message = "No file inventory for AI review."))
  }

  progress(83, "Code quality AI: selecting files")
  selected <- quality_select_files_with_ai(scan_repos, scanned$files)
  if (!is.data.frame(selected) || nrow(selected) == 0L) {
    return(list(status = "skipped", message = "AI did not select files for review."))
  }

  progress(85, "Code quality AI: downloading selected fragments")
  fragments <- quality_fetch_ai_fragments(selected, scan_repos, scanned$files, token = token)
  readmes <- quality_fetch_readmes(scan_repos, scanned$files, token = token)
  vulnerabilities <- quality_fetch_vulnerability_lifecycle(username, conn = conn)

  progress(88, "Code quality AI: best practice review")
  payload <- list(
    profile = username,
    repositories = scan_repos[, intersect(c("full_name", "description", "language", "stargazers_count", "forks_count", "open_issues_count", "size", "default_branch", "pushed_at"), names(scan_repos)), drop = FALSE],
    repo_metrics = scanned$metrics,
    file_structure = quality_file_structure_for_ai(scanned$files),
    readme_fragments = readmes,
    selected_code_fragments = fragments,
    vulnerability_lifecycle = vulnerabilities
  )
  payload <- quality_compact_review_payload(payload)
  prompt <- paste(
    "Оцени качество кода пользователя и структуру репозиториев по best practices используемых языков и технологий.",
    "Учитывай README, структуру файлов, stack/languages, размеры файлов, выбранные фрагменты кода и OSV lifecycle findings.",
    "Верни строгий JSON по схеме из system prompt.",
    sep = "\n"
  )
  chat <- quality_ai_chat(
    messages = list(mcp_message("user", paste(prompt, as.character(quality_json(payload)), sep = "\n\n"))),
    system_prompt = quality_ai_prompt(),
    max_tokens = quality_env_int("GITHOUND_QUALITY_AI_REVIEW_MAX_TOKENS", 1800L, min_value = 500L),
    timeout = quality_env_int("GITHOUND_QUALITY_AI_TIMEOUT_SEC", 180L, min_value = 10L)
  )
  parsed <- quality_normalize_ai_review(quality_parse_ai_json(chat$final_message$content %||% ""))
  status <- if (isTRUE(parsed$error %||% FALSE)) "error" else "ok"
  list(
    status = status,
    message = if (identical(status, "ok")) "AI review completed." else parsed$message %||% "AI JSON parse error.",
    selected_files = selected,
    code_fragments = fragments,
    readmes = readmes,
    vulnerabilities = vulnerabilities,
    parsed = parsed,
    raw = chat$final_message$content %||% ""
  )
}

quality_ai_score <- function(ai_review) {
  if (!is.list(ai_review) || !identical(ai_review$status %||% "", "ok")) {
    return(NA_real_)
  }
  quality_score(ai_review$parsed$score %||% NA_real_)
}

quality_build_overall <- function(metrics, findings, repos_total, repos_scanned, ai_review = NULL) {
  if (!is.data.frame(metrics) || nrow(metrics) == 0L) {
    return(data.frame(
      metric = c("overall_score", "confidence", "repositories_total", "repositories_scanned", "findings_total"),
      value = c(NA, NA, repos_total, repos_scanned, if (is.data.frame(findings)) nrow(findings) else 0L),
      stringsAsFactors = FALSE
    ))
  }
  weight <- metrics$weight %||% rep(1, nrow(metrics))
  deterministic_score <- quality_weighted_mean(metrics$quality_score, weight)
  ai_score <- quality_ai_score(ai_review)
  ai_weight <- quality_env_num("GITHOUND_QUALITY_AI_WEIGHT", 0.2, min_value = 0, max_value = 0.8)
  overall_score <- if (!is.na(ai_score)) {
    quality_score(deterministic_score * (1 - ai_weight) + ai_score * ai_weight)
  } else {
    deterministic_score
  }
  ai_confidence <- if (is.list(ai_review) && identical(ai_review$status %||% "", "ok")) {
    quality_num(ai_review$parsed$confidence %||% NA_real_, default = NA_real_)
  } else {
    NA_real_
  }
  data.frame(
    metric = c(
      "overall_score",
      "deterministic_score",
      "ai_score",
      "ai_weight",
      "ai_confidence",
      "maintainability_score",
      "test_score",
      "ci_score",
      "docs_score",
      "security_score",
      "activity_score",
      "confidence",
      "repositories_total",
      "repositories_scanned",
      "findings_total",
      "high_findings",
      "medium_findings"
    ),
    value = c(
      overall_score,
      deterministic_score,
      ai_score,
      ai_weight,
      ai_confidence,
      quality_weighted_mean(metrics$maintainability_score, weight),
      quality_weighted_mean(metrics$test_score, weight),
      quality_weighted_mean(metrics$ci_score, weight),
      quality_weighted_mean(metrics$docs_score, weight),
      quality_weighted_mean(metrics$security_score, weight),
      quality_weighted_mean(metrics$activity_score, weight),
      round(mean(quality_num(metrics$confidence, default = NA_real_), na.rm = TRUE), 2),
      repos_total,
      repos_scanned,
      if (is.data.frame(findings)) nrow(findings) else 0L,
      if (is.data.frame(findings) && "severity" %in% names(findings)) sum(findings$severity == "high", na.rm = TRUE) else 0L,
      if (is.data.frame(findings) && "severity" %in% names(findings)) sum(findings$severity == "medium", na.rm = TRUE) else 0L
    ),
    stringsAsFactors = FALSE
  )
}

quality_save_to_clickhouse <- function(result, conn = NULL) {
  if (is.null(conn) || !exists("load_df_to_clickhouse", mode = "function")) {
    return(invisible(FALSE))
  }

  run_id <- result$run_id %||% format(Sys.time(), "%Y%m%d%H%M%S")
  scalar_text <- function(x, default = "") {
    if (is.null(x) || length(x) == 0L || (is.atomic(x) && length(x) == 1L && is.na(x))) {
      return(default)
    }
    as.character(x[[1L]])
  }
  scalar_num <- function(x, default = -1) {
    if (is.null(x) || length(x) == 0L || (is.atomic(x) && length(x) == 1L && is.na(x))) {
      return(default)
    }
    value <- suppressWarnings(as.numeric(x[[1L]]))
    if (is.na(value)) default else value
  }
  add_run_cols <- function(df) {
    if (!is.data.frame(df) || nrow(df) == 0L) {
      return(df)
    }
    df$profile <- result$profile %||% NA_character_
    df$run_id <- run_id
    df$fetched_at <- format(Sys.time(), "%Y-%m-%d %H:%M:%S")
    df
  }
  save_table <- function(df, table_name) {
    if (!is.data.frame(df) || nrow(df) == 0L) {
      return(FALSE)
    }
    tryCatch({
      load_df_to_clickhouse(df, table_name, conn = conn, overwrite = FALSE, append = TRUE)
      TRUE
    }, error = function(e) {
      warning("Failed to save ", table_name, " to ClickHouse: ", conditionMessage(e))
      FALSE
    })
  }

  metrics <- add_run_cols(result$repo_metrics)
  findings <- add_run_cols(result$findings)
  overall <- add_run_cols(result$overall)
  if (is.data.frame(overall) && nrow(overall) > 0L && "value" %in% names(overall)) {
    overall$value <- ifelse(is.na(overall$value), -1, overall$value)
  }

  ai_review <- result$ai_review %||% list()
  ai_summary <- data.frame()
  ai_selected <- data.frame()
  if (is.list(ai_review) && length(ai_review) > 0L) {
    parsed <- ai_review$parsed %||% list()
    ai_summary <- data.frame(
      status = scalar_text(ai_review$status, "unknown"),
      message = scalar_text(ai_review$message, ""),
      score = scalar_num(parsed$score),
      confidence = scalar_num(parsed$confidence),
      readability_score = scalar_num(parsed$readability_score),
      best_practice_score = scalar_num(parsed$best_practice_score),
      structure_score = scalar_num(parsed$structure_score),
      ai_generated_likelihood = scalar_num(parsed$ai_generated_likelihood),
      summary = scalar_text(parsed$summary, ""),
      strengths = paste(as.character(unlist(parsed$strengths %||% character(), use.names = FALSE)), collapse = " | "),
      weaknesses = paste(as.character(unlist(parsed$weaknesses %||% character(), use.names = FALSE)), collapse = " | "),
      recommendations = paste(as.character(unlist(parsed$recommendations %||% character(), use.names = FALSE)), collapse = " | "),
      stringsAsFactors = FALSE
    )
    ai_selected <- ai_review$selected_files %||% data.frame()
  }
  ai_summary <- add_run_cols(ai_summary)
  ai_selected <- add_run_cols(ai_selected)

  saved <- c(
    repo_metrics = save_table(metrics, "github_code_quality_repo_metrics"),
    findings = save_table(findings, "github_code_quality_findings"),
    overall = save_table(overall, "github_code_quality_overall"),
    ai_review = save_table(ai_summary, "github_code_quality_ai_review"),
    ai_selected_files = save_table(ai_selected, "github_code_quality_ai_selected_files")
  )
  invisible(any(saved))
}

analyze_code_quality_profile <- function(profile,
                                         token,
                                         conn = NULL,
                                         progress = function(value, label = NULL) NULL) {
  username <- quality_extract_username(profile)
  if (is.null(username) || !nzchar(username)) {
    stop("Could not extract GitHub username from target.", call. = FALSE)
  }
  if (!requireNamespace("gh", quietly = TRUE)) {
    stop("Package 'gh' is required for code quality analysis.", call. = FALSE)
  }

  progress(8, "Code quality: repository inventory")
  repos <- quality_fetch_repositories(username, token = token)
  repos_total <- nrow(repos)
  if (repos_total == 0L) {
    stop("No repositories were returned by GitHub for code quality analysis.", call. = FALSE)
  }

  include_archived <- quality_env_flag("GITHOUND_QUALITY_INCLUDE_ARCHIVED", default = FALSE)
  include_forks <- quality_env_flag("GITHOUND_QUALITY_INCLUDE_FORKS", default = FALSE)
  max_repos <- quality_env_int("GITHOUND_QUALITY_MAX_REPOS", 25L, min_value = 1L)

  scan_repos <- repos
  if (!include_archived && "archived" %in% names(scan_repos)) {
    archived <- as.logical(scan_repos$archived)
    scan_repos <- scan_repos[is.na(archived) | !archived, , drop = FALSE]
  }
  if (!include_forks && "fork" %in% names(scan_repos)) {
    fork <- as.logical(scan_repos$fork)
    scan_repos <- scan_repos[is.na(fork) | !fork, , drop = FALSE]
  }
  if (nrow(scan_repos) > 0L) {
    pushed <- suppressWarnings(as.POSIXct(scan_repos$pushed_at, tz = "UTC", format = "%Y-%m-%dT%H:%M:%SZ"))
    ord <- order(is.na(pushed), pushed, decreasing = TRUE)
    scan_repos <- scan_repos[ord, , drop = FALSE]
  }
  if (nrow(scan_repos) > max_repos) {
    scan_repos <- scan_repos[seq_len(max_repos), , drop = FALSE]
  }

  progress(20, paste0("Code quality: selected ", nrow(scan_repos), "/", repos_total, " repositories"))
  scanned <- quality_analyze_repositories(
    scan_repos,
    token = token,
    progress = progress,
    base_progress = 25,
    end_progress = 82
  )

  metrics <- scanned$metrics
  findings <- scanned$findings
  ai_review <- tryCatch(
    quality_run_ai_review(
      username = username,
      scan_repos = scan_repos,
      scanned = scanned,
      conn = conn,
      token = token,
      progress = progress
    ),
    error = function(e) list(status = "error", message = conditionMessage(e))
  )
  ai_findings <- quality_ai_review_findings_df(ai_review)
  if (is.data.frame(ai_findings) && nrow(ai_findings) > 0L) {
    findings <- rbind(findings, ai_findings)
  }
  if (is.data.frame(findings) && nrow(findings) > 0L) {
    severity_order <- match(findings$severity, c("high", "medium", "low"))
    findings <- findings[order(severity_order, -quality_num(findings$penalty), findings$repository), , drop = FALSE]
    rownames(findings) <- NULL
  }

  overall <- quality_build_overall(metrics, findings, repos_total = repos_total, repos_scanned = nrow(scan_repos), ai_review = ai_review)
  result <- list(
    profile = username,
    run_id = paste0(username, "_", format(Sys.time(), "%Y%m%d%H%M%S")),
    repositories = repos,
    scanned_repositories = scan_repos,
    repo_metrics = metrics,
    findings = findings,
    languages = scanned$languages,
    file_inventory = scanned$files,
    ai_review = ai_review,
    overall = overall,
    config = list(
      max_repos = max_repos,
      include_archived = include_archived,
      include_forks = include_forks,
      workers = quality_env_int("GITHOUND_QUALITY_WORKERS", 2L, min_value = 1L),
      max_tree_files = quality_env_int("GITHOUND_QUALITY_MAX_TREE_FILES", 5000L, min_value = 100L),
      include_private = quality_env_flag("GITHOUND_QUALITY_INCLUDE_PRIVATE", default = TRUE),
      ai_enabled = quality_env_flag("GITHOUND_QUALITY_AI_ENABLED", default = TRUE),
      ai_weight = quality_env_num("GITHOUND_QUALITY_AI_WEIGHT", 0.2, min_value = 0, max_value = 0.8),
      ai_code_lines = quality_env_int("GITHOUND_QUALITY_AI_CODE_LINES", 100L, min_value = 10L),
      ai_files_per_repo = quality_env_int("GITHOUND_QUALITY_AI_FILES_PER_REPO", 4L, min_value = 1L),
      ai_model = quality_ai_model()
    )
  )

  progress(90, "Code quality: saving evidence")
  quality_save_to_clickhouse(result, conn = conn)
  progress(95, "Code quality: report assembly")
  result
}
