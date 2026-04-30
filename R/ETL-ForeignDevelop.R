# Function to fetch extended GitHub user data with ClickHouse caching.

get_github_user_info <- function(profile,
                                 token = NULL,
                                 include_commit_stats = FALSE,
                                 max_commits = 1000,
                                 collect_commits = TRUE,
                                 commit_workers = NULL,
                                 conn = NULL) {
  if (is.null(conn)) {
    stop("Необходимо передать объект подключения ClickHouse в параметре 'conn'.")
  }
  
  required_packages <- c("gh", "dplyr", "purrr", "httr", "jsonlite")
  for (pkg in required_packages) {
    if (!requireNamespace(pkg, quietly = TRUE)) {
      stop("Пакет '", pkg, "' не установлен. Установите вручную: install.packages('", pkg, "')")
    }
  }
  
  library(gh)
  library(dplyr)
  library(purrr)
  library(httr)
  
  `%||%` <- function(x, y) if (is.null(x)) y else x

  resolve_commit_workers <- function(repo_count, override = NULL) {
    repo_count <- suppressWarnings(as.integer(repo_count))
    if (is.na(repo_count) || repo_count < 1L) {
      return(1L)
    }

    raw_value <- if (!is.null(override)) {
      as.character(override[[1]])
    } else {
      env_names <- c("GITHOUND_COMMIT_WORKERS", "GITHOUND_COMMIT_COLLECTION_WORKERS")
      values <- Sys.getenv(env_names, unset = "")
      values <- values[nzchar(values)]
      if (length(values) > 0L) values[[1L]] else ""
    }

    workers <- suppressWarnings(as.integer(raw_value))
    if (is.na(workers) || workers < 1L) {
      workers <- 1L
    }

    max(1L, min(workers, repo_count))
  }
  
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
    NULL
  }
  
  make_typed_na <- function(template, n) {
    if (inherits(template, "POSIXct") || inherits(template, "POSIXt")) {
      return(as.POSIXct(rep(NA_real_, n), origin = "1970-01-01", tz = "UTC"))
    }
    if (inherits(template, "Date")) {
      return(as.Date(rep(NA_real_, n), origin = "1970-01-01"))
    }
    if (is.character(template)) return(rep(NA_character_, n))
    if (is.integer(template)) return(rep(NA_integer_, n))
    if (is.numeric(template)) return(rep(NA_real_, n))
    if (is.logical(template)) return(rep(NA, n))
    rep(NA_character_, n)
  }
  
  repo_to_df <- function(repo_list) {
    if (length(repo_list) == 0) {
      return(data.frame())
    }
    
    bind_rows(lapply(repo_list, function(r) {
      data.frame(
        full_name = r$full_name %||% NA,
        html_url = r$html_url %||% NA,
        description = r$description %||% NA,
        created_at = r$created_at %||% NA,
        updated_at = r$updated_at %||% NA,
        language = r$language %||% NA,
        stargazers_count = r$stargazers_count %||% 0,
        forks_count = r$forks_count %||% 0,
        stringsAsFactors = FALSE
      )
    }))
  }
  
  repo_owner <- function(repo_full_name) {
    vapply(
      repo_full_name,
      function(x) {
        if (is.na(x) || !nzchar(x)) {
          return(NA_character_)
        }
        strsplit(x, "/", fixed = TRUE)[[1]][1]
      },
      character(1)
    )
  }
  
  read_cached_rows <- function(table_name, where_sql) {
    table_exists <- exists("table_exists_custom", mode = "function") &&
      table_exists_custom(conn, table_name)
    
    if (!table_exists) {
      return(data.frame())
    }
    
    sql <- paste0("SELECT * FROM ", table_name, " WHERE ", where_sql)
    tryCatch(
      query_clickhouse(conn, sql),
      error = function(e) {
        warning("Не удалось загрузить данные из ClickHouse для таблицы ", table_name, ": ", e$message)
        data.frame()
      }
    )
  }
  
  save_incremental <- function(df,
                               tbl_name,
                               key_cols,
                               latest_key_cols = key_cols,
                               compare_cols = key_cols) {
    existing_all <- read_cached_rows(tbl_name, paste0("profile = '", escaped_username, "'"))
    
    if ("fetched_at" %in% names(existing_all)) {
      existing_all$fetched_at <- as.character(existing_all$fetched_at)
    }
    
    if (nrow(existing_all) > 0) {
      message("В ClickHouse уже есть данные в ", tbl_name, ": ", nrow(existing_all), " записей.")
    }
    
    if (nrow(df) == 0) {
      if (length(latest_key_cols) > 0 && nrow(existing_all) > 0) {
        return(
          existing_all %>%
            arrange(desc(fetched_at)) %>%
            distinct(across(all_of(latest_key_cols)), .keep_all = TRUE)
        )
      }
      return(existing_all)
    }
    
    df <- df %>%
      mutate(
        profile = username,
        fetched_at = format(Sys.Date(), "%Y-%m-%d")
      )
    
    if ("fetched_at" %in% names(df)) {
      df$fetched_at <- as.character(df$fetched_at)
    }
    
    for (col in unique(c(compare_cols, latest_key_cols))) {
      if (!col %in% names(existing_all)) {
        existing_all[[col]] <- make_typed_na(df[[col]], nrow(existing_all))
      }
      if (!col %in% names(df)) {
        df[[col]] <- make_typed_na(existing_all[[col]], nrow(df))
      }
    }
    
    latest_existing <- if (nrow(existing_all) > 0) {
      existing_all %>%
        arrange(desc(fetched_at)) %>%
        distinct(across(all_of(latest_key_cols)), .keep_all = TRUE)
    } else {
      existing_all
    }
    
    df_for_compare <- df %>%
      distinct(across(all_of(compare_cols)), .keep_all = TRUE)
    
    new_rows <- if (nrow(latest_existing) > 0) {
      anti_join(df_for_compare, latest_existing, by = compare_cols)
    } else {
      df_for_compare
    }
    
    if (nrow(new_rows) > 0) {
      message("Сохраняю новые записи в ", tbl_name, ": ", nrow(new_rows))
      load_df_to_clickhouse(
        df = new_rows,
        table_name = tbl_name,
        conn = conn,
        overwrite = FALSE,
        append = TRUE
      )
      existing_all <- bind_rows(existing_all, new_rows)
    } else {
      message("Новых записей для таблицы ", tbl_name, " не найдено.")
    }
    
    existing_all %>%
      arrange(desc(fetched_at)) %>%
      distinct(across(all_of(latest_key_cols)), .keep_all = TRUE)
  }
  
  collect_event_repos <- function(username, token = NULL, max_pages = 3) {
    repo_names <- character()
    
    for (page in seq_len(max_pages)) {
      events <- tryCatch({
        gh::gh(
          "GET /users/{username}/events/public",
          username = username,
          per_page = 100,
          page = page,
          .token = token,
          .progress = FALSE
        )
      }, error = function(e) {
        warning("Не удалось получить публичные события пользователя ", username, ": ", e$message)
        NULL
      })
      
      if (is.null(events) || length(events) == 0) {
        break
      }
      
      page_repos <- vapply(
        events,
        function(evt) evt$repo$name %||% NA_character_,
        character(1)
      )
      
      repo_names <- c(repo_names, page_repos)
    }
    
    unique(repo_names[!is.na(repo_names) & nzchar(repo_names)])
  }
  
  collect_contributed_repos <- function(username, token = NULL) {
    if (is.null(token) || !nzchar(token)) {
      return(character())
    }
    
    query <- paste(
      "query($login: String!, $cursor: String) {",
      "  user(login: $login) {",
      "    repositoriesContributedTo(",
      "      first: 100,",
      "      after: $cursor,",
      "      includeUserRepositories: true,",
      "      contributionTypes: [COMMIT]",
      "    ) {",
      "      nodes {",
      "        nameWithOwner",
      "      }",
      "      pageInfo {",
      "        hasNextPage",
      "        endCursor",
      "      }",
      "    }",
      "  }",
      "}",
      sep = "\n"
    )
    
    repo_names <- character()
    cursor <- NULL
    
    repeat {
      response <- tryCatch({
        httr::POST(
          url = "https://api.github.com/graphql",
          httr::add_headers(
            Authorization = paste("bearer", token),
            Accept = "application/vnd.github+json"
          ),
          encode = "json",
          body = list(
            query = query,
            variables = list(
              login = username,
              cursor = cursor
            )
          )
        )
      }, error = function(e) {
        warning("Не удалось получить список репозиториев с contributions для пользователя ", username, ": ", e$message)
        NULL
      })
      
      if (is.null(response)) {
        break
      }
      
      payload <- tryCatch(
        httr::content(response, as = "parsed", type = "application/json", encoding = "UTF-8"),
        error = function(e) NULL
      )
      
      if (is.null(payload) || !is.null(payload$errors) || is.null(payload$data$user)) {
        break
      }
      
      repo_block <- payload$data$user$repositoriesContributedTo
      nodes <- repo_block$nodes %||% list()
      if (length(nodes) > 0) {
        repo_names <- c(
          repo_names,
          vapply(nodes, function(node) node$nameWithOwner %||% NA_character_, character(1))
        )
      }
      
      page_info <- repo_block$pageInfo
      if (is.null(page_info) || !isTRUE(page_info$hasNextPage)) {
        break
      }
      
      cursor <- page_info$endCursor %||% NULL
      if (is.null(cursor) || !nzchar(cursor)) {
        break
      }
    }
    
    unique(repo_names[!is.na(repo_names) & nzchar(repo_names)])
  }
  
  collect_commits_from_repos <- function(repo_list,
                                         username,
                                         token,
                                         include_commit_stats = FALSE,
                                         max_commits = 1000) {
    repo_list <- unique(repo_list[!is.na(repo_list) & nzchar(repo_list)])
    if (length(repo_list) == 0) {
      return(data.frame())
    }
    
    commits_accumulator <- list()
    commit_counter <- 0L

    commit_worker_count <- resolve_commit_workers(length(repo_list), commit_workers)
    if (commit_worker_count > 1L && requireNamespace("parallel", quietly = TRUE)) {
      message(
        "Collecting user-info commits in parallel: ",
        commit_worker_count,
        " workers for ",
        length(repo_list),
        " repositories."
      )

      cl <- parallel::makeCluster(commit_worker_count)
      on.exit(parallel::stopCluster(cl), add = TRUE)
      parallel::clusterEvalQ(cl, {
        library(gh)
        library(dplyr)
        library(purrr)
        library(httr)
        NULL
      })
      parallel::clusterExport(
        cl,
        varlist = c("username", "token", "include_commit_stats", "%||%"),
        envir = environment()
      )
      repo_results <- parallel::parLapply(
        cl = cl,
        X = as.list(repo_list),
        fun = function(repo) {
          tryCatch({
            search_items <- tryCatch({
              search_result <- gh::gh(
                "GET /search/commits",
                q = paste0("author:", username, " repo:", repo),
                per_page = 100,
                .limit = Inf,
                .token = token,
                .progress = FALSE
              )
              search_result$items %||% list()
            }, error = function(e) {
              NULL
            })

            repo_df <- if (length(search_items) > 0) {
              map_df(search_items, function(cmt) {
                data.frame(
                  repository = repo,
                  sha = cmt$sha %||% NA,
                  date = cmt$commit$author$date %||% NA,
                  message = cmt$commit$message %||% NA,
                  url = cmt$html_url %||% NA,
                  stringsAsFactors = FALSE
                )
              })
            } else {
              repo_commits <- tryCatch({
                gh::gh(
                  "GET /repos/{repo}/commits",
                  repo = repo,
                  author = username,
                  .limit = Inf,
                  .token = token,
                  .progress = FALSE
                )
              }, error = function(e) {
                NULL
              })

              if (is.null(repo_commits) || length(repo_commits) == 0) {
                data.frame()
              } else {
                map_df(repo_commits, function(cmt) {
                  data.frame(
                    repository = repo,
                    sha = cmt$sha %||% NA,
                    date = cmt$commit$author$date %||% NA,
                    message = cmt$commit$message %||% NA,
                    url = cmt$html_url %||% NA,
                    stringsAsFactors = FALSE
                  )
                })
              }
            } %>%
              distinct(repository, sha, .keep_all = TRUE)

            if (nrow(repo_df) > 0 && isTRUE(include_commit_stats)) {
              stats_df <- map_df(repo_df$sha, function(sha) {
                tryCatch({
                  cmt_detail <- gh::gh(
                    "GET /repos/{repo}/commits/{sha}",
                    repo = repo,
                    sha = sha,
                    .token = token
                  )
                  stats <- cmt_detail$stats %||% list(additions = 0, deletions = 0, total = 0)
                  data.frame(
                    repository = repo,
                    sha = sha,
                    additions = stats$additions %||% 0,
                    deletions = stats$deletions %||% 0,
                    changes = stats$total %||% 0,
                    stringsAsFactors = FALSE
                  )
                }, error = function(e) {
                  data.frame(
                    repository = repo,
                    sha = sha,
                    additions = NA,
                    deletions = NA,
                    changes = NA,
                    stringsAsFactors = FALSE
                  )
                })
              }) %>%
                distinct(repository, sha, .keep_all = TRUE)

              repo_df <- repo_df %>%
                left_join(stats_df, by = c("repository", "sha"))
            }

            list(repo = repo, commits = repo_df, error = NULL)
          }, error = function(e) {
            list(repo = repo, commits = data.frame(), error = conditionMessage(e))
          })
        }
      )

      for (result in repo_results) {
        if (!is.null(result$error) && nzchar(result$error)) {
          warning("Error while collecting commits for ", result$repo, ": ", result$error)
          next
        }
        if (is.data.frame(result$commits) && nrow(result$commits) > 0L) {
          commits_accumulator[[result$repo]] <- result$commits
        }
      }

      if (length(commits_accumulator) == 0) {
        return(data.frame())
      }

      return(
        bind_rows(commits_accumulator) %>%
          distinct(repository, sha, .keep_all = TRUE) %>%
          head(max_commits)
      )
    } else if (commit_worker_count > 1L) {
      warning("Package 'parallel' is not available. Falling back to sequential commit collection.")
    }
    
    for (repo in repo_list) {
      if (commit_counter >= max_commits) {
        break
      }
      
      search_items <- tryCatch({
        search_result <- gh::gh(
          "GET /search/commits",
          q = paste0("author:", username, " repo:", repo),
          per_page = 100,
          .limit = Inf,
          .token = token,
          .progress = FALSE
        )
        search_result$items %||% list()
      }, error = function(e) {
        NULL
      })
      
      repo_df <- if (length(search_items) > 0) {
        map_df(search_items, function(cmt) {
          data.frame(
            repository = repo,
            sha = cmt$sha %||% NA,
            date = cmt$commit$author$date %||% NA,
            message = cmt$commit$message %||% NA,
            url = cmt$html_url %||% NA,
            stringsAsFactors = FALSE
          )
        })
      } else {
        repo_commits <- tryCatch({
          gh::gh(
            "GET /repos/{repo}/commits",
            repo = repo,
            author = username,
            .limit = Inf,
            .token = token,
            .progress = FALSE
          )
        }, error = function(e) {
          warning("Не удалось получить коммиты пользователя ", username, " в репозитории ", repo, ": ", e$message)
          NULL
        })
        
        if (is.null(repo_commits) || length(repo_commits) == 0) {
          data.frame()
        } else {
          map_df(repo_commits, function(cmt) {
            data.frame(
              repository = repo,
              sha = cmt$sha %||% NA,
              date = cmt$commit$author$date %||% NA,
              message = cmt$commit$message %||% NA,
              url = cmt$html_url %||% NA,
              stringsAsFactors = FALSE
            )
          })
        }
      } %>%
        distinct(repository, sha, .keep_all = TRUE)
      
      if (nrow(repo_df) == 0) {
        next
      }
      
      if (include_commit_stats) {
        stats_df <- map_df(repo_df$sha, function(sha) {
          tryCatch({
            cmt_detail <- gh::gh(
              "GET /repos/{repo}/commits/{sha}",
              repo = repo,
              sha = sha,
              .token = token
            )
            stats <- cmt_detail$stats %||% list(additions = 0, deletions = 0, total = 0)
            data.frame(
              repository = repo,
              sha = sha,
              additions = stats$additions %||% 0,
              deletions = stats$deletions %||% 0,
              changes = stats$total %||% 0,
              stringsAsFactors = FALSE
            )
          }, error = function(e) {
            warning("Не удалось получить статистику для коммита ", sha, " в репозитории ", repo, ": ", e$message)
            data.frame(
              repository = repo,
              sha = sha,
              additions = NA,
              deletions = NA,
              changes = NA,
              stringsAsFactors = FALSE
            )
          })
        }) %>%
          distinct(repository, sha, .keep_all = TRUE)
        
        repo_df <- repo_df %>%
          left_join(stats_df, by = c("repository", "sha"))
      }
      
      commits_accumulator[[repo]] <- repo_df
      commit_counter <- commit_counter + nrow(repo_df)
    }
    
    if (length(commits_accumulator) == 0) {
      return(data.frame())
    }
    
    bind_rows(commits_accumulator) %>%
      distinct(repository, sha, .keep_all = TRUE) %>%
      head(max_commits)
  }
  
  username <- extract_github_username(profile)
  if (is.null(username)) {
    stop("Не удалось извлечь имя пользователя из: ", profile)
  }
  escaped_username <- gsub("'", "''", username)
  
  message("Запрашиваю репозитории, принадлежащие пользователю ", username, "...")
  owned_repos <- tryCatch({
    repos <- gh::gh(
      "GET /users/{username}/repos",
      username = username,
      .limit = Inf,
      .token = token,
      .progress = TRUE
    )
    repo_to_df(repos)
  }, error = function(e) {
    warning("Не удалось получить репозитории: ", e$message)
    data.frame()
  })
  
  message("Запрашиваю подписки пользователя ", username, "...")
  subscriptions <- tryCatch({
    subs <- gh::gh(
      "GET /users/{username}/subscriptions",
      username = username,
      .limit = Inf,
      .token = token,
      .progress = TRUE
    )
    repo_to_df(subs)
  }, error = function(e) {
    warning("Не удалось получить подписки: ", e$message)
    data.frame()
  })
  
  message("Запрашиваю звёзды пользователя ", username, "...")
  starred <- tryCatch({
    stars <- gh::gh(
      "GET /users/{username}/starred",
      username = username,
      .limit = Inf,
      .token = token,
      .progress = TRUE
    )
    repo_to_df(stars)
  }, error = function(e) {
    warning("Не удалось получить звёзды: ", e$message)
    data.frame()
  })
  
  message("Выполняю поиск коммитов автора ", username, " (макс. ", max_commits, ")...")
  commits_df <- data.frame()
  activity_summary <- data.frame()
  
  if (!isTRUE(collect_commits)) {
    message("Skipping commit search in get_github_user_info(): collect_commits = FALSE.")
  } else if (is.null(token)) {
    warning("Поиск коммитов требует аутентификации. Без токена эта часть будет пропущена.")
  } else {
    tryCatch({
      search_result <- gh::gh(
        "GET /search/commits",
        q = paste0("author:", username),
        .token = token,
        .limit = max_commits,
        .progress = TRUE
      )
      
      items <- search_result$items %||% list()
      search_commits_df <- if (length(items) > 0) {
        map_df(items, function(cmt) {
          data.frame(
            repository = cmt$repository$full_name %||% NA,
            sha = cmt$sha %||% NA,
            date = cmt$commit$author$date %||% NA,
            message = cmt$commit$message %||% NA,
            url = cmt$html_url %||% NA,
            stringsAsFactors = FALSE
          )
        }) %>%
          distinct(repository, sha, .keep_all = TRUE)
      } else {
        data.frame()
      }
      
      event_repos <- collect_event_repos(username = username, token = token)
      contributed_repos <- collect_contributed_repos(username = username, token = token)
      
      if (length(event_repos) > 0) {
        message("Найдено репозиториев из публичных событий пользователя: ", length(event_repos))
      }
      if (length(contributed_repos) > 0) {
        message("Найдено репозиториев из GraphQL contributions: ", length(contributed_repos))
      }
      
      candidate_repos <- unique(c(
        owned_repos$full_name,
        subscriptions$full_name,
        starred$full_name,
        search_commits_df$repository,
        event_repos,
        contributed_repos
      ))
      candidate_repos <- candidate_repos[!is.na(candidate_repos) & nzchar(candidate_repos)]
      
      own_candidate_repos <- candidate_repos[repo_owner(candidate_repos) == username]
      foreign_candidate_repos <- candidate_repos[repo_owner(candidate_repos) != username]
      
      if (length(foreign_candidate_repos) > 0) {
        message("Дополнительно проверяю чужие репозитории, где пользователь мог коммитить (", length(foreign_candidate_repos), " шт.)...")
      }
      
      repo_scan_commits_df <- collect_commits_from_repos(
        repo_list = c(own_candidate_repos, foreign_candidate_repos),
        username = username,
        token = token,
        include_commit_stats = include_commit_stats,
        max_commits = max_commits
      )
      
      commits_df <- bind_rows(search_commits_df, repo_scan_commits_df) %>%
        distinct(repository, sha, .keep_all = TRUE) %>%
        head(max_commits)
      
      if (nrow(commits_df) > 0) {
        if (!"additions" %in% names(commits_df) && include_commit_stats) {
          stats_df <- collect_commits_from_repos(
            repo_list = unique(commits_df$repository),
            username = username,
            token = token,
            include_commit_stats = TRUE,
            max_commits = nrow(commits_df)
          )
          
          if (!is.data.frame(stats_df) || nrow(stats_df) == 0) {
            stats_df <- data.frame(
              repository = character(),
              sha = character(),
              additions = numeric(),
              deletions = numeric(),
              changes = numeric(),
              stringsAsFactors = FALSE
            )
          } else {
            for (col in c("repository", "sha", "additions", "deletions", "changes")) {
              if (!col %in% names(stats_df)) {
                stats_df[[col]] <- if (col %in% c("repository", "sha")) {
                  rep(NA_character_, nrow(stats_df))
                } else {
                  rep(NA_real_, nrow(stats_df))
                }
              }
            }
            
            stats_df <- stats_df %>%
              dplyr::select(repository, sha, additions, deletions, changes) %>%
              distinct(repository, sha, .keep_all = TRUE)
          }
          
          commits_df <- commits_df %>%
            left_join(stats_df, by = c("repository", "sha"))
        }
        
        if (include_commit_stats && all(c("additions", "deletions", "changes") %in% names(commits_df))) {
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
      } else {
        message("Коммитов не найдено.")
      }
    }, error = function(e) {
      warning("Ошибка при поиске коммитов: ", e$message)
    })
  }
  
  message("Сохраняю данные в ClickHouse...")
  
  owned_repos_result <- save_incremental(
    df = owned_repos,
    tbl_name = "github_owned_repos",
    key_cols = c("profile", "full_name")
  )
  
  subscriptions_result <- save_incremental(
    df = subscriptions,
    tbl_name = "github_subscriptions",
    key_cols = c("profile", "full_name")
  )
  
  starred_result <- save_incremental(
    df = starred,
    tbl_name = "github_starred",
    key_cols = c("profile", "full_name")
  )
  
  raw_commits_result <- save_incremental(
    df = commits_df,
    tbl_name = "github_raw_commits",
    key_cols = c("profile", "repository", "sha")
  )
  
  activity_compare_cols <- c("profile", "repository", "total_commits", "first_commit", "last_commit")
  if (include_commit_stats && nrow(activity_summary) > 0) {
    activity_compare_cols <- c(
      activity_compare_cols,
      "total_additions",
      "total_deletions",
      "total_changes"
    )
  }
  
  commits_activity_result <- save_incremental(
    df = activity_summary,
    tbl_name = "github_commits_activity",
    key_cols = activity_compare_cols,
    latest_key_cols = c("profile", "repository"),
    compare_cols = activity_compare_cols
  )
  
  result <- list(
    owned_repos = owned_repos_result,
    subscriptions = subscriptions_result,
    starred = starred_result,
    commits_activity = commits_activity_result,
    raw_commits = raw_commits_result
  )
  
  message("Сбор данных завершён.")
  result
}
