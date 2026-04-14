.syft_collect_components <- function(component, acc = list(), idx = 0L) {
  if (is.null(component)) return(list(items = acc, idx = idx))

  idx <- idx + 1L
  acc[[idx]] <- data.frame(
    bom_ref = .osv_to_scalar(component[["bom-ref"]]),
    type = .osv_to_scalar(component$type),
    name = .osv_to_scalar(component$name),
    version = .osv_to_scalar(component$version),
    purl = .osv_to_scalar(component$purl),
    stringsAsFactors = FALSE
  )

  nested <- component$components
  if (!is.null(nested) && length(nested) > 0L) {
    for (child in nested) {
      res <- .syft_collect_components(child, acc = acc, idx = idx)
      acc <- res$items
      idx <- res$idx
    }
  }

  list(items = acc, idx = idx)
}

#' Generate CycloneDX JSON SBOM using Syft.
#'
#' @param target_path Path to directory/file/image for scanning.
#' @param syft_path Path to `syft` executable.
#' @param output_file Output path for CycloneDX JSON SBOM.
#' @param syft_timeout_sec Timeout for syft process in seconds (`0` means no timeout).
#' @param syft_excludes Character vector of syft `--exclude` patterns.
#'
#' @return Absolute path to generated SBOM JSON.
#' @export
generate_syft_cyclonedx_sbom <- function(
    target_path,
    syft_path = "syft.exe",
    output_file = tempfile(pattern = "sbom-", fileext = ".cyclonedx.json"),
    syft_timeout_sec = 0L,
    syft_excludes = NULL
) {
  stopifnot(is.character(target_path), length(target_path) == 1L, nzchar(target_path))
  stopifnot(is.character(syft_path), length(syft_path) == 1L, nzchar(syft_path))
  stopifnot(is.character(output_file), length(output_file) == 1L, nzchar(output_file))
  stopifnot(is.numeric(syft_timeout_sec), length(syft_timeout_sec) == 1L, syft_timeout_sec >= 0)

  if (!file.exists(target_path)) {
    stop("Scan target does not exist: ", target_path, call. = FALSE)
  }

  syft_abs <- if (file.exists(syft_path)) {
    normalizePath(syft_path, winslash = "/", mustWork = FALSE)
  } else {
    syft_path
  }

  out_dir <- dirname(output_file)
  if (!dir.exists(out_dir)) dir.create(out_dir, recursive = TRUE, showWarnings = FALSE)

  target_abs <- normalizePath(target_path, winslash = "/", mustWork = FALSE)
  output_abs <- normalizePath(output_file, winslash = "/", mustWork = FALSE)

  .syft_cli_path <- function(path, must_exist = FALSE) {
    out <- normalizePath(path, winslash = "/", mustWork = must_exist)
    if (.Platform$OS.type == "windows") {
      short <- tryCatch(utils::shortPathName(out), error = function(e) "")
      if (is.character(short) && length(short) == 1L && nzchar(short)) {
        out <- normalizePath(short, winslash = "/", mustWork = FALSE)
      }
    }
    out
  }

  target_cli <- .syft_cli_path(target_abs, must_exist = TRUE)
  output_cli <- .syft_cli_path(output_abs, must_exist = FALSE)

  common_args <- c(
    "-o",
    paste0("cyclonedx-json=", output_cli)
  )
  if (!is.null(syft_excludes) && length(syft_excludes) > 0L) {
    excludes <- as.character(syft_excludes)
    excludes <- excludes[nzchar(trimws(excludes))]
    if (length(excludes) > 0L) {
      for (ex in excludes) {
        common_args <- c(common_args, "--exclude", ex)
      }
    }
  }

  .run_syft <- function(args) {
    run_args <- args
    if (.Platform$OS.type == "windows") {
      run_args <- vapply(args, function(x) shQuote(x, type = "cmd"), character(1))
    }
    out <- tryCatch(
      system2(
        syft_abs,
        args = run_args,
        stdout = TRUE,
        stderr = TRUE,
        timeout = as.integer(syft_timeout_sec)
      ),
      error = function(e) structure(character(0), status = 1L, message = e$message)
    )
    list(
      out = out,
      status = attr(out, "status"),
      ok = is.null(attr(out, "status")) || identical(as.integer(attr(out, "status")), 0L)
    )
  }

  # Some syft builds do not support interspersed args:
  # options must go before SOURCE, otherwise "-o" is treated as a positional arg.
  cmd1 <- .run_syft(c(common_args, target_cli))
  cmd_out <- cmd1$out
  status <- cmd1$status

  if (!isTRUE(cmd1$ok) || !file.exists(output_file)) {
    # Compatibility fallback for CLI variants that require `scan` subcommand.
    cmd2 <- .run_syft(c("scan", common_args, target_cli))
    if (isTRUE(cmd2$ok) && file.exists(output_file)) {
      cmd_out <- cmd2$out
      status <- cmd2$status
    } else {
      if (!is.null(status) && status == 124L) {
        stop(
          "Syft scan timed out after ", as.integer(syft_timeout_sec), " seconds.",
          call. = FALSE
        )
      }
      stop(
        "Syft failed in both invocation modes.\n",
        "Mode 1 (`syft [flags] SOURCE`) status: ", .osv_null(status, NA_integer_), "\n",
        "Mode 1 output: ", paste(cmd1$out, collapse = "\n"), "\n",
        "Mode 2 (`syft scan [flags] SOURCE`) status: ", .osv_null(cmd2$status, NA_integer_), "\n",
        "Mode 2 output: ", paste(cmd2$out, collapse = "\n"),
        call. = FALSE
      )
    }
  }

  if (!is.null(status) && status == 124L) {
    stop(
      "Syft scan timed out after ", as.integer(syft_timeout_sec), " seconds.",
      call. = FALSE
    )
  }
  if (!is.null(status) && status != 0L) {
    stop(
      "Syft failed with status ", status, ". Output: ",
      paste(cmd_out, collapse = "\n"),
      call. = FALSE
    )
  }

  if (!file.exists(output_file)) {
    stop("Syft finished without creating SBOM file: ", output_file, call. = FALSE)
  }

  normalizePath(output_file, winslash = "/", mustWork = FALSE)
}

#' Parse CycloneDX JSON SBOM and return a flat component table.
#'
#' @param sbom_file Path to CycloneDX JSON.
#'
#' @return Data frame with SBOM components.
#' @export
parse_cyclonedx_sbom <- function(sbom_file) {
  stopifnot(is.character(sbom_file), length(sbom_file) == 1L, nzchar(sbom_file))
  if (!file.exists(sbom_file)) {
    stop("SBOM file not found: ", sbom_file, call. = FALSE)
  }

  sbom <- jsonlite::fromJSON(sbom_file, simplifyVector = FALSE)
  components <- list()
  idx <- 0L

  meta_component <- sbom$metadata$component
  if (!is.null(meta_component)) {
    res <- .syft_collect_components(meta_component, components, idx)
    components <- res$items
    idx <- res$idx
  }

  bom_components <- sbom$components
  if (!is.null(bom_components) && length(bom_components) > 0L) {
    for (comp in bom_components) {
      res <- .syft_collect_components(comp, components, idx)
      components <- res$items
      idx <- res$idx
    }
  }

  if (length(components) == 0L) {
    return(data.frame())
  }

  out <- unique(do.call(rbind, components))
  rownames(out) <- NULL
  out
}

#' Scan CycloneDX components against OSV database.
#'
#' @param components Data frame from `parse_cyclonedx_sbom()`.
#' @param osv_db Prepared OSV DB object or path.
#' @param include_uncertain Include uncertain version matches.
#'
#' @return Data frame with vulnerabilities for SBOM components.
#' @export
scan_sbom_components_with_osv <- function(components, osv_db, include_uncertain = TRUE) {
  stopifnot(is.data.frame(components))
  stopifnot(is.logical(include_uncertain), length(include_uncertain) == 1L)

  if (nrow(components) == 0L) {
    return(data.frame())
  }

  required_cols <- c("name", "version", "purl")
  missing_cols <- setdiff(required_cols, names(components))
  if (length(missing_cols) > 0L) {
    stop("`components` is missing columns: ", paste(missing_cols, collapse = ", "), call. = FALSE)
  }

  searchable <- components[!is.na(components$purl) & nzchar(components$purl), , drop = FALSE]
  if (nrow(searchable) == 0L) {
    return(data.frame())
  }

  query_version <- vapply(seq_len(nrow(searchable)), function(i) {
    ver <- searchable$version[[i]]
    if (is.null(ver) || is.na(ver)) return(NA_character_)
    ver <- trimws(as.character(ver))
    if (!nzchar(ver)) NA_character_ else ver
  }, character(1))
  query_key <- paste0(searchable$purl, "\r", ifelse(is.na(query_version), "<NA>", query_version))
  unique_keys <- unique(query_key)
  lookup <- vector("list", length(unique_keys))
  names(lookup) <- unique_keys
  lookup_errors <- character(0)

  for (k in unique_keys) {
    i <- match(k, query_key)
    version <- if (is.na(query_version[[i]])) NULL else query_version[[i]]
    lookup[[k]] <- tryCatch(
      search_osv_vulnerabilities(
        osv_db = osv_db,
        purl = searchable$purl[[i]],
        version = version,
        include_uncertain = include_uncertain
      ),
      error = function(e) {
        lookup_errors <<- c(
          lookup_errors,
          paste0("purl=", searchable$purl[[i]], "; version=", .osv_null(version, "<NA>"), "; error=", conditionMessage(e))
        )
        data.frame()
      }
    )
  }

  if (length(lookup_errors) > 0L) {
    warning(
      length(lookup_errors),
      " component queries failed during OSV search and were skipped. First error: ",
      lookup_errors[[1]],
      call. = FALSE
    )
  }

  out <- list()
  idx <- 0L

  for (i in seq_len(nrow(searchable))) {
    row <- searchable[i, , drop = FALSE]
    matches <- lookup[[query_key[[i]]]]

    if (nrow(matches) == 0L) next

    matches$component_bom_ref <- .osv_null(row$bom_ref[[1]], NA_character_)
    matches$component_type <- .osv_null(row$type[[1]], NA_character_)
    matches$component_name <- .osv_null(row$name[[1]], NA_character_)
    matches$component_version <- .osv_null(row$version[[1]], NA_character_)
    matches$component_purl <- .osv_null(row$purl[[1]], NA_character_)

    idx <- idx + 1L
    out[[idx]] <- matches
  }

  if (length(out) == 0L) {
    return(data.frame())
  }

  result <- do.call(rbind, out)
  rownames(result) <- NULL
  result
}

#' Generate SBOM via Syft and scan components against OSV.
#'
#' @param target_path Path to scan with Syft.
#' @param osv_db Prepared OSV DB object or path.
#' @param syft_path Path to `syft` executable.
#' @param keep_sbom Keep generated SBOM file.
#' @param include_uncertain Include uncertain version matches.
#' @param syft_timeout_sec Timeout for syft process in seconds (`0` means no timeout).
#' @param syft_excludes Character vector of syft `--exclude` patterns.
#'
#' @return List with `sbom_file`, `components`, `vulnerabilities`.
#' @export
scan_path_with_syft_and_osv <- function(
    target_path,
    osv_db,
    syft_path = "syft.exe",
    keep_sbom = FALSE,
    include_uncertain = TRUE,
    syft_timeout_sec = 0L,
    syft_excludes = NULL
) {
  stopifnot(is.logical(keep_sbom), length(keep_sbom) == 1L)
  stopifnot(is.logical(include_uncertain), length(include_uncertain) == 1L)
  stopifnot(is.numeric(syft_timeout_sec), length(syft_timeout_sec) == 1L, syft_timeout_sec >= 0)

  sbom_file <- generate_syft_cyclonedx_sbom(
    target_path = target_path,
    syft_path = syft_path,
    syft_timeout_sec = syft_timeout_sec,
    syft_excludes = syft_excludes
  )

  if (!isTRUE(keep_sbom)) {
    on.exit(unlink(sbom_file, force = TRUE), add = TRUE)
  }

  components <- parse_cyclonedx_sbom(sbom_file)
  vulnerabilities <- scan_sbom_components_with_osv(
    components = components,
    osv_db = osv_db,
    include_uncertain = include_uncertain
  )

  list(
    sbom_file = if (isTRUE(keep_sbom)) sbom_file else NULL,
    components = components,
    vulnerabilities = vulnerabilities
  )
}
