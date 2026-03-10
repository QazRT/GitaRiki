is_version_vulnerable <- function(entry, version = NULL) {
  # Без версии — всё, что соответствует пакету, уязвимо
  if (is.null(version)) {
    return(TRUE)
  }

  # 1) Явный список versions
  if (!is.null(entry$versions) && length(entry$versions) > 0) {
    if (version %in% entry$versions) {
      return(TRUE)
    }
  }

  # 2) Диапазоны introduced/fixed
  if ("affected" %in% names(entry)) {
    for (aff in entry$affected) {
      if (!is.null(aff$ranges)) {
        for (range in aff$ranges) {
          if (!is.null(range$events)) {
            introduced <- NULL
            fixed <- NULL

            for (ev in range$events) {
              if (!is.null(ev$introduced)) introduced <- ev$introduced
              if (!is.null(ev$fixed)) fixed <- ev$fixed
            }

            # Логика:
            # introduced <= version < fixed

            # ниже introduced
            compare <-   tryCatch(
              safe_compare_version(version, introduced),
              warning = function(w) -1L,
              error   = function(e) -1L
            )
            if (!is.null(introduced) && compare < 0) {
              next
            }
            # >= fixed
            if (!is.null(fixed) && compare >= 0) {
              next
            }

            # соответствует
            return(TRUE)
          }
        }
      }
    }
  }

  return(FALSE)
}



safe_compare_version <- function(v, i) {

  # привести к строке
  v <- as.character(v[1])
  i <- as.character(i[1])

  # если NA или пустое, считаем v < i
  if (is.na(v) || is.na(i) || v == "" || i == "") return(-1L)

  # очистка основной версии (числа и точки)
  clean_version <- function(x) {
    x <- sub("^v", "", trimws(x))
    # отделяем числовую часть и суффикс (alpha, beta, rc, dev)
    m <- regmatches(x, regexpr("^[0-9]+(\\.[0-9]+)*", x))
    if (length(m) == 0 || m == "") m <- "0"
    m
  }

  # извлечение суффикса
  extract_suffix <- function(x) {
    s <- sub("^[0-9]+(\\.[0-9]+)*", "", x)
    s <- tolower(trimws(s))
    if (s == "") s <- "z"   # пустой суффикс — самый "поздний"
    s
  }

  v_main <- clean_version(v)
  i_main <- clean_version(i)

  v_suf <- extract_suffix(v)
  i_suf <- extract_suffix(i)

  # сначала сравниваем числовые части
  cmp_main <- tryCatch(
    utils::compareVersion(v_main, i_main),
    error = function(e) -1L,
    warning = function(w) -1L
  )

  if (cmp_main != 0) return(cmp_main)

  # если числовые равны, сравниваем суффиксы (alpha < beta < rc < z)
  suffix_order <- c("alpha"=1, "beta"=2, "rc"=3, "z"=4)

  get_suf_rank <- function(s) {
    # ищем ключ в order
    keys <- names(suffix_order)
    for (k in keys) {
      if (grepl(k, s)) return(suffix_order[k])
    }
    # если не найдено — ставим самый поздний
    return(suffix_order["z"])
  }

  cmp_suf <- sign(get_suf_rank(v_suf) - get_suf_rank(i_suf))

  return(cmp_suf)
}
