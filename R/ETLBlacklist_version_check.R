is_version_vulnerable <- function(entry, version = NULL) {

  # Без версии — всё, что соответствует пакету, уязвимо
  if (is.null(version)) return(TRUE)

  # 1) Явный список versions
  if (!is.null(entry$versions) && length(entry$versions) > 0) {
    if (version %in% entry$versions) {
      return(TRUE)
    }
  }

  cat("hui3")
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
            if (!is.null(introduced) && utils::compareVersion(version, introduced) < 0)
              next

            # >= fixed
            if (!is.null(fixed) && utils::compareVersion(version, fixed) >= 0)
              next

            # соответствует
            return(TRUE)
          }
        }
      }
    }
  }

  return(FALSE)
}
