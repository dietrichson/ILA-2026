# =============================================================================
# run-balanced-sample.R
# =============================================================================
# Collect 1000 English-language pages per year from CC, balanced across TLDs.
#
# Strategy:
#   - Cycle through all 15 TLD prefixes for each year
#   - Collect ~67 candidates per TLD (15 × 67 = ~1000)
#   - Filter to languages containing "eng" (or NA for old crawls)
#   - Oversample 3x to account for non-English pages and fetch failures
#
# Usage:
#   Rscript R/run-balanced-sample.R
# =============================================================================

suppressPackageStartupMessages({
  library(tidyverse)
  library(httr2)
  library(rvest)
  library(jsonlite)
  library(here)
  library(DBI)
  library(RSQLite)
})

set.seed(2026)

source(here("R", "00-db-helpers.R"))

CC_INDEX_BASE <- "https://index.commoncrawl.org"
CC_S3_BASE    <- "https://data.commoncrawl.org"
DELAY         <- 1
TARGET        <- 1000  # per year

# TLDs likely to have English content — balanced mix
URL_PREFIXES <- c(
  "*.com", "*.org", "*.net", "*.edu", "*.gov",
  "*.co.uk", "*.ca", "*.au", "*.nz", "*.ie",
  "*.io", "*.info", "*.us", "*.co", "*.int"
)

YEARS <- c(2009, 2021, 2026)

safe_read_html <- function(content) {
  if (is.na(content) || !nzchar(content)) return(NULL)
  tryCatch(read_html(charToRaw(content)), error = function(e) NULL)
}

con <- open_db()
on.exit(dbDisconnect(con), add = TRUE)

# Get crawl list once
collinfo <- fromJSON(resp_body_string(
  request("https://index.commoncrawl.org/collinfo.json") |>
  req_timeout(60) |> req_perform()))

# -----------------------------------------------------------------------------
# Helper: fetch WARC and extract English text
# Returns list(text, fetch_status, languages)
# -----------------------------------------------------------------------------
fetch_and_extract <- function(rec) {
  raw_bytes <- tryCatch({
    Sys.sleep(DELAY)
    byte_end <- as.numeric(rec$offset) + as.numeric(rec$length) - 1
    resp_body_raw(
      request(paste0(CC_S3_BASE, "/", rec$filename)) |>
        req_headers(
          Range = paste0("bytes=", rec$offset, "-", byte_end),
          "User-Agent" = "ILA2026-Research/1.0 (academic research)") |>
        req_timeout(60) |>
        req_retry(max_tries = 2, backoff = ~ 2) |>
        req_perform())
  }, error = function(e) NULL)

  if (is.null(raw_bytes)) {
    return(list(text = NA_character_, fetch_status = "error: WARC fetch", languages = NA_character_))
  }

  html_str <- tryCatch({
    dec <- rawToChar(memDecompress(raw_bytes, type = "gzip"))
    tl <- str_split(dec, "\\r?\\n")[[1]]
    wb <- which(tl == "")[1]
    hl <- tl[(wb + 1):length(tl)]
    hb <- which(hl == "")[1]
    paste(hl[(hb + 1):length(hl)], collapse = "\n")
  }, error = function(e) NA_character_)

  doc <- safe_read_html(html_str)
  if (is.null(doc)) {
    return(list(text = NA_character_, fetch_status = "error: parse", languages = NA_character_))
  }

  text <- tryCatch({
    xml2::xml_remove(xml2::xml_find_all(doc, "//script"))
    xml2::xml_remove(xml2::xml_find_all(doc, "//style"))
    str_squish(html_text2(doc))
  }, error = function(e) tryCatch(str_squish(html_text2(doc)), error = function(e2) NA_character_))

  fetch_status <- if (!is.na(text) && nzchar(text)) "success" else "error: empty"
  lang <- if ("languages" %in% names(rec)) as.character(rec$languages) else NA_character_

  list(text = text, fetch_status = fetch_status, languages = lang)
}

# =============================================================================
# Main loop: for each year, balanced TLD sampling
# =============================================================================

for (year in YEARS) {
  crawl_id <- collinfo$id[str_detect(collinfo$id, paste0("CC-MAIN-", year))][1]
  if (is.na(crawl_id)) { message("No crawl for ", year); next }

  already <- dbGetQuery(con,
    "SELECT COUNT(*) as n FROM commoncrawl_samples
     WHERE year = ? AND fetch_status = 'success'",
    params = list(year))$n
  need <- TARGET - already

  message("\n", strrep("=", 60))
  message("CC ", year, " (", crawl_id, ")")
  message("Have: ", already, " | Need: ", need, " | Target: ", TARGET)
  message(strrep("=", 60))

  if (need <= 0) { message("Already at target. Skipping."); next }

  # Step 1: Gather candidates from ALL TLDs
  # Aim for ~200 candidates per TLD (3000 total for 1000 target)
  all_records <- list()

  for (prefix in URL_PREFIXES) {
    total_pages <- tryCatch({
      resp <- request(paste0(CC_INDEX_BASE, "/", crawl_id, "-index")) |>
        req_url_query(url = prefix, showNumPages = "true", output = "json") |>
        req_timeout(60) |>
        req_headers("User-Agent" = "ILA2026-Research/1.0 (academic research)") |>
        req_perform()
      parsed <- fromJSON(resp_body_string(resp))
      if ("pages" %in% names(parsed)) as.integer(parsed$pages) else 0L
    }, error = function(e) 0L)

    if (total_pages == 0) {
      message("  ", prefix, " -> 0 pages, skipping")
      next
    }

    # Fetch 2 random pages per TLD
    n_pages_to_fetch <- min(2, total_pages)
    random_pages <- sample(0:(total_pages - 1), n_pages_to_fetch)

    for (pg in random_pages) {
      Sys.sleep(DELAY)
      recs <- tryCatch({
        resp <- request(paste0(CC_INDEX_BASE, "/", crawl_id, "-index")) |>
          req_url_query(url = prefix, output = "json", page = pg,
                        filter = "mime:text/html", filter = "status:200",
                        .multi = "explode") |>
          req_timeout(60) |>
          req_retry(max_tries = 2, backoff = ~ 2) |>
          req_headers("User-Agent" = "ILA2026-Research/1.0 (academic research)") |>
          req_perform()
        lines <- str_split(str_trim(resp_body_string(resp)), "\n")[[1]]
        lines <- lines[nzchar(lines)]
        compact(lapply(lines, function(l) tryCatch(fromJSON(l), error = function(e) NULL)))
      }, error = function(e) list())

      all_records <- c(all_records, recs)
    }

    message("  ", prefix, " -> ", total_pages, " pages, fetched ", n_pages_to_fetch,
            " (total candidates: ", length(all_records), ")")
  }

  message("\nTotal candidates: ", length(all_records))

  # Convert and filter
  df <- bind_rows(lapply(all_records, as_tibble)) |>
    filter(!is.na(filename), !is.na(offset), !is.na(length)) |>
    distinct(url, .keep_all = TRUE)

  # Pre-filter to English where language metadata exists
  if ("languages" %in% names(df)) {
    df_eng <- df |> filter(is.na(languages) | str_detect(languages, "eng"))
    message("After English filter: ", nrow(df_eng), " of ", nrow(df), " candidates")
    if (nrow(df_eng) > 0) df <- df_eng
  }

  # Exclude URLs already in DB
  existing <- dbGetQuery(con,
    "SELECT original_url FROM commoncrawl_samples WHERE year = ?",
    params = list(year))$original_url
  df <- df |> filter(!(url %in% existing))

  # Oversample to account for fetch failures
  sampled <- slice_sample(df, n = min(need * 2, nrow(df)))
  message("Fetching up to ", nrow(sampled), " pages (stopping at ", need, " successes)\n")

  success <- 0
  fail <- 0
  start_time <- Sys.time()

  for (i in seq_len(nrow(sampled))) {
    if (success >= need) break

    rec <- sampled[i, ]
    result <- fetch_and_extract(rec)

    # Skip non-English results (for crawls with language metadata)
    if (!is.na(result$languages) && !str_detect(result$languages, "eng")) {
      fail <- fail + 1
      next
    }

    now <- format(Sys.time(), "%Y-%m-%d %H:%M:%S")
    tryCatch(
      dbExecute(con,
        "INSERT OR IGNORE INTO commoncrawl_samples
         (year, crawl_id, shard_id, original_url, text, text_length,
          languages, fetch_status, collected_at)
         VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
        params = list(
          as.integer(year), crawl_id, NA_integer_,
          as.character(rec$url), result$text,
          if (!is.na(result$text)) nchar(result$text) else NA_integer_,
          result$languages, result$fetch_status, now)),
      error = function(e) NULL)

    if (result$fetch_status == "success") success <- success + 1 else fail <- fail + 1

    if ((success + fail) %% 50 == 0 || success >= need) {
      elapsed <- as.numeric(difftime(Sys.time(), start_time, units = "mins"))
      rate <- if (elapsed > 0) (success + fail) / elapsed else 0
      eta <- if (rate > 0) (need - success) / (rate * success / max(success + fail, 1)) else NA
      message(sprintf("  %d fetched | %d success, %d fail | %.1f min elapsed",
                       success + fail, success, fail, elapsed))
    }
  }

  total_now <- dbGetQuery(con,
    "SELECT COUNT(*) as n FROM commoncrawl_samples
     WHERE year = ? AND fetch_status = 'success'",
    params = list(year))$n
  message(sprintf("\nCC %d: %d successful in DB (target: %d)", year, total_now, TARGET))
}

# =============================================================================
# Export
# =============================================================================

message("\n", strrep("=", 60))
message("EXPORT")
message(strrep("=", 60))

for (year in YEARS) {
  data <- as_tibble(dbGetQuery(con,
    paste0("SELECT * FROM commoncrawl_samples WHERE year = ", year)))
  rds <- file.path(PROCESSED_DIR, paste0("commoncrawl_", year, "_sample.rds"))
  csv <- file.path(PROCESSED_DIR, paste0("commoncrawl_", year, "_metadata.csv"))
  saveRDS(data, rds)
  write_csv(select(data, -text), csv)
  n_ok <- sum(data$fetch_status == "success")
  message("CC ", year, ": ", nrow(data), " rows, ", n_ok, " successful")
}

message("\nDone.")
