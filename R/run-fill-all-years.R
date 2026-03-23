# =============================================================================
# run-fill-all-years.R
# =============================================================================
# Fill in all missing years (2010-2020, 2022-2025) with 1000 English-language
# pages each, balanced across 15 TLDs. Resume-safe.
#
# Usage:
#   Rscript R/run-fill-all-years.R
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
TARGET        <- 1000

URL_PREFIXES <- c(
  "*.com", "*.org", "*.net", "*.edu", "*.gov",
  "*.co.uk", "*.ca", "*.au", "*.nz", "*.ie",
  "*.io", "*.info", "*.us", "*.co", "*.int"
)

YEARS <- c(2010:2020, 2022:2025)

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

# Show resume status
message("=== Fill All Years ===")
message("Target: ", TARGET, " pages/year")
message("Years: ", paste(YEARS, collapse = ", "))
for (y in YEARS) {
  n <- dbGetQuery(con,
    "SELECT COUNT(*) as n FROM commoncrawl_samples
     WHERE year = ? AND fetch_status = 'success'",
    params = list(y))$n
  if (n > 0) message("  ", y, ": ", n, " already collected")
}
message("")

# =============================================================================
# Main loop
# =============================================================================

for (year in YEARS) {
  crawl_id <- collinfo$id[str_detect(collinfo$id, paste0("CC-MAIN-", year))][1]
  if (is.na(crawl_id)) { message("No crawl for ", year, "; skipping."); next }

  already <- dbGetQuery(con,
    "SELECT COUNT(*) as n FROM commoncrawl_samples
     WHERE year = ? AND fetch_status = 'success'",
    params = list(year))$n
  need <- TARGET - already

  message("\n", strrep("=", 60))
  message("CC ", year, " (", crawl_id, ")")
  message("Have: ", already, " | Need: ", need)
  message(strrep("=", 60))

  if (need <= 0) { message("Already at target. Skipping."); next }

  # Gather candidates from all TLDs
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
      message("  ", prefix, " -> 0 pages")
      next
    }

    n_fetch <- min(2, total_pages)
    random_pages <- sample(0:(total_pages - 1), n_fetch)

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
    message("  ", prefix, " -> ", total_pages, " pages (candidates: ", length(all_records), ")")
  }

  message("Total candidates: ", length(all_records))

  if (length(all_records) == 0) {
    message("No candidates! Skipping year.")
    next
  }

  # Filter and deduplicate
  df <- bind_rows(lapply(all_records, as_tibble)) |>
    filter(!is.na(filename), !is.na(offset), !is.na(length)) |>
    distinct(url, .keep_all = TRUE)

  if ("languages" %in% names(df)) {
    df_eng <- df |> filter(is.na(languages) | str_detect(languages, "eng"))
    message("English filter: ", nrow(df_eng), " of ", nrow(df))
    if (nrow(df_eng) > 0) df <- df_eng
  }

  existing <- dbGetQuery(con,
    "SELECT original_url FROM commoncrawl_samples WHERE year = ?",
    params = list(year))$original_url
  df <- df |> filter(!(url %in% existing))

  sampled <- slice_sample(df, n = min(need * 2, nrow(df)))
  message("Fetching up to ", nrow(sampled), " (stopping at ", need, " successes)\n")

  success <- 0
  fail <- 0
  start_time <- Sys.time()

  for (i in seq_len(nrow(sampled))) {
    if (success >= need) break

    rec <- sampled[i, ]

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
      text <- NA_character_
      fetch_status <- "error: WARC fetch failed"
    } else {
      html_str <- tryCatch({
        dec <- rawToChar(memDecompress(raw_bytes, type = "gzip"))
        tl <- str_split(dec, "\\r?\\n")[[1]]
        wb <- which(tl == "")[1]
        hl <- tl[(wb + 1):length(tl)]
        hb <- which(hl == "")[1]
        paste(hl[(hb + 1):length(hl)], collapse = "\n")
      }, error = function(e) NA_character_)

      doc <- safe_read_html(html_str)
      if (!is.null(doc)) {
        text <- tryCatch({
          xml2::xml_remove(xml2::xml_find_all(doc, "//script"))
          xml2::xml_remove(xml2::xml_find_all(doc, "//style"))
          str_squish(html_text2(doc))
        }, error = function(e)
          tryCatch(str_squish(html_text2(doc)), error = function(e2) NA_character_))
        fetch_status <- if (!is.na(text) && nzchar(text)) "success" else "error: empty text"
      } else {
        text <- NA_character_
        fetch_status <- "error: HTML parse failed"
      }
    }

    # Skip non-English after fetch (double-check)
    lang <- if ("languages" %in% names(rec)) as.character(rec$languages) else NA_character_
    if (!is.na(lang) && !str_detect(lang, "eng")) {
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
          as.character(rec$url), text,
          if (!is.na(text)) nchar(text) else NA_integer_,
          lang, fetch_status, now)),
      error = function(e) NULL)

    if (fetch_status == "success") success <- success + 1 else fail <- fail + 1

    if ((success + fail) %% 100 == 0 || success >= need) {
      elapsed <- as.numeric(difftime(Sys.time(), start_time, units = "mins"))
      message(sprintf("  %d success, %d fail | %.1f min", success, fail, elapsed))
    }
  }

  total_now <- dbGetQuery(con,
    "SELECT COUNT(*) as n FROM commoncrawl_samples
     WHERE year = ? AND fetch_status = 'success'",
    params = list(year))$n
  message(sprintf("\nCC %d: %d successful in DB", year, total_now))
}

# =============================================================================
# Export all years
# =============================================================================

message("\n", strrep("=", 60))
message("EXPORT")
message(strrep("=", 60))

PROCESSED_DIR <- here("data", "processed")
all_years <- dbGetQuery(con,
  "SELECT DISTINCT year FROM commoncrawl_samples ORDER BY year")$year

for (year in all_years) {
  data <- read_cc_samples(con, year)
  saveRDS(data, file.path(PROCESSED_DIR, paste0("commoncrawl_", year, "_sample.rds")))
  write_csv(select(data, -text), file.path(PROCESSED_DIR, paste0("commoncrawl_", year, "_metadata.csv")))
  message("CC ", year, ": ", nrow(data), " rows")
}

message("\n=== Final Summary ===")
print(db_summary(con), n = 20)
message("\nDone.")
