# =============================================================================
# run-sample-900.R
# =============================================================================
# Collect 900 additional pages from CC 2009 and CC 2026.
# Resume-safe: checks DB for existing samples before collecting.
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

set.seed(42)  # Different seed from previous runs for sampling diversity

DB_PATH       <- here("data", "processed", "web_archive_samples.db")
PROCESSED_DIR <- here("data", "processed")
CC_INDEX_BASE <- "https://index.commoncrawl.org"
CC_S3_BASE    <- "https://data.commoncrawl.org"
DELAY         <- 1
TARGET_TOTAL  <- 1000  # 100 already collected + 900 new = 1000 per year

URL_PREFIXES <- c("*.com", "*.org", "*.net", "*.edu", "*.gov",
                   "*.co.uk", "*.de", "*.fr", "*.jp", "*.br",
                   "*.in", "*.ru", "*.au", "*.ca", "*.it")

dir.create(PROCESSED_DIR, showWarnings = FALSE, recursive = TRUE)

safe_read_html <- function(content) {
  if (is.na(content) || !nzchar(content)) return(NULL)
  tryCatch(read_html(charToRaw(content)), error = function(e) NULL)
}

con <- dbConnect(SQLite(), DB_PATH)
on.exit(dbDisconnect(con), add = TRUE)

# Ensure table exists
dbExecute(con, "CREATE TABLE IF NOT EXISTS commoncrawl_samples (
  id INTEGER PRIMARY KEY AUTOINCREMENT, year INTEGER NOT NULL,
  crawl_id TEXT, shard_id INTEGER, original_url TEXT, text TEXT,
  text_length INTEGER, languages TEXT, fetch_status TEXT NOT NULL,
  collected_at TEXT, UNIQUE(year, crawl_id, original_url))")

# --- Helper: collect N pages for a given year/crawl --------------------------

collect_cc_pages <- function(con, year, crawl_id, target_total) {
  already <- dbGetQuery(con,
    "SELECT COUNT(*) as n FROM commoncrawl_samples
     WHERE year = ? AND fetch_status = 'success'",
    params = list(year))$n
  need <- target_total - already

  message("\n", strrep("=", 60))
  message("CC ", year, " (", crawl_id, ")")
  message("Have: ", already, " | Need: ", need, " | Target: ", target_total)
  message(strrep("=", 60))

  if (need <= 0) {
    message("Already at target. Skipping.")
    return(invisible(NULL))
  }

  # Gather candidates — oversample by 3x to account for failures
  all_records <- list()
  attempts <- 0
  max_attempts <- 50
  candidates_needed <- need * 3

  while (length(all_records) < candidates_needed && attempts < max_attempts) {
    attempts <- attempts + 1
    prefix <- sample(URL_PREFIXES, 1)

    total_pages <- tryCatch({
      resp <- request(paste0(CC_INDEX_BASE, "/", crawl_id, "-index")) |>
        req_url_query(url = prefix, showNumPages = "true", output = "json") |>
        req_timeout(60) |>
        req_headers("User-Agent" = "ILA2026-Research/1.0 (academic research)") |>
        req_perform()
      parsed <- fromJSON(resp_body_string(resp))
      if ("pages" %in% names(parsed)) as.integer(parsed$pages) else 0L
    }, error = function(e) 0L)

    if (total_pages == 0) next

    pg <- sample(0:(total_pages - 1), 1)
    message("  [", attempts, "] ", prefix, " page ", pg, "/", total_pages,
            " (", length(all_records), " candidates)")

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
    }, error = function(e) {
      message("    error: ", conditionMessage(e))
      list()
    })

    all_records <- c(all_records, recs)
  }

  message("\nTotal candidate records: ", length(all_records))

  if (length(all_records) == 0) {
    message("No candidates found!")
    return(invisible(NULL))
  }

  # Deduplicate and sample
  df <- bind_rows(lapply(all_records, as_tibble)) |>
    filter(!is.na(filename), !is.na(offset), !is.na(length)) |>
    distinct(url, .keep_all = TRUE)

  # Also exclude URLs already in DB
  existing_urls <- dbGetQuery(con,
    "SELECT original_url FROM commoncrawl_samples WHERE year = ?",
    params = list(year))$original_url
  df <- df |> filter(!(url %in% existing_urls))

  sampled <- slice_sample(df, n = min(need, nrow(df)))
  message("Fetching ", nrow(sampled), " pages\n")

  success <- 0
  fail <- 0
  start_time <- Sys.time()

  for (i in seq_len(nrow(sampled))) {
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
          req_perform()
      )
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
        }, error = function(e) {
          # Fallback: extract text without removing script/style
          tryCatch(str_squish(html_text2(doc)), error = function(e2) NA_character_)
        })
        fetch_status <- if (!is.na(text) && nzchar(text)) "success" else "error: empty text"
      } else {
        text <- NA_character_
        fetch_status <- "error: HTML parse failed"
      }
    }

    lang <- if ("languages" %in% names(rec)) as.character(rec$languages) else NA_character_
    now <- format(Sys.time(), "%Y-%m-%d %H:%M:%S")

    tryCatch(
      dbExecute(con,
        "INSERT OR IGNORE INTO commoncrawl_samples
         (year, crawl_id, shard_id, original_url, text, text_length,
          languages, fetch_status, collected_at)
         VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
        params = list(as.integer(year), crawl_id, NA_integer_,
                      as.character(rec$url), text,
                      if (!is.na(text)) nchar(text) else NA_integer_,
                      lang, fetch_status, now)),
      error = function(e) message("  [DB ERROR]: ", conditionMessage(e))
    )

    if (fetch_status == "success") success <- success + 1 else fail <- fail + 1

    if (i %% 50 == 0 || i == nrow(sampled)) {
      elapsed <- as.numeric(difftime(Sys.time(), start_time, units = "mins"))
      rate <- i / elapsed
      eta <- (nrow(sampled) - i) / rate
      message(sprintf("  %d/%d | %d success, %d fail | %.1f min elapsed, ~%.1f min remaining",
                       i, nrow(sampled), success, fail, elapsed, eta))
    }
  }

  # Summary
  total_now <- dbGetQuery(con,
    "SELECT COUNT(*) as n FROM commoncrawl_samples
     WHERE year = ? AND fetch_status = 'success'",
    params = list(year))$n
  message(sprintf("\nCC %d complete: %d/%d successful total in DB", year, total_now, target_total))
}

# =============================================================================
# Run for both years
# =============================================================================

# Get crawl IDs
collinfo <- fromJSON(resp_body_string(
  request("https://index.commoncrawl.org/collinfo.json") |>
  req_timeout(60) |> req_perform()))

crawl_2009 <- collinfo$id[str_detect(collinfo$id, "CC-MAIN-2009")][1]
crawl_2026 <- collinfo$id[str_detect(collinfo$id, "CC-MAIN-2026")][1]

message("Crawls: ", crawl_2009, " / ", crawl_2026)

collect_cc_pages(con, 2009L, crawl_2009, TARGET_TOTAL)
collect_cc_pages(con, 2026L, crawl_2026, TARGET_TOTAL)

# =============================================================================
# Export
# =============================================================================

message("\n", strrep("=", 60))
message("EXPORT")
message(strrep("=", 60))

for (year in c(2009, 2026)) {
  data <- as_tibble(dbGetQuery(con,
    paste0("SELECT * FROM commoncrawl_samples WHERE year = ", year)))
  rds <- file.path(PROCESSED_DIR, paste0("commoncrawl_", year, "_sample.rds"))
  csv <- file.path(PROCESSED_DIR, paste0("commoncrawl_", year, "_metadata.csv"))
  saveRDS(data, rds)
  write_csv(select(data, -text), csv)
  n_success <- sum(data$fetch_status == "success")
  message("CC ", year, ": ", nrow(data), " total rows, ", n_success, " successful")
}

message("\nDone.")
