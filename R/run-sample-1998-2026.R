# =============================================================================
# run-sample-1998-2026.R
# =============================================================================
# One-off runner: collect 100 pages from 1998 (Wayback) and 100 pages from
# 2026 (Common Crawl). Uses the same DB and functions as the main scripts.
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

# --- Shared config -----------------------------------------------------------
DB_PATH       <- here("data", "processed", "web_archive_samples.db")
PROCESSED_DIR <- here("data", "processed")
dir.create(PROCESSED_DIR, showWarnings = FALSE, recursive = TRUE)

# --- Shared helpers -----------------------------------------------------------

safe_read_html <- function(content) {
  if (is.na(content) || !nzchar(content)) return(NULL)
  tryCatch(read_html(charToRaw(content)), error = function(e) {
    message("  [HTML PARSE WARNING]: ", conditionMessage(e))
    NULL
  })
}

init_db <- function() {
  con <- dbConnect(SQLite(), DB_PATH)
  dbExecute(con, "CREATE TABLE IF NOT EXISTS wayback_samples (
    id INTEGER PRIMARY KEY AUTOINCREMENT, year INTEGER NOT NULL,
    domain TEXT NOT NULL, timestamp TEXT, original_url TEXT,
    wayback_url TEXT, text TEXT, text_length INTEGER,
    fetch_status TEXT NOT NULL, collected_at TEXT,
    UNIQUE(year, domain, timestamp, original_url))")
  dbExecute(con, "CREATE TABLE IF NOT EXISTS commoncrawl_samples (
    id INTEGER PRIMARY KEY AUTOINCREMENT, year INTEGER NOT NULL,
    crawl_id TEXT, shard_id INTEGER, original_url TEXT, text TEXT,
    text_length INTEGER, languages TEXT, fetch_status TEXT NOT NULL,
    collected_at TEXT, UNIQUE(year, crawl_id, original_url))")
  con
}

con <- init_db()
on.exit(dbDisconnect(con), add = TRUE)

# =============================================================================
# PART 1: Wayback Machine — 1998, 100 pages
# =============================================================================

message("\n", strrep("=", 70))
message("PART 1: Wayback Machine — 1998 (100 pages across 5 domains)")
message(strrep("=", 70))

WB_SAMPLE_SIZE <- 20  # 20 per domain × 5 domains = 100 total
WB_DELAY       <- 2
CDX_BASE_URL   <- "https://web.archive.org/cdx/search/cdx"
WAYBACK_BASE   <- "https://web.archive.org/web"
TARGET_DOMAINS <- c("nytimes.com", "bbc.co.uk", "cnn.com",
                     "washingtonpost.com", "theguardian.com")

wb_completed <- function(year, domain) {
  dbGetQuery(con,
    "SELECT COUNT(*) AS n FROM wayback_samples
     WHERE year = ? AND domain = ? AND fetch_status = 'success'",
    params = list(year, domain))$n
}

for (domain in TARGET_DOMAINS) {
  already <- wb_completed(1998L, domain)
  if (already >= WB_SAMPLE_SIZE) {
    message("Skipping ", domain, " 1998 (", already, " already collected)")
    next
  }
  need <- WB_SAMPLE_SIZE - already
  message("\n--- ", domain, " 1998: need ", need, " more pages ---")

  # Query CDX
  snapshots <- tryCatch({
    resp <- request(CDX_BASE_URL) |>
      req_url_query(url = paste0(domain, "/*"), output = "json",
                    from = "19980101", to = "19981231",
                    limit = 5000,
                    fl = "timestamp,original,statuscode,mimetype",
                    filter = "statuscode:200", filter = "mimetype:text/html",
                    .multi = "explode") |>
      req_timeout(120) |>
      req_retry(max_tries = 3, backoff = ~ 5) |>
      req_headers("User-Agent" = "ILA2026-Research/1.0 (academic research)") |>
      req_perform()
    body_str <- resp_body_string(resp)
    if (!nzchar(body_str)) { message("  Empty CDX response"); next }
    parsed <- fromJSON(body_str, simplifyMatrix = TRUE)
    if (is.null(parsed) || length(parsed) < 2) { message("  No results"); next }
    headers <- parsed[1, ]
    data <- as_tibble(parsed[-1, , drop = FALSE], .name_repair = "minimal")
    colnames(data) <- headers
    data
  }, error = function(e) {
    message("  [CDX ERROR] ", domain, ": ", conditionMessage(e))
    NULL
  })

  if (is.null(snapshots) || nrow(snapshots) == 0) next

  # Deduplicate by URL (keep one snapshot per unique page)
  snapshots <- snapshots |> distinct(original, .keep_all = TRUE)

  sampled <- snapshots |> slice_sample(n = min(need, nrow(snapshots)))
  message("  CDX returned ", nrow(snapshots), " unique pages; sampling ", nrow(sampled))

  success_count <- 0
  for (i in seq_len(nrow(sampled))) {
    ts  <- sampled$timestamp[i]
    url <- sampled$original[i]
    wayback_url <- paste0(WAYBACK_BASE, "/", ts, "id_/", url)

    fetched <- tryCatch({
      Sys.sleep(WB_DELAY)
      resp <- request(wayback_url) |>
        req_timeout(60) |>
        req_retry(max_tries = 2, backoff = ~ 3) |>
        req_headers("User-Agent" = "ILA2026-Research/1.0 (academic research)") |>
        req_perform()
      doc <- safe_read_html(resp_body_string(resp))
      if (is.null(doc)) {
        list(text = NA_character_, status = "error: parse failed")
      } else {
        xml2::xml_remove(html_nodes(doc, "script"))
        xml2::xml_remove(html_nodes(doc, "style"))
        txt <- str_squish(html_text2(doc))
        list(text = txt, status = "success")
      }
    }, error = function(e) {
      list(text = NA_character_, status = paste0("error: ", conditionMessage(e)))
    })

    now <- format(Sys.time(), "%Y-%m-%d %H:%M:%S")
    tryCatch(
      dbExecute(con,
        "INSERT OR IGNORE INTO wayback_samples
         (year, domain, timestamp, original_url, wayback_url,
          text, text_length, fetch_status, collected_at)
         VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
        params = list(1998L, domain, ts, url, wayback_url,
                      fetched$text,
                      if (!is.na(fetched$text)) nchar(fetched$text) else NA_integer_,
                      fetched$status, now)),
      error = function(e) message("  [DB ERROR]: ", conditionMessage(e))
    )

    if (fetched$status == "success") success_count <- success_count + 1
    if (i %% 10 == 0 || i == nrow(sampled)) {
      message("  [", domain, "] ", i, "/", nrow(sampled),
              " fetched (", success_count, " success)")
    }
  }
}

# Wayback summary
wb_stats <- dbGetQuery(con,
  "SELECT domain,
          SUM(CASE WHEN fetch_status = 'success' THEN 1 ELSE 0 END) AS success,
          SUM(CASE WHEN fetch_status != 'success' THEN 1 ELSE 0 END) AS fail
   FROM wayback_samples WHERE year = 1998 GROUP BY domain")
message("\n--- Wayback 1998 Summary ---")
print(wb_stats)

# =============================================================================
# PART 2: Common Crawl — 2026, 100 pages
# =============================================================================

message("\n", strrep("=", 70))
message("PART 2: Common Crawl — 2026 (100 representative pages)")
message(strrep("=", 70))

CC_SAMPLE_SIZE <- 100
CC_DELAY       <- 1
CC_INDEX_BASE  <- "https://index.commoncrawl.org"
CC_S3_BASE     <- "https://data.commoncrawl.org"
URL_PREFIXES   <- c("*.com", "*.org", "*.net", "*.edu", "*.gov",
                     "*.co.uk", "*.de", "*.fr", "*.jp", "*.br",
                     "*.in", "*.ru", "*.au", "*.ca", "*.it")

cc_completed <- dbGetQuery(con,
  "SELECT COUNT(*) AS n FROM commoncrawl_samples
   WHERE year = 2026 AND fetch_status = 'success'")$n

if (cc_completed >= CC_SAMPLE_SIZE) {
  message("Already have ", cc_completed, " CC 2026 pages; skipping.")
} else {
  need <- CC_SAMPLE_SIZE - cc_completed
  message("Need ", need, " more pages (have ", cc_completed, " already)")

  # Find 2026 crawl
  collinfo <- fromJSON(resp_body_string(
    request("https://index.commoncrawl.org/collinfo.json") |>
    req_timeout(60) |> req_perform()))
  crawl_id <- collinfo$id[str_detect(collinfo$id, "CC-MAIN-2026")][1]

  if (is.na(crawl_id)) {
    message("No CC crawl found for 2026!")
  } else {
    message("Using crawl: ", crawl_id)

    # Collect records from random TLD pages until we have enough
    all_records <- list()
    attempts <- 0
    max_attempts <- 30  # safety limit

    while (length(all_records) < need * 3 && attempts < max_attempts) {
      attempts <- attempts + 1
      prefix <- sample(URL_PREFIXES, 1)

      total_pages <- tryCatch({
        resp <- request(paste0(CC_INDEX_BASE, "/", crawl_id, "-index")) |>
          req_url_query(url = prefix, showNumPages = "true", output = "json") |>
          req_timeout(60) |>
          req_headers("User-Agent" = "ILA2026-Research/1.0") |>
          req_perform()
        parsed <- fromJSON(resp_body_string(resp))
        if ("pages" %in% names(parsed)) as.integer(parsed$pages) else 0L
      }, error = function(e) 0L)

      if (total_pages == 0) next

      pg <- sample(0:(total_pages - 1), 1)
      message("  [", attempts, "] ", prefix, " page ", pg, "/", total_pages,
              " (have ", length(all_records), " candidates)")

      Sys.sleep(CC_DELAY)
      page_recs <- tryCatch({
        resp <- request(paste0(CC_INDEX_BASE, "/", crawl_id, "-index")) |>
          req_url_query(url = prefix, output = "json", page = pg,
                        filter = "mime:text/html", filter = "status:200",
                        .multi = "explode") |>
          req_timeout(60) |>
          req_retry(max_tries = 2, backoff = ~ 2) |>
          req_headers("User-Agent" = "ILA2026-Research/1.0") |>
          req_perform()
        lines <- str_split(str_trim(resp_body_string(resp)), "\n")[[1]]
        lines <- lines[nzchar(lines)]
        compact(lapply(lines, function(l) tryCatch(fromJSON(l), error = function(e) NULL)))
      }, error = function(e) {
        message("    error: ", conditionMessage(e))
        list()
      })

      all_records <- c(all_records, page_recs)
    }

    message("  Total candidate records: ", length(all_records))

    if (length(all_records) > 0) {
      df <- bind_rows(lapply(all_records, as_tibble))
      df <- df |>
        filter(!is.na(filename), !is.na(offset), !is.na(length)) |>
        distinct(url, .keep_all = TRUE)

      sampled <- df |> slice_sample(n = min(need, nrow(df)))
      message("  Sampling ", nrow(sampled), " pages for WARC fetch")

      success_count <- 0
      for (i in seq_len(nrow(sampled))) {
        rec <- sampled[i, ]

        raw_bytes <- tryCatch({
          Sys.sleep(CC_DELAY)
          byte_end <- as.numeric(rec$offset) + as.numeric(rec$length) - 1
          resp <- request(paste0(CC_S3_BASE, "/", rec$filename)) |>
            req_headers(
              Range = paste0("bytes=", rec$offset, "-", byte_end),
              "User-Agent" = "ILA2026-Research/1.0") |>
            req_timeout(60) |>
            req_retry(max_tries = 2, backoff = ~ 2) |>
            req_perform()
          resp_body_raw(resp)
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
            xml2::xml_remove(html_nodes(doc, "script"))
            xml2::xml_remove(html_nodes(doc, "style"))
            text <- str_squish(html_text2(doc))
            fetch_status <- if (!is.na(text) && nzchar(text)) "success" else "error: empty"
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
            params = list(2026L, crawl_id, NA_integer_, as.character(rec$url),
                          text,
                          if (!is.na(text)) nchar(text) else NA_integer_,
                          lang, fetch_status, now)),
          error = function(e) message("  [DB ERROR]: ", conditionMessage(e))
        )

        if (fetch_status == "success") success_count <- success_count + 1
        if (i %% 10 == 0 || i == nrow(sampled)) {
          message("  CC 2026: ", i, "/", nrow(sampled),
                  " fetched (", success_count, " success)")
        }
      }
    }
  }
}

# CC summary
cc_stats <- dbGetQuery(con,
  "SELECT year,
          SUM(CASE WHEN fetch_status = 'success' THEN 1 ELSE 0 END) AS success,
          SUM(CASE WHEN fetch_status != 'success' THEN 1 ELSE 0 END) AS fail
   FROM commoncrawl_samples WHERE year = 2026")
message("\n--- CC 2026 Summary ---")
print(cc_stats)

# =============================================================================
# Export
# =============================================================================

message("\n", strrep("=", 70))
message("EXPORT")
message(strrep("=", 70))

wb_data <- as_tibble(dbGetQuery(con, "SELECT * FROM wayback_samples WHERE year = 1998"))
cc_data <- as_tibble(dbGetQuery(con, "SELECT * FROM commoncrawl_samples WHERE year = 2026"))

saveRDS(wb_data, file.path(PROCESSED_DIR, "wayback_sample.rds"))
write_csv(select(wb_data, -text), file.path(PROCESSED_DIR, "wayback_metadata.csv"))
message("Wayback 1998: ", nrow(wb_data), " rows exported")

saveRDS(cc_data, file.path(PROCESSED_DIR, "commoncrawl_sample.rds"))
write_csv(select(cc_data, -text), file.path(PROCESSED_DIR, "commoncrawl_metadata.csv"))
message("CC 2026: ", nrow(cc_data), " rows exported")

message("\nDone.")
