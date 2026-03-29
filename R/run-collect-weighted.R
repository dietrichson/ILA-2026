# =============================================================================
# run-collect-weighted.R
# =============================================================================
# Collect 1000 English-language pages per year from CC (2012-2026),
# with TLD-proportional weighting and domain diversity enforcement.
#
# Sampling strategy:
#   1. For each year, query showNumPages for all 15 TLD prefixes
#   2. Allocate pages per TLD proportionally to CC index size
#   3. Fetch random index pages per TLD according to allocation
#   4. Pool candidates, filter to English, cap at 5 per domain
#   5. Sample 1000 from the weighted pool
#   6. Fetch WARC records and extract text
#
# Separate database: data/processed/web_archive_weighted.db
#
# Resume-safe via SQLite. Re-run to continue interrupted collections.
#
# Usage:
#   Rscript R/run-collect-weighted.R
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

CC_INDEX_BASE   <- "https://index.commoncrawl.org"
CC_S3_BASE      <- "https://data.commoncrawl.org"
DELAY           <- 1
TARGET          <- 1000
MAX_PER_DOMAIN  <- 5

URL_PREFIXES <- c(
  "*.com", "*.org", "*.net", "*.edu", "*.gov",
  "*.co.uk", "*.ca", "*.au", "*.nz", "*.ie",
  "*.io", "*.info", "*.us", "*.co", "*.int"
)

YEARS <- 2012:2026

# Hardcoded crawl IDs
CRAWL_IDS <- c(
  "2012" = "CC-MAIN-2012",
  "2013" = "CC-MAIN-2013-48",
  "2014" = "CC-MAIN-2014-52",
  "2015" = "CC-MAIN-2015-48",
  "2016" = "CC-MAIN-2016-50",
  "2017" = "CC-MAIN-2017-51",
  "2018" = "CC-MAIN-2018-51",
  "2019" = "CC-MAIN-2019-51",
  "2020" = "CC-MAIN-2020-50",
  "2021" = "CC-MAIN-2021-49",
  "2022" = "CC-MAIN-2022-49",
  "2023" = "CC-MAIN-2023-50",
  "2024" = "CC-MAIN-2024-51",
  "2025" = "CC-MAIN-2025-51",
  "2026" = "CC-MAIN-2026-12"
)

# --- Separate database -------------------------------------------------------
DB_PATH       <- here("data", "processed", "web_archive_weighted.db")
PROCESSED_DIR <- here("data", "processed")

open_weighted_db <- function() {
  dir.create(dirname(DB_PATH), showWarnings = FALSE, recursive = TRUE)
  con <- dbConnect(SQLite(), DB_PATH)
  dbExecute(con, "CREATE TABLE IF NOT EXISTS samples (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    year INTEGER NOT NULL,
    crawl_id TEXT,
    original_url TEXT,
    tld_prefix TEXT,
    text TEXT,
    text_length INTEGER,
    languages TEXT,
    fetch_status TEXT NOT NULL,
    collected_at TEXT,
    UNIQUE(year, crawl_id, original_url))")
  con
}

# =============================================================================
# Helpers
# =============================================================================

safe_read_html <- function(content) {
  if (is.na(content) || !nzchar(content)) return(NULL)
  tryCatch(read_html(charToRaw(content)), error = function(e) NULL)
}

extract_domain <- function(urls) {
  host <- str_extract(urls, "https?://([^/:]+)", group = 1)
  host <- str_remove(host, ":[0-9]+$")
  host <- str_remove(host, "^www[0-9]*[.]")
  two_part_tlds <- c("co.uk", "com.au", "co.nz", "co.jp", "com.br",
                      "co.in", "org.uk", "ac.uk", "gov.uk", "net.au",
                      "org.au", "com.cn", "co.za", "com.mx",
                      "org.nz", "net.nz", "govt.nz")
  sapply(host, function(h) {
    if (is.na(h)) return(NA_character_)
    parts <- str_split(h, "[.]")[[1]]
    n <- length(parts)
    if (n <= 2) return(h)
    suffix2 <- paste(parts[(n-1):n], collapse = ".")
    if (suffix2 %in% two_part_tlds) {
      if (n >= 3) paste(parts[(n-2):n], collapse = ".") else h
    } else {
      paste(parts[(n-1):n], collapse = ".")
    }
  }, USE.NAMES = FALSE)
}

#' Get TLD page counts for a crawl and compute proportional allocation.
#'
#' @param crawl_id CC crawl identifier
#' @param target   Target number of pages
#' @return Named list: prefix -> list(pages, alloc, n_index_pages_to_fetch)
get_tld_weights <- function(crawl_id, target) {
  page_counts <- list()

  for (prefix in URL_PREFIXES) {
    total_pages <- tryCatch({
      resp <- request(paste0(CC_INDEX_BASE, "/", crawl_id, "-index")) |>
        req_url_query(url = prefix, showNumPages = "true", output = "json") |>
        req_timeout(60) |>
        req_retry(max_tries = 3, backoff = ~ 5) |>
        req_headers("User-Agent" = "ILA2026-Research/1.0 (academic research)") |>
        req_perform()
      parsed <- fromJSON(resp_body_string(resp))
      if ("pages" %in% names(parsed)) as.integer(parsed$pages) else 0L
    }, error = function(e) 0L)
    page_counts[[prefix]] <- total_pages
    Sys.sleep(DELAY)
  }

  grand_total <- sum(unlist(page_counts))
  if (grand_total == 0) return(NULL)

  # Proportional allocation: how many of the 1000 pages should come from each TLD
  # Oversample by 3x to account for failures and domain cap
  result <- list()
  for (prefix in URL_PREFIXES) {
    pc <- page_counts[[prefix]]
    if (pc == 0) next
    proportion <- pc / grand_total
    alloc <- max(1, round(target * proportion))
    # How many index pages to fetch: each gives ~10-15K records
    # Fetch proportionally but ensure minimum of 2 for large TLDs (reliability)
    n_fetch <- max(2, ceiling(alloc / 200))
    n_fetch <- min(n_fetch, pc)  # don't exceed available
    result[[prefix]] <- list(pages = pc, alloc = alloc, n_fetch = n_fetch,
                              pct = round(proportion * 100, 1))
  }
  result
}

# =============================================================================
# Main collection function
# =============================================================================

collect_year <- function(con, year, crawl_id) {
  already <- dbGetQuery(con,
    "SELECT COUNT(*) as n FROM samples
     WHERE year = ? AND fetch_status = 'success'",
    params = list(year))$n
  need <- TARGET - already

  message("\n", strrep("=", 60))
  message("CC ", year, " (", crawl_id, ")")
  message("Have: ", already, " | Need: ", need)
  message(strrep("=", 60))

  if (need <= 0) { message("Already at target. Skipping."); return(invisible(NULL)) }

  # ----- Step 1: Get TLD weights for this crawl -----
  weights <- get_tld_weights(crawl_id, TARGET)
  if (is.null(weights)) {
    message("Could not get TLD weights. Skipping.")
    return(invisible(NULL))
  }

  message("\nTLD allocation:")
  for (pfx in names(weights)) {
    w <- weights[[pfx]]
    message(sprintf("  %-12s %5.1f%% -> %4d pages (%d index pages to fetch)",
                     pfx, w$pct, w$alloc, w$n_fetch))
  }

  # ----- Step 2: Gather candidates proportionally -----
  all_records <- list()

  for (pfx in names(weights)) {
    w <- weights[[pfx]]
    random_pages <- sample(0:(w$pages - 1), w$n_fetch)

    for (pg in random_pages) {
      Sys.sleep(DELAY)
      recs <- tryCatch({
        resp <- request(paste0(CC_INDEX_BASE, "/", crawl_id, "-index")) |>
          req_url_query(url = pfx, output = "json", page = pg,
                        filter = "mime:text/html", filter = "status:200",
                        .multi = "explode") |>
          req_timeout(60) |>
          req_retry(max_tries = 2, backoff = ~ 2) |>
          req_headers("User-Agent" = "ILA2026-Research/1.0 (academic research)") |>
          req_perform()
        lines <- str_split(str_trim(resp_body_string(resp)), "\n")[[1]]
        lines <- lines[nzchar(lines)]
        parsed <- compact(lapply(lines, function(l) tryCatch(fromJSON(l), error = function(e) NULL)))
        # Tag each record with its TLD prefix
        lapply(parsed, function(r) { r$tld_prefix <- pfx; r })
      }, error = function(e) list())
      all_records <- c(all_records, recs)
    }
    message("  ", pfx, " fetched ", w$n_fetch, " pages (candidates: ", length(all_records), ")")
  }

  message("\nRaw candidates: ", length(all_records))

  if (length(all_records) == 0) {
    message("No candidates! Skipping.")
    return(invisible(NULL))
  }

  # ----- Step 3: Filter, deduplicate, enforce domain cap -----
  df <- bind_rows(lapply(all_records, as_tibble)) |>
    filter(!is.na(filename), !is.na(offset), !is.na(length)) |>
    distinct(url, .keep_all = TRUE)

  message("Unique URLs: ", nrow(df))

  # English filter
  if ("languages" %in% names(df)) {
    df_eng <- df |> filter(is.na(languages) | str_detect(languages, "eng"))
    message("After English filter: ", nrow(df_eng))
    if (nrow(df_eng) >= need) df <- df_eng
  }

  # Domain cap
  df <- df |> mutate(reg_domain = extract_domain(url))
  df <- df[sample(nrow(df)), ] |>
    group_by(reg_domain) |>
    slice_head(n = MAX_PER_DOMAIN) |>
    ungroup()

  message("After domain cap: ", nrow(df), " pages from ", n_distinct(df$reg_domain), " domains")

  # Exclude already-fetched
  existing <- dbGetQuery(con,
    "SELECT original_url FROM samples WHERE year = ?",
    params = list(year))$original_url
  df <- df |> filter(!(url %in% existing))

  # ----- Step 4: Sample from the full pool -----
  # The TLD weighting is already enforced by how many candidates each TLD
  # contributed (proportional index page fetches). Just oversample from pool.
  sampled <- slice_sample(df, n = min(need * 2, nrow(df)))

  message("Fetching up to ", nrow(sampled), " (stopping at ", need, " successes)\n")

  # ----- Step 5: Fetch WARC records -----
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

    lang <- if ("languages" %in% names(rec)) as.character(rec$languages) else NA_character_
    if (!is.na(lang) && !str_detect(lang, "eng")) {
      fail <- fail + 1
      next
    }

    tld_pfx <- if ("tld_prefix" %in% names(rec)) as.character(rec$tld_prefix) else NA_character_
    now <- format(Sys.time(), "%Y-%m-%d %H:%M:%S")
    tryCatch(
      dbExecute(con,
        "INSERT OR IGNORE INTO samples
         (year, crawl_id, original_url, tld_prefix, text, text_length,
          languages, fetch_status, collected_at)
         VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
        params = list(
          as.integer(year), crawl_id, as.character(rec$url), tld_pfx,
          text, if (!is.na(text)) nchar(text) else NA_integer_,
          lang, fetch_status, now)),
      error = function(e) NULL)

    if (fetch_status == "success") success <- success + 1 else fail <- fail + 1

    if ((success + fail) %% 100 == 0 || success >= need) {
      elapsed <- as.numeric(difftime(Sys.time(), start_time, units = "mins"))
      message(sprintf("  %d success, %d fail | %.1f min", success, fail, elapsed))
    }
  }

  total_now <- dbGetQuery(con,
    "SELECT COUNT(*) as n FROM samples
     WHERE year = ? AND fetch_status = 'success'",
    params = list(year))$n
  message(sprintf("\nCC %d: %d successful in DB (target: %d)", year, total_now, TARGET))
}

# =============================================================================
# Run
# =============================================================================

con <- open_weighted_db()
on.exit(dbDisconnect(con), add = TRUE)

message("=== Weighted Collection ===")
message("Database: ", DB_PATH)
message("Years: ", paste(YEARS, collapse = ", "))
message("Target: ", TARGET, "/year | Max per domain: ", MAX_PER_DOMAIN)
message("")

for (year in YEARS) {
  crawl_id <- CRAWL_IDS[as.character(year)]
  if (is.na(crawl_id)) { message("No crawl for ", year); next }
  collect_year(con, year, crawl_id)
}

# =============================================================================
# Export
# =============================================================================

message("\n", strrep("=", 60))
message("EXPORT")
message(strrep("=", 60))

all_years <- dbGetQuery(con,
  "SELECT DISTINCT year FROM samples ORDER BY year")$year

for (year in all_years) {
  data <- as_tibble(dbGetQuery(con,
    paste0("SELECT * FROM samples WHERE year = ", year, " AND fetch_status = 'success'")))
  rds <- file.path(PROCESSED_DIR, paste0("weighted_", year, "_sample.rds"))
  csv <- file.path(PROCESSED_DIR, paste0("weighted_", year, "_metadata.csv"))
  saveRDS(data, rds)
  write_csv(select(data, -text), csv)
  message("CC ", year, ": ", nrow(data), " rows")
}

# TLD distribution report
message("\n=== TLD Distribution ===")
for (year in all_years) {
  tlds <- dbGetQuery(con, paste0(
    "SELECT tld_prefix, COUNT(*) as n FROM samples
     WHERE year = ", year, " AND fetch_status = 'success'
     GROUP BY tld_prefix ORDER BY n DESC"))
  top <- paste(sprintf("%s=%d", tlds$tld_prefix[1:min(5,nrow(tlds))],
                        tlds$n[1:min(5,nrow(tlds))]), collapse = ", ")
  message(sprintf("  %d: %s ...", year, top))
}

# Domain diversity report
message("\n=== Domain Diversity ===")
for (year in all_years) {
  urls <- dbGetQuery(con,
    paste0("SELECT original_url FROM samples WHERE year = ", year,
           " AND fetch_status = 'success'"))$original_url
  domains <- extract_domain(urls)
  n_unique <- n_distinct(domains)
  top_domain <- names(sort(table(domains), decreasing = TRUE))[1]
  top_count <- max(table(domains))
  message(sprintf("  %d: %d pages, %d unique domains, max %d from %s",
                   year, length(urls), n_unique, top_count, top_domain))
}

message("\n=== Summary ===")
summary_tbl <- as_tibble(dbGetQuery(con,
  "SELECT year,
          SUM(CASE WHEN fetch_status = 'success' THEN 1 ELSE 0 END) AS success,
          SUM(CASE WHEN fetch_status != 'success' THEN 1 ELSE 0 END) AS fail
   FROM samples GROUP BY year ORDER BY year"))
print(summary_tbl, n = 20)

message("\nDone.")
