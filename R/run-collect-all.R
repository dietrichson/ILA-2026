# =============================================================================
# run-collect-all.R
# =============================================================================
# Collect 1000 English-language pages per year from CC (2012-2026),
# with balanced TLD sampling and domain diversity enforcement.
#
# Sampling strategy:
#   1. Query all 15 TLD prefixes, 3 random pages per TLD = 45 index pages
#   2. Pool all candidates (~500K records)
#   3. Filter to English (languages containing "eng" or NA)
#   4. Deduplicate by registered domain (keep max 5 pages per domain)
#   5. Sample 1000 from the deduplicated pool
#   6. Fetch WARC records and extract text
#
# Resume-safe via SQLite. Re-run to continue interrupted collections.
#
# Usage:
#   Rscript R/run-collect-all.R
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

CC_INDEX_BASE   <- "https://index.commoncrawl.org"
CC_S3_BASE      <- "https://data.commoncrawl.org"
DELAY           <- 1
TARGET          <- 1000
MAX_PER_DOMAIN  <- 5   # Cap pages per registered domain for diversity

# English-likely TLDs for balanced sampling
URL_PREFIXES <- c(
  "*.com", "*.org", "*.net", "*.edu", "*.gov",
  "*.co.uk", "*.ca", "*.au", "*.nz", "*.ie",
  "*.io", "*.info", "*.us", "*.co", "*.int"
)

PAGES_PER_TLD <- 3  # Random index pages to fetch per TLD

YEARS <- 2012:2026

# =============================================================================
# Helpers
# =============================================================================

safe_read_html <- function(content) {
  if (is.na(content) || !nzchar(content)) return(NULL)
  tryCatch(read_html(charToRaw(content)), error = function(e) NULL)
}

#' Extract registered domain from a URL (simplified).
#' E.g. "https://news.bbc.co.uk/sport/..." -> "bbc.co.uk"
extract_domain <- function(urls) {
  # Get host
  host <- str_extract(urls, "https?://([^/]+)", group = 1)
  # Remove port
  host <- str_remove(host, ":[0-9]+$")
  # Remove www prefix
  host <- str_remove(host, "^www[0-9]*[.]")
  # For two-part TLDs (co.uk, com.au, etc.), keep 3 parts; otherwise 2
  two_part_tlds <- c("co.uk", "com.au", "co.nz", "co.jp", "com.br",
                      "co.in", "org.uk", "ac.uk", "gov.uk", "net.au",
                      "org.au", "com.cn", "co.za", "com.mx")
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

# =============================================================================
# Main collection function
# =============================================================================

collect_year <- function(con, year, crawl_id) {
  already <- dbGetQuery(con,
    "SELECT COUNT(*) as n FROM commoncrawl_samples
     WHERE year = ? AND fetch_status = 'success'",
    params = list(year))$n
  need <- TARGET - already

  message("\n", strrep("=", 60))
  message("CC ", year, " (", crawl_id, ")")
  message("Have: ", already, " | Need: ", need)
  message(strrep("=", 60))

  if (need <= 0) { message("Already at target. Skipping."); return(invisible(NULL)) }

  # ----- Step 1: Gather candidates from all TLDs -----
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

    n_fetch <- min(PAGES_PER_TLD, total_pages)
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
    message("  ", prefix, " -> ", total_pages, " index pages, fetched ",
            n_fetch, " (candidates: ", length(all_records), ")")
  }

  message("\nRaw candidates: ", length(all_records))

  if (length(all_records) == 0) {
    message("No candidates! Skipping.")
    return(invisible(NULL))
  }

  # ----- Step 2: Filter and enforce domain diversity -----
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

  # Extract domain and cap per domain
  df <- df |> mutate(reg_domain = extract_domain(url))
  n_domains_before <- n_distinct(df$reg_domain)

  # Shuffle within groups then cap per domain
  df <- df[sample(nrow(df)), ] |>
    group_by(reg_domain) |>
    slice_head(n = MAX_PER_DOMAIN) |>
    ungroup()

  message("After domain cap (max ", MAX_PER_DOMAIN, "/domain): ",
          nrow(df), " pages from ", n_distinct(df$reg_domain), " domains",
          " (was ", n_domains_before, " domains)")

  # Exclude already-fetched URLs
  existing <- dbGetQuery(con,
    "SELECT original_url FROM commoncrawl_samples WHERE year = ?",
    params = list(year))$original_url
  df <- df |> filter(!(url %in% existing))

  # Oversample to account for fetch failures
  sampled <- slice_sample(df, n = min(need * 2, nrow(df)))
  message("Fetching up to ", nrow(sampled), " (stopping at ", need, " successes)\n")

  # ----- Step 3: Fetch WARC records -----
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

    # Post-fetch English check
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
      n_in_db <- dbGetQuery(con,
        "SELECT COUNT(DISTINCT original_url) as n FROM commoncrawl_samples
         WHERE year = ? AND fetch_status = 'success'",
        params = list(year))$n
      n_domains <- dbGetQuery(con,
        "SELECT COUNT(DISTINCT original_url) as n FROM commoncrawl_samples
         WHERE year = ? AND fetch_status = 'success'",
        params = list(year))$n
      message(sprintf("  %d success, %d fail | %.1f min | %d in DB",
                       success, fail, elapsed, n_in_db))
    }
  }

  total_now <- dbGetQuery(con,
    "SELECT COUNT(*) as n FROM commoncrawl_samples
     WHERE year = ? AND fetch_status = 'success'",
    params = list(year))$n
  message(sprintf("\nCC %d: %d successful in DB (target: %d)", year, total_now, TARGET))
}

# =============================================================================
# Run
# =============================================================================

con <- open_db()
on.exit(dbDisconnect(con), add = TRUE)

# Hardcoded crawl IDs — avoids dependency on collinfo.json at startup.
# One crawl per year (newest available). Update if new crawls are added.
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

message("=== Collect All Years ===")
message("Years: ", paste(YEARS, collapse = ", "))
message("Target: ", TARGET, "/year | Max per domain: ", MAX_PER_DOMAIN)
message("TLD prefixes: ", length(URL_PREFIXES), " | Pages per TLD: ", PAGES_PER_TLD)
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

PROCESSED_DIR <- here("data", "processed")
all_years <- dbGetQuery(con,
  "SELECT DISTINCT year FROM commoncrawl_samples ORDER BY year")$year

for (year in all_years) {
  data <- read_cc_samples(con, year)
  saveRDS(data, file.path(PROCESSED_DIR, paste0("commoncrawl_", year, "_sample.rds")))
  write_csv(select(data, -text), file.path(PROCESSED_DIR, paste0("commoncrawl_", year, "_metadata.csv")))
  message("CC ", year, ": ", nrow(data), " rows")
}

# Domain diversity report
message("\n=== Domain Diversity ===")
for (year in all_years) {
  urls <- dbGetQuery(con,
    "SELECT original_url FROM commoncrawl_samples
     WHERE year = ? AND fetch_status = 'success'",
    params = list(year))$original_url
  domains <- extract_domain(urls)
  n_unique <- n_distinct(domains)
  top_domain <- names(sort(table(domains), decreasing = TRUE))[1]
  top_count <- max(table(domains))
  message(sprintf("  %d: %d pages, %d unique domains, max %d from %s",
                   year, length(urls), n_unique, top_count, top_domain))
}

message("\n=== Summary ===")
print(db_summary(con), n = 20)
message("\nDone.")
