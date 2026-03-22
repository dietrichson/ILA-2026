# =============================================================================
# 01-collect-commoncrawl.R
# =============================================================================
# Collects web page text from Common Crawl archives via representative
# sampling of the CC Index API (no domain restriction).
#
# For each year, this script:
#   1. Checks SQLite for already-collected samples (resume support).
#   2. Finds the matching crawl ID from collinfo.json.
#   3. Picks random TLD prefixes and random page numbers for representative sampling.
#   5. Randomly samples SAMPLE_SIZE records.
#   6. Fetches each WARC record from S3 via byte-range request.
#   7. Parses WARC -> HTML -> plain text.
#   8. Inserts each result immediately into SQLite.
#
# Usage (from project root):
#   Rscript R/01-collect-commoncrawl.R
#
# Output:
#   data/processed/web_archive_samples.db     SQLite database (shared with WB script)
#   data/processed/commoncrawl_sample.rds     Full tibble with text
#   data/processed/commoncrawl_metadata.csv   Metadata without text column
#
# Notes:
#   - Common Crawl data is available from approximately 2008 onward.
#   - WARC records are gzip-compressed; memDecompress() is used to extract HTML.
#   - Sampling is not domain-specific; pages are drawn from the full crawl.
# =============================================================================

# -----------------------------------------------------------------------------
# 0. Package checks
# -----------------------------------------------------------------------------

required_packages <- c("tidyverse", "httr2", "rvest", "jsonlite", "here",
                       "DBI", "RSQLite")

missing_packages <- required_packages[
  !sapply(required_packages, requireNamespace, quietly = TRUE)
]

if (length(missing_packages) > 0) {
  stop(
    "The following required packages are not installed: ",
    paste(missing_packages, collapse = ", "),
    "\nInstall them with: install.packages(c(",
    paste0('"', missing_packages, '"', collapse = ", "),
    "))"
  )
}

suppressPackageStartupMessages({
  library(tidyverse)
  library(httr2)
  library(rvest)
  library(jsonlite)
  library(here)
  library(DBI)
  library(RSQLite)
})

# -----------------------------------------------------------------------------
# 1. Configuration
# -----------------------------------------------------------------------------

SAMPLE_SIZE        <- 1   # Pages per year (total, not per domain)

set.seed(2026)

YEARS              <- 2008:2026  # Common Crawl begins ~2008

PROCESSED_DIR      <- here("data", "processed")
DB_PATH            <- here("data", "processed", "web_archive_samples.db")

CC_COLLINFO_URL    <- "https://index.commoncrawl.org/collinfo.json"
CC_INDEX_BASE      <- "https://index.commoncrawl.org"
CC_S3_BASE         <- "https://data.commoncrawl.org"

DELAY_SECONDS      <- 1   # Polite delay between HTTP requests
N_RANDOM_PAGES     <- 5   # Random TLD+page combinations to try per year

# URL prefixes for TLD-based random sampling
URL_PREFIXES <- c(
  "*.com", "*.org", "*.net", "*.edu", "*.gov",
  "*.co.uk", "*.de", "*.fr", "*.jp", "*.br",
  "*.in", "*.ru", "*.au", "*.ca", "*.it"
)

# -----------------------------------------------------------------------------
# 2. Database helpers
# -----------------------------------------------------------------------------

#' Open (or create) the shared SQLite database and ensure tables exist.
#'
#' @return A DBI connection object.
init_db <- function() {
  con <- dbConnect(SQLite(), DB_PATH)

  dbExecute(con, "
    CREATE TABLE IF NOT EXISTS wayback_samples (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      year INTEGER NOT NULL,
      domain TEXT NOT NULL,
      timestamp TEXT,
      original_url TEXT,
      wayback_url TEXT,
      text TEXT,
      text_length INTEGER,
      fetch_status TEXT NOT NULL,
      collected_at TEXT,
      UNIQUE(year, domain, timestamp, original_url)
    )
  ")

  dbExecute(con, "
    CREATE TABLE IF NOT EXISTS commoncrawl_samples (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      year INTEGER NOT NULL,
      crawl_id TEXT,
      shard_id INTEGER,
      original_url TEXT,
      text TEXT,
      text_length INTEGER,
      languages TEXT,
      fetch_status TEXT NOT NULL,
      collected_at TEXT,
      UNIQUE(year, crawl_id, original_url)
    )
  ")

  con
}

#' Count successful samples already in the database for a given year.
#'
#' @param con   DBI connection
#' @param year  Four-digit integer year
#' @return Integer count of successful rows.
get_completed_count <- function(con, year) {
  dbGetQuery(con,
    "SELECT COUNT(*) AS n FROM commoncrawl_samples
     WHERE year = ? AND fetch_status = 'success'",
    params = list(year)
  )$n
}

#' Export the commoncrawl_samples table to RDS and CSV.
#'
#' @param con       DBI connection
#' @param rds_path  Path for the .rds output
#' @param csv_path  Path for the .csv output (text column excluded)
export_results <- function(con, rds_path, csv_path) {
  results <- dbGetQuery(con, "SELECT * FROM commoncrawl_samples")
  results_tbl <- as_tibble(results)
  saveRDS(results_tbl, rds_path)
  write_csv(select(results_tbl, -text), csv_path)
  message("Exported ", nrow(results_tbl), " rows to RDS and CSV.")
}

# -----------------------------------------------------------------------------
# 3. Fetch helpers
# -----------------------------------------------------------------------------

#' Safely parse an HTML content string, avoiding xml2 path confusion.
#'
#' @param content  Character string of raw HTML
#' @return An xml_document, or NULL on failure.
safe_read_html <- function(content) {
  if (is.na(content) || !nzchar(content)) return(NULL)
  tryCatch(
    read_html(charToRaw(content)),
    error = function(e) {
      message("  [HTML PARSE WARNING]: ", conditionMessage(e))
      NULL
    }
  )
}

#' Fetch and return the list of available Common Crawl crawl indexes.
#'
#' @return A tibble with columns: id, name, timegate, cdx-api (and others).
get_crawl_index_list <- function() {
  message("Fetching Common Crawl index list from ", CC_COLLINFO_URL, "...")

  tryCatch({
    resp <- request(CC_COLLINFO_URL) |>
      req_timeout(60) |>
      req_retry(max_tries = 3, backoff = ~ 2) |>
      req_headers("User-Agent" = "ILA2026-Research/1.0 (academic research)") |>
      req_perform()

    fromJSON(resp_body_string(resp)) |> as_tibble()
  }, error = function(e) {
    stop("Failed to fetch CC index list: ", conditionMessage(e))
  })
}

#' Select the crawl ID that corresponds to a given year.
#'
#' CC crawl IDs follow the pattern "CC-MAIN-YYYY-WW". Early crawls may use
#' "CC-MAIN-YYYY" (no week). We match on "CC-MAIN-YYYY" without a trailing
#' dash to catch both formats.
#'
#' @param crawl_list  Tibble returned by get_crawl_index_list()
#' @param year        Four-digit integer year
#' @return A single crawl ID string, or NA if none found.
find_crawl_for_year <- function(crawl_list, year) {
  pattern  <- paste0("CC-MAIN-", year)
  matching <- crawl_list |> filter(str_detect(id, pattern))
  if (nrow(matching) == 0) return(NA_character_)
  matching$id[1]
}

#' Fetch one page of CC index results and return parsed records.
#'
#' @param crawl_id   CC crawl identifier
#' @param url_query  URL pattern to query (e.g. "*" or "*.com")
#' @param page_num   Zero-based page number
#' @return A list of parsed JSON record objects (may be empty).
fetch_index_page <- function(crawl_id, url_query, page_num) {
  Sys.sleep(DELAY_SECONDS)

  tryCatch({
    resp <- request(paste0(CC_INDEX_BASE, "/", crawl_id, "-index")) |>
      req_url_query(
        url    = url_query,
        output = "json",
        page   = page_num,
        filter = "mime:text/html",
        filter = "status:200",
        .multi = "explode"
      ) |>
      req_timeout(60) |>
      req_retry(max_tries = 3, backoff = ~ 2) |>
      req_headers("User-Agent" = "ILA2026-Research/1.0 (academic research)") |>
      req_perform()

    body  <- resp_body_string(resp)
    lines <- str_split(str_trim(body), "\n")[[1]]
    lines <- lines[nzchar(lines)]
    compact(lapply(lines, function(l) tryCatch(fromJSON(l), error = function(e) NULL)))
  }, error = function(e) {
    message("  [CC INDEX ERROR] ", crawl_id, " page ", page_num, ": ",
            conditionMessage(e))
    list()
  })
}

#' Get available records using TLD-based random pagination.
#'
#' For each attempt, picks a random TLD prefix, gets total pages,
#' picks a random page, and fetches records from it.
#'
#' @param crawl_id        CC crawl identifier
#' @param n_attempts      Number of random pages to try across different TLDs
#' @return A list of parsed index records.
fetch_records_random <- function(crawl_id, n_attempts) {
  records <- list()

  for (i in seq_len(n_attempts)) {
    # Pick a random prefix
    prefix <- sample(URL_PREFIXES, 1)

    # Get page count for this prefix
    total_pages <- tryCatch({
      resp <- request(paste0(CC_INDEX_BASE, "/", crawl_id, "-index")) |>
        req_url_query(url = prefix, showNumPages = "true", output = "json") |>
        req_timeout(60) |>
        req_retry(max_tries = 3, backoff = ~ 2) |>
        req_headers("User-Agent" = "ILA2026-Research/1.0 (academic research)") |>
        req_perform()
      parsed <- tryCatch(fromJSON(resp_body_string(resp)), error = function(e) NULL)
      if (is.list(parsed) && "pages" %in% names(parsed)) as.integer(parsed$pages) else NA_integer_
    }, error = function(e) {
      message("  [PAGE COUNT ERROR] ", prefix, ": ", conditionMessage(e))
      NA_integer_
    })

    if (is.na(total_pages) || total_pages == 0) {
      message("  [SKIP] No pages for prefix ", prefix)
      next
    }

    # Pick random page
    random_page <- sample(0:(total_pages - 1), 1)
    message("  Querying ", prefix, " page ", random_page, " of ", total_pages)

    page_records <- fetch_index_page(crawl_id, prefix, random_page)
    records <- c(records, page_records)

    if (length(records) >= SAMPLE_SIZE * 10) break  # enough candidates
  }

  records
}

#' Fetch a WARC record from Common Crawl S3 using a byte-range request.
#'
#' @param filename  S3 path, e.g. "crawl-data/CC-MAIN-.../...warc.gz"
#' @param offset    Byte offset of the WARC record
#' @param length    Byte length of the WARC record
#' @return Raw bytes of the compressed WARC record, or NULL on failure.
fetch_warc_record_bytes <- function(filename, offset, length) {
  s3_url       <- paste0(CC_S3_BASE, "/", filename)
  byte_end     <- as.numeric(offset) + as.numeric(length) - 1
  range_header <- paste0("bytes=", offset, "-", byte_end)

  Sys.sleep(DELAY_SECONDS)

  tryCatch({
    resp <- request(s3_url) |>
      req_timeout(60) |>
      req_retry(max_tries = 3, backoff = ~ 2) |>
      req_headers(
        "Range"      = range_header,
        "User-Agent" = "ILA2026-Research/1.0 (academic research)"
      ) |>
      req_perform()

    resp_body_raw(resp)
  }, error = function(e) {
    message("  [WARC FETCH ERROR] ", s3_url, " range ", range_header, ": ",
            conditionMessage(e))
    NULL
  })
}

#' Decompress gzip WARC bytes and extract the HTTP response body (HTML).
#'
#' WARC structure:
#'   WARC/1.0 header block  <blank line>
#'   HTTP/1.x header block  <blank line>
#'   HTML body
#'
#' @param raw_bytes  Raw vector of gzip-compressed WARC data
#' @return Character string of HTML content, or NA on failure.
parse_warc_html <- function(raw_bytes) {
  tryCatch({
    decompressed <- memDecompress(raw_bytes, type = "gzip")
    text_raw     <- rawToChar(decompressed)

    lines      <- str_split(text_raw, "\\r?\\n")[[1]]
    warc_blank <- which(lines == "")[1]
    if (is.na(warc_blank)) return(NA_character_)

    http_lines <- lines[(warc_blank + 1):length(lines)]
    http_blank <- which(http_lines == "")[1]
    if (is.na(http_blank)) return(NA_character_)

    body_lines <- http_lines[(http_blank + 1):length(http_lines)]
    paste(body_lines, collapse = "\n")
  }, error = function(e) {
    message("  [WARC PARSE ERROR]: ", conditionMessage(e))
    NA_character_
  })
}

#' Extract plain text from an HTML string using rvest.
#'
#' @param html_string  Character string of raw HTML
#' @return Cleaned plain text, or NA if parsing fails or text is empty.
extract_text_from_html <- function(html_string) {
  if (is.na(html_string) || !nzchar(html_string)) return(NA_character_)
  doc <- safe_read_html(html_string)
  if (is.null(doc)) return(NA_character_)
  tryCatch({
    xml2::xml_remove(html_nodes(doc, "script"))
    xml2::xml_remove(html_nodes(doc, "style"))
    text <- doc |> html_text2() |> str_squish()
    if (!nzchar(text)) NA_character_ else text
  }, error = function(e) {
    message("  [TEXT EXTRACT ERROR]: ", conditionMessage(e))
    NA_character_
  })
}

# -----------------------------------------------------------------------------
# 4. Per-year collector (writes directly to DB)
# -----------------------------------------------------------------------------

#' Process one year: sample from CC index and insert results into SQLite.
#'
#' @param con        DBI connection
#' @param year       Four-digit integer year
#' @param crawl_id   CC crawl identifier for this year
collect_year <- function(con, year, crawl_id) {
  # Resume check
  already <- get_completed_count(con, year)
  if (already >= SAMPLE_SIZE) {
    message("  Skipping year ", year,
            " (", already, " successful samples already in DB)")
    return(invisible(NULL))
  }

  message("Processing year ", year, " (", crawl_id, ")...")

  # Sample via TLD-based random pagination
  records <- fetch_records_random(crawl_id, N_RANDOM_PAGES)

  if (length(records) == 0) {
    message("  No index records found for year ", year, ".")
    return(invisible(NULL))
  }

  # Convert to tibble and filter to HTML / status 200 with WARC location
  df <- tryCatch(
    bind_rows(lapply(records, as_tibble)),
    error = function(e) tibble()
  )

  required_cols <- c("filename", "offset", "length", "url")
  if (!all(required_cols %in% colnames(df))) {
    message("  [WARNING] Index records missing expected columns for year ", year)
    return(invisible(NULL))
  }

  filtered <- df |>
    filter(!is.na(filename), !is.na(offset), !is.na(length))

  if ("status" %in% colnames(filtered)) {
    filtered <- filtered |> filter(status == "200")
  }
  if ("mime" %in% colnames(filtered)) {
    filtered <- filtered |> filter(str_detect(mime, "text/html"))
  }

  if (nrow(filtered) == 0) {
    message("  No fetchable records after filtering for year ", year, ".")
    return(invisible(NULL))
  }

  sampled <- filtered |>
    slice_sample(n = min(SAMPLE_SIZE, nrow(filtered)))

  message("  Sampled ", nrow(sampled), " of ", nrow(filtered), " records.")

  for (i in seq_len(nrow(sampled))) {
    rec          <- sampled[i, ]
    original_url <- as.character(rec$url)
    rec_crawl_id <- if ("crawl_id" %in% colnames(rec)) as.character(rec$crawl_id) else crawl_id

    message("  Fetching WARC: ", original_url)

    raw_bytes <- fetch_warc_record_bytes(rec$filename, rec$offset, rec$length)

    if (is.null(raw_bytes)) {
      fetch_status <- "error: failed to fetch WARC bytes"
      text         <- NA_character_
    } else {
      html_string  <- parse_warc_html(raw_bytes)
      text         <- extract_text_from_html(html_string)
      fetch_status <- if (!is.na(text) && nzchar(text)) "success" else "error: empty text"
    }

    languages <- if ("languages" %in% colnames(rec)) as.character(rec$languages) else NA_character_
    shard_id  <- if ("shard_id"  %in% colnames(rec)) as.integer(rec$shard_id)   else NA_integer_

    now <- format(Sys.time(), "%Y-%m-%d %H:%M:%S")

    tryCatch(
      dbExecute(con,
        "INSERT OR IGNORE INTO commoncrawl_samples
           (year, crawl_id, shard_id, original_url,
            text, text_length, languages, fetch_status, collected_at)
         VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
        params = list(
          as.integer(year),
          rec_crawl_id,
          shard_id,
          original_url,
          text,
          if (!is.na(text)) nchar(text) else NA_integer_,
          languages,
          fetch_status,
          now
        )
      ),
      error = function(e) message("  [DB INSERT ERROR]: ", conditionMessage(e))
    )
  }
}

# -----------------------------------------------------------------------------
# 5. Main
# -----------------------------------------------------------------------------

dir.create(PROCESSED_DIR, showWarnings = FALSE, recursive = TRUE)

con <- init_db()
on.exit(dbDisconnect(con), add = TRUE)

# Fetch crawl index list once
crawl_list <- get_crawl_index_list()
message("Available crawl indexes: ", nrow(crawl_list))

# Resume status summary
completed_years <- dbGetQuery(con,
  "SELECT year FROM commoncrawl_samples
   WHERE fetch_status = 'success'
   GROUP BY year
   HAVING COUNT(*) >= ?",
  params = list(SAMPLE_SIZE)
)$year

message("\n=== Common Crawl data collection ===")
message("Years:   ", min(YEARS), " - ", max(YEARS))
message("Sample size per year (total): ", SAMPLE_SIZE)
message("Resuming: ", length(completed_years), " years already complete, ",
        length(YEARS) - length(completed_years), " remaining.")
message("")

for (year in YEARS) {
  crawl_id <- find_crawl_for_year(crawl_list, year)

  if (is.na(crawl_id)) {
    message("No crawl index found for year ", year, "; skipping.")
    next
  }

  collect_year(con, year, crawl_id)

  # Per-year summary from DB
  yr_stats <- dbGetQuery(con,
    "SELECT
       SUM(CASE WHEN fetch_status = 'success' THEN 1 ELSE 0 END) AS retrieved,
       SUM(CASE WHEN fetch_status != 'success' THEN 1 ELSE 0 END) AS failures
     FROM commoncrawl_samples WHERE year = ?",
    params = list(year)
  )
  message("  Year ", year, " summary: ",
          yr_stats$retrieved, " success, ", yr_stats$failures, " failures.")
}

# -----------------------------------------------------------------------------
# 6. Export
# -----------------------------------------------------------------------------

rds_path <- file.path(PROCESSED_DIR, "commoncrawl_sample.rds")
csv_path <- file.path(PROCESSED_DIR, "commoncrawl_metadata.csv")

export_results(con, rds_path, csv_path)

message("\nSaved full dataset to: ", rds_path)
message("Saved metadata to:     ", csv_path)
message("Done.")
