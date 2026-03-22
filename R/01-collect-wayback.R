# =============================================================================
# 01-collect-wayback.R
# =============================================================================
# Collects web page text from the Wayback Machine (Internet Archive) CDX API.
#
# For each combination of year and domain, this script:
#   1. Checks SQLite for already-collected samples (resume support).
#   2. Queries the CDX API for archived snapshots.
#   3. Randomly samples SAMPLE_SIZE URLs.
#   4. Fetches the original page content using the id_ flag (no toolbar).
#   5. Extracts plain text using rvest.
#   6. Inserts each result immediately into SQLite.
#
# Usage (from project root):
#   Rscript R/01-collect-wayback.R
#
# Output:
#   data/processed/web_archive_samples.db   SQLite database (shared with CC script)
#   data/processed/wayback_sample.rds       Full tibble with text
#   data/processed/wayback_metadata.csv     Metadata without text column
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

SAMPLE_SIZE <- 1  # Pages per year per domain

set.seed(2026)

TARGET_DOMAINS <- c(
  "nytimes.com",
  "bbc.co.uk",
  "cnn.com",
  "washingtonpost.com",
  "theguardian.com"
)

YEARS <- 1996:2026

PROCESSED_DIR <- here("data", "processed")

DB_PATH       <- here("data", "processed", "web_archive_samples.db")
CDX_BASE_URL  <- "https://web.archive.org/cdx/search/cdx"
WAYBACK_BASE  <- "https://web.archive.org/web"

DELAY_SECONDS <- 2  # Polite delay (configurable for multi-day runs)

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

  # Ensure the commoncrawl table also exists so both scripts share one DB
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

#' Count successful samples already in the database for a year/domain pair.
#'
#' @param con     DBI connection
#' @param year    Four-digit integer year
#' @param domain  Domain string
#' @return Integer count of successful rows.
get_completed_count <- function(con, year, domain) {
  dbGetQuery(con,
    "SELECT COUNT(*) AS n FROM wayback_samples
     WHERE year = ? AND domain = ? AND fetch_status = 'success'",
    params = list(year, domain)
  )$n
}

#' Export the wayback_samples table to RDS and CSV.
#'
#' @param con       DBI connection
#' @param rds_path  Path for the .rds output
#' @param csv_path  Path for the .csv output (text column excluded)
export_results <- function(con, rds_path, csv_path) {
  results <- dbGetQuery(con, "SELECT * FROM wayback_samples")
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

#' Query the CDX API for snapshots of a domain within a given year.
#'
#' @param domain  Domain to query, e.g. "nytimes.com"
#' @param year    Four-digit integer year
#' @return A tibble with columns: timestamp, original, statuscode, mimetype.
#'         Returns an empty tibble on failure or no results.
query_cdx <- function(domain, year) {
  tryCatch({
    resp <- request(CDX_BASE_URL) |>
      req_url_query(
        url    = paste0(domain, "/*"),
        output = "json",
        from   = paste0(year, "0101"),
        to     = paste0(year, "1231"),
        limit  = 1000,
        fl     = "timestamp,original,statuscode,mimetype",
        filter = "statuscode:200",
        filter = "mimetype:text/html",
        .multi = "explode"
      ) |>
      req_timeout(60) |>
      req_retry(max_tries = 3, backoff = ~ 2) |>
      req_headers("User-Agent" = "ILA2026-Research/1.0 (academic research)") |>
      req_perform()

    body_str <- resp_body_string(resp)

    if (!nzchar(body_str)) {
      message("  [CDX] Empty response for ", domain, " in ", year)
      return(tibble(timestamp = character(), original = character(),
                    statuscode = character(), mimetype = character()))
    }

    parsed <- fromJSON(body_str, simplifyMatrix = TRUE)

    # CDX returns a matrix: first row is header, remaining rows are data
    if (is.null(parsed) || length(parsed) < 2) {
      message("  [CDX] No results for ", domain, " in ", year)
      return(tibble(timestamp = character(), original = character(),
                    statuscode = character(), mimetype = character()))
    }

    headers <- parsed[1, ]
    data    <- as_tibble(parsed[-1, , drop = FALSE], .name_repair = "minimal")
    colnames(data) <- headers
    data
  }, error = function(e) {
    message("  [CDX ERROR] ", domain, " ", year, ": ", conditionMessage(e))
    tibble(timestamp = character(), original = character(),
           statuscode = character(), mimetype = character())
  })
}

#' Fetch archived page content from the Wayback Machine.
#'
#' Uses the id_ modifier so the response is the original HTML without the
#' Wayback Machine toolbar injected.
#'
#' @param timestamp    CDX timestamp string, e.g. "19961015120000"
#' @param original_url Original URL of the archived page
#' @return A list: wayback_url, text, fetch_status.
fetch_wayback_page <- function(timestamp, original_url) {
  wayback_url <- paste0(WAYBACK_BASE, "/", timestamp, "id_/", original_url)

  tryCatch({
    Sys.sleep(DELAY_SECONDS)

    resp <- request(wayback_url) |>
      req_timeout(60) |>
      req_headers("User-Agent" = "ILA2026-Research/1.0 (academic research)") |>
      req_retry(max_tries = 3, backoff = ~ 2) |>
      req_perform()

    doc <- safe_read_html(resp_body_string(resp))

    if (is.null(doc)) {
      return(list(wayback_url = wayback_url, text = NA_character_,
                  fetch_status = "error: HTML parse failed"))
    }

    xml2::xml_remove(html_nodes(doc, "script"))
    xml2::xml_remove(html_nodes(doc, "style"))
    text <- doc |> html_text2() |> str_squish()

    list(wayback_url = wayback_url, text = text, fetch_status = "success")
  }, error = function(e) {
    message("  [FETCH ERROR] ", wayback_url, ": ", conditionMessage(e))
    list(wayback_url = wayback_url, text = NA_character_,
         fetch_status = paste0("error: ", conditionMessage(e)))
  })
}

# -----------------------------------------------------------------------------
# 4. Per-combination collector (writes directly to DB)
# -----------------------------------------------------------------------------

#' Process one year/domain combination and insert results into SQLite.
#'
#' @param con     DBI connection
#' @param year    Four-digit integer year
#' @param domain  Domain string
collect_year_domain <- function(con, year, domain) {
  # Resume check
  already <- get_completed_count(con, year, domain)
  if (already >= SAMPLE_SIZE) {
    message("  Skipping ", domain, " ", year,
            " (", already, " successful samples already in DB)")
    return(invisible(NULL))
  }

  message("Fetching year ", year, ", domain ", domain, "...")

  snapshots <- query_cdx(domain, year)

  filtered <- snapshots |>
    filter(statuscode == "200", str_detect(mimetype, "text/html"))

  if (nrow(filtered) == 0) {
    message("  No valid snapshots found.")
    return(invisible(NULL))
  }

  sampled <- filtered |>
    slice_sample(n = min(SAMPLE_SIZE, nrow(filtered)))

  message("  Sampled ", nrow(sampled), " of ", nrow(filtered), " snapshots.")

  for (i in seq_len(nrow(sampled))) {
    ts  <- sampled$timestamp[i]
    url <- sampled$original[i]
    message("  Fetching: ", url, " @ ", ts)

    fetched <- fetch_wayback_page(ts, url)

    row <- data.frame(
      year         = as.integer(year),
      domain       = domain,
      timestamp    = ts,
      original_url = url,
      wayback_url  = fetched$wayback_url,
      text         = fetched$text,
      text_length  = if (!is.na(fetched$text)) nchar(fetched$text) else NA_integer_,
      fetch_status = fetched$fetch_status,
      stringsAsFactors = FALSE
    )

    now <- format(Sys.time(), "%Y-%m-%d %H:%M:%S")

    tryCatch(
      dbExecute(con,
        "INSERT OR IGNORE INTO wayback_samples
           (year, domain, timestamp, original_url, wayback_url,
            text, text_length, fetch_status, collected_at)
         VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
        params = list(row$year, row$domain, row$timestamp, row$original_url,
                      row$wayback_url, row$text, row$text_length,
                      row$fetch_status, now)
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

# Resume status summary
completed_pairs <- dbGetQuery(con,
  "SELECT year, domain FROM wayback_samples
   WHERE fetch_status = 'success'
   GROUP BY year, domain
   HAVING COUNT(*) >= ?",
  params = list(SAMPLE_SIZE)
)
total_pairs     <- length(YEARS) * length(TARGET_DOMAINS)
completed_count <- nrow(completed_pairs)

message("=== Wayback Machine data collection ===")
message("Years:   ", min(YEARS), " - ", max(YEARS))
message("Domains: ", paste(TARGET_DOMAINS, collapse = ", "))
message("Sample size per year/domain: ", SAMPLE_SIZE)
message("Resuming: ", completed_count, " year/domain pairs already complete, ",
        total_pairs - completed_count, " remaining.")
message("")

for (year in YEARS) {
  year_results <- list()
  for (domain in TARGET_DOMAINS) {
    collect_year_domain(con, year, domain)
  }

  # Per-year summary from DB
  yr_stats <- dbGetQuery(con,
    "SELECT
       SUM(CASE WHEN fetch_status = 'success' THEN 1 ELSE 0 END) AS retrieved,
       SUM(CASE WHEN fetch_status != 'success' THEN 1 ELSE 0 END) AS failures
     FROM wayback_samples WHERE year = ?",
    params = list(year)
  )
  message("  Year ", year, " summary: ",
          yr_stats$retrieved, " success, ", yr_stats$failures, " failures.")
}

# -----------------------------------------------------------------------------
# 6. Export
# -----------------------------------------------------------------------------

rds_path <- file.path(PROCESSED_DIR, "wayback_sample.rds")
csv_path <- file.path(PROCESSED_DIR, "wayback_metadata.csv")

export_results(con, rds_path, csv_path)

message("\nSaved full dataset to: ", rds_path)
message("Saved metadata to:     ", csv_path)
message("Done.")
