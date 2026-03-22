# =============================================================================
# 01-collect-wayback.R
# =============================================================================
# Collects web page text from the Wayback Machine (Internet Archive) CDX API.
#
# For each combination of year and domain, this script:
#   1. Queries the CDX API for archived snapshots.
#   2. Filters to successful HTML responses.
#   3. Randomly samples SAMPLE_SIZE URLs.
#   4. Fetches the original page content using the id_ flag (no toolbar).
#   5. Extracts plain text using rvest.
#   6. Saves a combined tibble to data/processed/ (.rds and metadata .csv).
#
# Usage (from project root):
#   Rscript R/01-collect-wayback.R
#
# Output:
#   data/raw/wayback/          Raw per-page text files (gitignored)
#   data/processed/wayback_sample.rds        Full tibble with text
#   data/processed/wayback_metadata.csv      Metadata without text column
# =============================================================================

# -----------------------------------------------------------------------------
# 0. Package checks
# -----------------------------------------------------------------------------

required_packages <- c("tidyverse", "httr2", "rvest", "jsonlite", "here")

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
})

# -----------------------------------------------------------------------------
# 1. Configuration
# -----------------------------------------------------------------------------

SAMPLE_SIZE <- 1  # Number of random pages to retrieve per year per domain

set.seed(2026)    # For reproducibility of random sampling

TARGET_DOMAINS <- c(
  "nytimes.com",
  "bbc.co.uk",
  "cnn.com",
  "washingtonpost.com",
  "theguardian.com"
)

YEARS <- 1996:2026

OUTPUT_DIR <- here("data", "raw", "wayback")

PROCESSED_DIR <- here("data", "processed")

CDX_BASE_URL  <- "https://web.archive.org/cdx/search/cdx"
WAYBACK_BASE  <- "https://web.archive.org/web"

DELAY_SECONDS <- 1  # Polite delay between HTTP requests

# -----------------------------------------------------------------------------
# 2. Helper functions
# -----------------------------------------------------------------------------

#' Safely parse HTML content string, avoiding xml2 path confusion.
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

#' Query CDX API for snapshots of a domain within a given year.
#'
#' @param domain  Domain to query, e.g. "nytimes.com"
#' @param year    Four-digit integer year
#' @return A tibble with columns: timestamp, original, statuscode, mimetype.
#'         Returns an empty tibble on failure.
query_cdx <- function(domain, year) {
  result <- tryCatch({
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

  result
}

#' Fetch archived page content from the Wayback Machine.
#'
#' Uses the id_ modifier so the response is the original HTML without the
#' Wayback Machine toolbar injected.
#'
#' @param timestamp  CDX timestamp string, e.g. "19961015120000"
#' @param original_url  Original URL of the archived page
#' @return A list with elements: text (character), fetch_status (character)
fetch_wayback_page <- function(timestamp, original_url) {
  wayback_url <- paste0(WAYBACK_BASE, "/", timestamp, "id_/", original_url)

  result <- tryCatch({
    Sys.sleep(DELAY_SECONDS)

    resp <- request(wayback_url) |>
      req_timeout(60) |>
      req_headers("User-Agent" = "ILA2026-Research/1.0 (academic research)") |>
      req_retry(max_tries = 3, backoff = ~ 2) |>
      req_perform()

    html_content <- resp_body_string(resp)

    # Parse HTML and extract readable text
    doc <- safe_read_html(html_content)

    if (is.null(doc)) {
      return(list(
        wayback_url  = wayback_url,
        text         = NA_character_,
        fetch_status = "error: HTML parse failed"
      ))
    }

    # Remove script and style nodes before extracting text
    xml2::xml_remove(html_nodes(doc, "script"))
    xml2::xml_remove(html_nodes(doc, "style"))

    text <- doc |>
      html_text2() |>
      str_squish()

    list(
      wayback_url  = wayback_url,
      text         = text,
      fetch_status = "success"
    )
  }, error = function(e) {
    message("  [FETCH ERROR] ", wayback_url, ": ", conditionMessage(e))
    list(
      wayback_url  = wayback_url,
      text         = NA_character_,
      fetch_status = paste0("error: ", conditionMessage(e))
    )
  })

  result
}

#' Process one year/domain combination.
#'
#' @param year    Four-digit integer year
#' @param domain  Domain string
#' @return A tibble of results for this combination
collect_year_domain <- function(year, domain) {
  message("Fetching year ", year, ", domain ", domain, "...")

  # Step 1: query CDX
  snapshots <- query_cdx(domain, year)

  # Step 2: filter to successful HTML pages
  filtered <- snapshots |>
    filter(statuscode == "200",
           str_detect(mimetype, "text/html"))

  if (nrow(filtered) == 0) {
    message("  No valid snapshots found.")
    return(tibble(
      year         = integer(),
      domain       = character(),
      timestamp    = character(),
      original_url = character(),
      wayback_url  = character(),
      text         = character(),
      fetch_status = character()
    ))
  }

  # Step 3: random sample
  sampled <- filtered |>
    slice_sample(n = min(SAMPLE_SIZE, nrow(filtered)))

  message("  Sampled ", nrow(sampled), " of ", nrow(filtered), " snapshots.")

  # Step 4: fetch each sampled page
  rows <- map(seq_len(nrow(sampled)), function(i) {
    ts  <- sampled$timestamp[i]
    url <- sampled$original[i]
    message("  Fetching: ", url, " @ ", ts)
    fetched <- fetch_wayback_page(ts, url)
    tibble(
      year         = as.integer(year),
      domain       = domain,
      timestamp    = ts,
      original_url = url,
      wayback_url  = fetched$wayback_url,
      text         = fetched$text,
      fetch_status = fetched$fetch_status
    )
  })

  bind_rows(rows)
}

# -----------------------------------------------------------------------------
# 3. Main collection loop
# -----------------------------------------------------------------------------

dir.create(OUTPUT_DIR,    showWarnings = FALSE, recursive = TRUE)
dir.create(PROCESSED_DIR, showWarnings = FALSE, recursive = TRUE)

message("=== Wayback Machine data collection ===")
message("Years:   ", min(YEARS), " - ", max(YEARS))
message("Domains: ", paste(TARGET_DOMAINS, collapse = ", "))
message("Sample size per year/domain: ", SAMPLE_SIZE)
message("")

all_results <- map(YEARS, function(year) {
  map(TARGET_DOMAINS, function(domain) {
    collect_year_domain(year, domain)
  }) |> bind_rows()
}) |> bind_rows()

# -----------------------------------------------------------------------------
# 4. Save outputs
# -----------------------------------------------------------------------------

rds_path  <- file.path(PROCESSED_DIR, "wayback_sample.rds")
csv_path  <- file.path(PROCESSED_DIR, "wayback_metadata.csv")

saveRDS(all_results, rds_path)
message("\nSaved full dataset to: ", rds_path)

metadata <- all_results |> select(-text)
write_csv(metadata, csv_path)
message("Saved metadata to:     ", csv_path)

# -----------------------------------------------------------------------------
# 5. Summary
# -----------------------------------------------------------------------------

message("\n=== Collection Summary ===")

summary_tbl <- all_results |>
  group_by(year) |>
  summarise(
    retrieved = sum(fetch_status == "success"),
    failures  = sum(fetch_status != "success"),
    .groups   = "drop"
  )

print(summary_tbl, n = Inf)

total_retrieved <- sum(summary_tbl$retrieved)
total_failures  <- sum(summary_tbl$failures)

message("\nTotal pages retrieved: ", total_retrieved)
message("Total failures:        ", total_failures)
message("Done.")
