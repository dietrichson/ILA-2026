# =============================================================================
# 01-collect-commoncrawl.R
# =============================================================================
# Collects web page text from Common Crawl archives via the CC Index API and
# WARC files stored on S3.
#
# For each combination of year and domain, this script:
#   1. Retrieves the list of available Common Crawl crawl indexes.
#   2. Matches each year to one or more crawl index IDs.
#   3. Queries the CC Index API for pages matching the domain.
#   4. Randomly samples SAMPLE_SIZE URLs from the results.
#   5. Fetches the raw WARC record from S3 using byte-range requests.
#   6. Decompresses and parses the WARC record to extract HTML.
#   7. Extracts plain text using rvest.
#   8. Saves a combined tibble to data/processed/ (.rds and metadata .csv).
#
# Usage (from project root):
#   Rscript R/01-collect-commoncrawl.R
#
# Output:
#   data/raw/commoncrawl/               Raw per-page text files (gitignored)
#   data/processed/commoncrawl_sample.rds       Full tibble with text
#   data/processed/commoncrawl_metadata.csv     Metadata without text column
#
# Notes:
#   - Common Crawl data is available from approximately 2008 onward.
#   - Early years (2008-2012) may have very limited coverage for some domains.
#   - WARC records are gzip-compressed; memDecompress() is used to extract HTML.
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

YEARS <- 2008:2026  # Common Crawl begins ~2008

OUTPUT_DIR <- here("data", "raw", "commoncrawl")

PROCESSED_DIR <- here("data", "processed")

CC_COLLINFO_URL <- "https://index.commoncrawl.org/collinfo.json"
CC_INDEX_BASE   <- "https://index.commoncrawl.org"
CC_S3_BASE      <- "https://data.commoncrawl.org"

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

#' Fetch and cache the list of available Common Crawl indexes.
#'
#' @return A tibble with columns: id, name, timegate, cdx-api (and others).
get_crawl_index_list <- function() {
  message("Fetching Common Crawl index list from ", CC_COLLINFO_URL, "...")

  tryCatch({
    resp <- request(CC_COLLINFO_URL) |>
      req_timeout(60) |>
      req_headers("User-Agent" = "ILA2026-Research/1.0 (academic research)") |>
      req_perform()

    fromJSON(resp_body_string(resp)) |>
      as_tibble()
  }, error = function(e) {
    stop("Failed to fetch CC index list: ", conditionMessage(e))
  })
}

#' Select crawl IDs that correspond to a given year.
#'
#' CC crawl IDs follow the pattern "CC-MAIN-YYYY-WW" where YYYY is the year
#' and WW is the week number. Early crawls may use "CC-MAIN-YYYY" (no week).
#' We match on the year portion without a trailing dash to catch both formats.
#'
#' @param crawl_list  Tibble returned by get_crawl_index_list()
#' @param year        Four-digit integer year
#' @return Character vector of matching crawl IDs (may be empty)
find_crawls_for_year <- function(crawl_list, year) {
  # Match both "CC-MAIN-YYYY-WW" (standard) and "CC-MAIN-YYYY" (old format)
  pattern <- paste0("CC-MAIN-", year)
  matching <- crawl_list |>
    filter(str_detect(id, pattern))

  if (nrow(matching) == 0) return(character(0))

  # Return just the first (newest) crawl to reduce API calls
  matching$id[1]
}

#' Query the CC Index API for pages matching a domain in a specific crawl.
#'
#' @param crawl_id  CC crawl identifier, e.g. "CC-MAIN-2020-16"
#' @param domain    Domain string, e.g. "nytimes.com"
#' @return A tibble of index records, or an empty tibble on failure/no results.
query_cc_index <- function(crawl_id, domain) {
  index_url <- paste0(CC_INDEX_BASE, "/", crawl_id, "-index")

  tryCatch({
    Sys.sleep(DELAY_SECONDS)

    resp <- request(index_url) |>
      req_url_query(
        url    = paste0("*.", domain),
        output = "json",
        limit  = 1000,
        filter = "mime:text/html"
      ) |>
      req_timeout(60) |>
      req_headers("User-Agent" = "ILA2026-Research/1.0 (academic research)") |>
      req_perform()

    body <- resp_body_string(resp)

    # CC index returns newline-delimited JSON (one object per line)
    lines <- str_split(str_trim(body), "\n")[[1]]
    lines <- lines[nzchar(lines)]

    if (length(lines) == 0) {
      return(tibble())
    }

    records <- map(lines, function(line) {
      tryCatch(fromJSON(line), error = function(e) NULL)
    })
    records <- compact(records)

    bind_rows(records)
  }, error = function(e) {
    message("  [CC INDEX ERROR] ", crawl_id, " / ", domain, ": ",
            conditionMessage(e))
    tibble()
  })
}

#' Fetch a WARC record from Common Crawl S3 storage using a byte-range request.
#'
#' @param filename  S3 path from the CC index, e.g. "crawl-data/CC-MAIN-.../...warc.gz"
#' @param offset    Byte offset of the WARC record in the file (integer/numeric)
#' @param length    Byte length of the WARC record (integer/numeric)
#' @return Raw bytes of the (compressed) WARC record, or NULL on failure.
fetch_warc_record_bytes <- function(filename, offset, length) {
  s3_url     <- paste0(CC_S3_BASE, "/", filename)
  byte_end   <- as.numeric(offset) + as.numeric(length) - 1
  range_header <- paste0("bytes=", offset, "-", byte_end)

  tryCatch({
    Sys.sleep(DELAY_SECONDS)

    resp <- request(s3_url) |>
      req_timeout(60) |>
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

#' Decompress gzip-compressed WARC bytes and extract the HTTP response body.
#'
#' A WARC record has the structure:
#'   WARC/1.0 header block
#'   <blank line>
#'   HTTP/1.x header block
#'   <blank line>
#'   HTML body
#'
#' @param raw_bytes  Raw vector of gzip-compressed WARC data
#' @return Character string of HTML content, or NA on failure.
parse_warc_html <- function(raw_bytes) {
  tryCatch({
    # Decompress gzip data
    decompressed <- memDecompress(raw_bytes, type = "gzip")
    text_raw     <- rawToChar(decompressed)

    # Split into lines (handle both \r\n and \n)
    lines <- str_split(text_raw, "\\r?\\n")[[1]]

    # Find the blank line separating WARC headers from HTTP response
    warc_blank <- which(lines == "")[1]
    if (is.na(warc_blank)) return(NA_character_)

    # The HTTP response starts after the WARC header block
    http_lines <- lines[(warc_blank + 1):length(lines)]

    # Find the blank line separating HTTP headers from body
    http_blank <- which(http_lines == "")[1]
    if (is.na(http_blank)) return(NA_character_)

    # Everything after the second blank line is the HTML body
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
#' @return Cleaned plain text string, or NA if parsing fails or text is empty.
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
    message("  [HTML PARSE ERROR]: ", conditionMessage(e))
    NA_character_
  })
}

#' Process one year/domain combination across all matching crawls for that year.
#'
#' @param year        Four-digit integer year
#' @param domain      Domain string
#' @param crawl_list  Full tibble of available crawl indexes
#' @return A tibble of results for this combination
collect_year_domain <- function(year, domain, crawl_list) {
  message("Fetching year ", year, ", domain ", domain, "...")

  crawl_ids <- find_crawls_for_year(crawl_list, year)

  if (length(crawl_ids) == 0) {
    message("  No crawl indexes found for year ", year, ".")
    return(tibble(
      year         = integer(),
      domain       = character(),
      crawl_id     = character(),
      original_url = character(),
      text         = character(),
      fetch_status = character()
    ))
  }

  message("  Found ", length(crawl_ids), " crawl(s): ",
          paste(crawl_ids, collapse = ", "))

  # Collect index records from all matching crawls and pool them
  all_records <- map(crawl_ids, function(cid) {
    recs <- query_cc_index(cid, domain)
    if (nrow(recs) > 0) recs$crawl_id <- cid
    recs
  }) |> bind_rows()

  if (nrow(all_records) == 0) {
    message("  No index records found.")
    return(tibble(
      year         = integer(),
      domain       = character(),
      crawl_id     = character(),
      original_url = character(),
      text         = character(),
      fetch_status = character()
    ))
  }

  # Filter to successful HTML records that have WARC location info
  required_cols <- c("filename", "offset", "length", "url")
  has_required  <- all(required_cols %in% colnames(all_records))

  filtered <- if (has_required) {
    all_records |>
      filter(
        !is.na(filename), !is.na(offset), !is.na(length),
        if ("status" %in% colnames(all_records)) status == "200" else TRUE,
        if ("mime"   %in% colnames(all_records)) str_detect(mime, "text/html") else TRUE
      )
  } else {
    message("  [WARNING] Index records missing expected columns.")
    tibble()
  }

  if (nrow(filtered) == 0) {
    message("  No fetchable records after filtering.")
    return(tibble(
      year         = integer(),
      domain       = character(),
      crawl_id     = character(),
      original_url = character(),
      text         = character(),
      fetch_status = character()
    ))
  }

  # Random sample across all available records
  sampled <- filtered |>
    slice_sample(n = min(SAMPLE_SIZE, nrow(filtered)))

  message("  Sampled ", nrow(sampled), " of ", nrow(filtered), " records.")

  # Fetch WARC records and extract text
  rows <- map(seq_len(nrow(sampled)), function(i) {
    rec <- sampled[i, ]
    original_url <- if ("url" %in% colnames(rec)) rec$url else NA_character_
    crawl_id_val <- if ("crawl_id" %in% colnames(rec)) rec$crawl_id else crawl_ids[1]

    message("  Fetching WARC: ", original_url)

    raw_bytes <- fetch_warc_record_bytes(rec$filename, rec$offset, rec$length)

    if (is.null(raw_bytes)) {
      return(tibble(
        year         = as.integer(year),
        domain       = domain,
        crawl_id     = as.character(crawl_id_val),
        original_url = as.character(original_url),
        text         = NA_character_,
        fetch_status = "error: failed to fetch WARC bytes"
      ))
    }

    html_string <- parse_warc_html(raw_bytes)
    text        <- extract_text_from_html(html_string)

    fetch_status <- if (!is.na(text) && nzchar(text)) "success" else "error: empty text"

    tibble(
      year         = as.integer(year),
      domain       = domain,
      crawl_id     = as.character(crawl_id_val),
      original_url = as.character(original_url),
      text         = text,
      fetch_status = fetch_status
    )
  })

  bind_rows(rows)
}

# -----------------------------------------------------------------------------
# 3. Main collection loop
# -----------------------------------------------------------------------------

dir.create(OUTPUT_DIR,    showWarnings = FALSE, recursive = TRUE)
dir.create(PROCESSED_DIR, showWarnings = FALSE, recursive = TRUE)

message("=== Common Crawl data collection ===")
message("Years:   ", min(YEARS), " - ", max(YEARS))
message("Domains: ", paste(TARGET_DOMAINS, collapse = ", "))
message("Sample size per year/domain: ", SAMPLE_SIZE)
message("")

# Fetch the crawl index list once and reuse it
crawl_list <- get_crawl_index_list()
message("Available crawl indexes: ", nrow(crawl_list))
message("")

all_results <- map(YEARS, function(year) {
  map(TARGET_DOMAINS, function(domain) {
    collect_year_domain(year, domain, crawl_list)
  }) |> bind_rows()
}) |> bind_rows()

# -----------------------------------------------------------------------------
# 4. Save outputs
# -----------------------------------------------------------------------------

rds_path <- file.path(PROCESSED_DIR, "commoncrawl_sample.rds")
csv_path <- file.path(PROCESSED_DIR, "commoncrawl_metadata.csv")

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
