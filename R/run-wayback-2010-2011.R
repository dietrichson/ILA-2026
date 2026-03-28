# =============================================================================
# run-wayback-2010-2011.R
# =============================================================================
# Collect 1000 English pages each for 2010 and 2011 from the Wayback Machine.
# Uses a broad set of domains since CC has no crawls for these years.
# Resume-safe via SQLite.
#
# Usage:
#   Rscript R/run-wayback-2010-2011.R
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

CDX_BASE_URL <- "https://web.archive.org/cdx/search/cdx"
WAYBACK_BASE <- "https://web.archive.org/web"
DELAY        <- 3  # Longer delay — Wayback is sensitive
TARGET       <- 1000

# Diverse English-language domains across sectors
# Broad enough for representativeness, all known to have 2010-2011 archives
DOMAINS <- c(
  # News/media
  "slate.com", "salon.com", "pbs.org", "theatlantic.com", "wired.com",
  "arstechnica.com", "techcrunch.com", "engadget.com", "gizmodo.com",
  # Reference/education
  "wikipedia.org", "about.com", "ehow.com", "howstuffworks.com",
  "harvard.edu", "stanford.edu", "berkeley.edu", "columbia.edu", "mit.edu",
  "yale.edu", "princeton.edu", "cornell.edu", "ucla.edu",
  # Government
  "nasa.gov", "nih.gov", "cdc.gov", "usda.gov", "census.gov",
  "loc.gov", "state.gov", "epa.gov",
  # Tech/community
  "python.org", "mozilla.org", "apache.org", "drupal.org", "wordpress.org",
  "craigslist.org", "stackoverflow.com", "sourceforge.net",
  # General
  "weather.com", "imdb.com", "yelp.com", "tripadvisor.com",
  "webmd.com", "mayoclinic.org", "allrecipes.com"
)

safe_read_html <- function(content) {
  if (is.na(content) || !nzchar(content)) return(NULL)
  tryCatch(read_html(charToRaw(content)), error = function(e) NULL)
}

con <- open_db()
on.exit(dbDisconnect(con), add = TRUE)

# Also store in commoncrawl_samples table for consistency (same schema works)
# We'll mark these with crawl_id = "WAYBACK" so they're identifiable

for (year in c(2010, 2011)) {
  already <- dbGetQuery(con,
    "SELECT COUNT(*) as n FROM commoncrawl_samples
     WHERE year = ? AND fetch_status = 'success'",
    params = list(year))$n
  need <- TARGET - already

  message("\n", strrep("=", 60))
  message("Wayback ", year, " — Have: ", already, " | Need: ", need)
  message(strrep("=", 60))

  if (need <= 0) { message("Already at target. Skipping."); next }

  # Step 1: Gather CDX snapshots from all domains
  # Shuffle domains so retries hit different ones first
  shuffled_domains <- sample(DOMAINS)
  all_snapshots <- tibble()

  for (d in shuffled_domains) {
    if (nrow(all_snapshots) >= need * 5) break  # enough candidates

    tryCatch({
      Sys.sleep(DELAY)
      resp <- request(CDX_BASE_URL) |>
        req_url_query(
          url = paste0(d, "/*"), output = "json",
          from = paste0(year, "0101"), to = paste0(year, "1231"),
          limit = 5000,
          fl = "timestamp,original,statuscode,mimetype",
          filter = "statuscode:200", filter = "mimetype:text/html",
          .multi = "explode") |>
        req_timeout(30) |>
        req_retry(max_tries = 2, backoff = ~ 5) |>
        req_headers("User-Agent" = "ILA2026-Research/1.0 (academic research)") |>
        req_perform()

      body <- resp_body_string(resp)
      if (!nzchar(body)) { message("  ", d, " -> empty"); next }

      parsed <- fromJSON(body, simplifyMatrix = TRUE)
      if (is.null(parsed) || length(parsed) < 2) { message("  ", d, " -> 0"); next }

      headers <- parsed[1, ]
      data <- as_tibble(parsed[-1, , drop = FALSE], .name_repair = "minimal")
      colnames(data) <- headers
      data$domain <- d

      all_snapshots <- bind_rows(all_snapshots, data)
      message("  ", d, " -> ", nrow(data), " snapshots (total: ", nrow(all_snapshots), ")")
    }, error = function(e) {
      message("  ", d, " -> timeout/error")
    })
  }

  message("\nTotal snapshots: ", nrow(all_snapshots))

  if (nrow(all_snapshots) == 0) {
    message("No snapshots available! Try again later.")
    next
  }

  # Deduplicate by URL
  all_snapshots <- all_snapshots |> distinct(original, .keep_all = TRUE)

  # Exclude already-fetched URLs
  existing <- dbGetQuery(con,
    "SELECT original_url FROM commoncrawl_samples WHERE year = ?",
    params = list(year))$original_url
  all_snapshots <- all_snapshots |> filter(!(original %in% existing))

  # Sample across domains proportionally
  sampled <- all_snapshots |> slice_sample(n = min(need * 2, nrow(all_snapshots)))
  message("Sampling ", nrow(sampled), " pages (stopping at ", need, " successes)\n")

  # Step 2: Fetch pages
  success <- 0
  fail <- 0
  start_time <- Sys.time()

  for (i in seq_len(nrow(sampled))) {
    if (success >= need) break

    ts  <- sampled$timestamp[i]
    url <- sampled$original[i]
    domain <- sampled$domain[i]
    wayback_url <- paste0(WAYBACK_BASE, "/", ts, "id_/", url)

    result <- tryCatch({
      Sys.sleep(DELAY)
      resp <- request(wayback_url) |>
        req_timeout(60) |>
        req_retry(max_tries = 2, backoff = ~ 5) |>
        req_headers("User-Agent" = "ILA2026-Research/1.0 (academic research)") |>
        req_perform()

      doc <- safe_read_html(resp_body_string(resp))
      if (is.null(doc)) {
        list(text = NA_character_, status = "error: parse failed")
      } else {
        text <- tryCatch({
          xml2::xml_remove(xml2::xml_find_all(doc, "//script"))
          xml2::xml_remove(xml2::xml_find_all(doc, "//style"))
          str_squish(html_text2(doc))
        }, error = function(e)
          tryCatch(str_squish(html_text2(doc)), error = function(e2) NA_character_))
        list(text = text,
             status = if (!is.na(text) && nzchar(text)) "success" else "error: empty")
      }
    }, error = function(e) {
      list(text = NA_character_, status = paste0("error: ", conditionMessage(e)))
    })

    now <- format(Sys.time(), "%Y-%m-%d %H:%M:%S")
    tryCatch(
      dbExecute(con,
        "INSERT OR IGNORE INTO commoncrawl_samples
         (year, crawl_id, shard_id, original_url, text, text_length,
          languages, fetch_status, collected_at)
         VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
        params = list(
          as.integer(year), "WAYBACK", NA_integer_, url,
          result$text,
          if (!is.na(result$text)) nchar(result$text) else NA_integer_,
          NA_character_,  # No language metadata from Wayback
          result$status, now)),
      error = function(e) NULL)

    if (result$status == "success") success <- success + 1 else fail <- fail + 1

    if ((success + fail) %% 50 == 0 || success >= need) {
      elapsed <- as.numeric(difftime(Sys.time(), start_time, units = "mins"))
      message(sprintf("  %d success, %d fail | %.1f min elapsed", success, fail, elapsed))
    }
  }

  total_now <- dbGetQuery(con,
    "SELECT COUNT(*) as n FROM commoncrawl_samples
     WHERE year = ? AND fetch_status = 'success'",
    params = list(year))$n
  message(sprintf("\n%d: %d successful in DB (target: %d)", year, total_now, TARGET))
}

# =============================================================================
# Export
# =============================================================================

message("\n", strrep("=", 60))
message("EXPORT")
message(strrep("=", 60))

PROCESSED_DIR <- here("data", "processed")

for (year in c(2010, 2011)) {
  data <- read_cc_samples(con, year)
  if (nrow(data) > 0) {
    saveRDS(data, file.path(PROCESSED_DIR, paste0("commoncrawl_", year, "_sample.rds")))
    write_csv(select(data, -text), file.path(PROCESSED_DIR, paste0("commoncrawl_", year, "_metadata.csv")))
    message("CC ", year, ": ", nrow(data), " rows exported")
  } else {
    message("CC ", year, ": no data to export")
  }
}

message("\n=== Final Summary ===")
print(db_summary(con), n = 20)
message("\nDone.")
