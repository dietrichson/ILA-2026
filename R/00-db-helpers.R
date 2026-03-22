# =============================================================================
# 00-db-helpers.R
# =============================================================================
# Shared database helpers for the ILA 2026 project.
# Source this file from any script that needs DB access:
#   source(here::here("R", "00-db-helpers.R"))
# =============================================================================

if (!requireNamespace("DBI", quietly = TRUE) ||
    !requireNamespace("RSQLite", quietly = TRUE) ||
    !requireNamespace("here", quietly = TRUE)) {
  stop("Required packages: DBI, RSQLite, here\n",
       "Install with: install.packages(c('DBI', 'RSQLite', 'here'))")
}

library(DBI)
library(RSQLite)
library(here)

DB_PATH <- here("data", "processed", "web_archive_samples.db")

#' Open the shared SQLite database, creating tables if needed.
#'
#' @return A DBI connection object. Caller is responsible for disconnecting.
open_db <- function() {
  dir.create(dirname(DB_PATH), showWarnings = FALSE, recursive = TRUE)
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

#' Read successful Common Crawl samples for given years.
#'
#' @param con   DBI connection from open_db()
#' @param years Integer vector of years (default: all years)
#' @return A tibble with all columns from commoncrawl_samples.
read_cc_samples <- function(con, years = NULL) {
  if (is.null(years)) {
    query <- "SELECT * FROM commoncrawl_samples WHERE fetch_status = 'success'"
    params <- list()
  } else {
    placeholders <- paste(rep("?", length(years)), collapse = ", ")
    query <- paste0(
      "SELECT * FROM commoncrawl_samples WHERE fetch_status = 'success' AND year IN (",
      placeholders, ")")
    params <- as.list(as.integer(years))
  }
  tibble::as_tibble(dbGetQuery(con, query, params = params))
}

#' Read successful Wayback Machine samples for given years.
#'
#' @param con   DBI connection from open_db()
#' @param years Integer vector of years (default: all years)
#' @return A tibble with all columns from wayback_samples.
read_wb_samples <- function(con, years = NULL) {
  if (is.null(years)) {
    query <- "SELECT * FROM wayback_samples WHERE fetch_status = 'success'"
    params <- list()
  } else {
    placeholders <- paste(rep("?", length(years)), collapse = ", ")
    query <- paste0(
      "SELECT * FROM wayback_samples WHERE fetch_status = 'success' AND year IN (",
      placeholders, ")")
    params <- as.list(as.integer(years))
  }
  tibble::as_tibble(dbGetQuery(con, query, params = params))
}

#' Summary counts per year for a given table.
#'
#' @param con   DBI connection
#' @param table "commoncrawl_samples" or "wayback_samples"
#' @return A tibble with year, success, fail columns.
db_summary <- function(con, table = "commoncrawl_samples") {
  tibble::as_tibble(dbGetQuery(con, paste0(
    "SELECT year,
            SUM(CASE WHEN fetch_status = 'success' THEN 1 ELSE 0 END) AS success,
            SUM(CASE WHEN fetch_status != 'success' THEN 1 ELSE 0 END) AS fail
     FROM ", table, " GROUP BY year ORDER BY year")))
}
