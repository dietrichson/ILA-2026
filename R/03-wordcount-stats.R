# =============================================================================
# 03-wordcount-stats.R
# =============================================================================
# Computes word count per document statistics for CC 2009 vs CC 2026.
# Uses tidytext to tokenize, then summarises by year.
#
# Usage:
#   Rscript R/03-wordcount-stats.R
# =============================================================================

suppressPackageStartupMessages({
  library(tidyverse)
  library(tidytext)
  library(here)
})

source(here("R", "00-db-helpers.R"))

con <- open_db()
on.exit(dbDisconnect(con))

# Load successful samples for 2009 and 2026
samples <- read_cc_samples(con, years = c(2009, 2026)) |>
  select(id, year, original_url, text)

message("Loaded ", nrow(samples), " documents (",
        sum(samples$year == 2009), " from 2009, ",
        sum(samples$year == 2026), " from 2026)")

# Tokenize into words using tidytext
words <- samples |>
  select(id, year, text) |>
  unnest_tokens(word, text)

# Word count per document
doc_wordcounts <- words |>
  count(id, year, name = "word_count")

# Summary statistics by year
stats <- doc_wordcounts |>
  group_by(year) |>
  summarise(
    n_docs    = n(),
    mean_wc   = mean(word_count),
    median_wc = median(word_count),
    sd_wc     = sd(word_count),
    min_wc    = min(word_count),
    max_wc    = max(word_count),
    q25       = quantile(word_count, 0.25),
    q75       = quantile(word_count, 0.75),
    .groups   = "drop"
  )

message("\n=== Word Count per Document ===\n")
print(stats, width = Inf)

message("\n=== Comparison ===")
s09 <- filter(stats, year == 2009)
s26 <- filter(stats, year == 2026)
message(sprintf("Mean:   2009 = %.0f words, 2026 = %.0f words (%.1f%% change)",
                s09$mean_wc, s26$mean_wc,
                (s26$mean_wc - s09$mean_wc) / s09$mean_wc * 100))
message(sprintf("Median: 2009 = %.0f words, 2026 = %.0f words (%.1f%% change)",
                s09$median_wc, s26$median_wc,
                (s26$median_wc - s09$median_wc) / s09$median_wc * 100))
