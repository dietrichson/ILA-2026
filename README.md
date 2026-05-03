# ILA-2026

Academic research project for a paper presentation at the 2026 ILA conference. Analyzes English-language web page text from Common Crawl archives (2012-2026) to study longitudinal changes in web content.

Presentation available at: <https://dietrichson.github.io/ILA-2026/>

## Data Samples

This project maintains two separate web page samples, each with different sampling methodologies:

### Sample 1: Balanced TLD (equal-weight)

-   **Database**: `data/processed/web_archive_samples.db`
-   **Exported files**: `data/processed/commoncrawl_{year}_sample.rds` / `_metadata.csv`
-   **Script**: `R/run-collect-all.R`
-   **Method**: Equal number of index pages fetched per TLD prefix (3 pages each from 15 TLDs). Domain diversity enforced (max 5 pages per registered domain). English-only filtering.
-   **Bias**: Overrepresents small TLDs (`.co.nz`, `.ie`, `.co.uk`) relative to their actual share of the English web. Each TLD contributes \~equal candidates regardless of size. `.com` is 9% of sample vs \~74% of CC index.
-   **Use case**: Maximizes geographic/TLD diversity. Useful if equal representation across English-speaking regions is desired.

### Sample 2: Proportionally weighted

-   **Database**: `data/processed/web_archive_weighted.db`
-   **Exported files**: `data/processed/weighted_{year}_sample.rds` / `_metadata.csv`
-   **Script**: `R/run-collect-weighted.R`
-   **Method**: Index pages allocated proportionally to each TLD's actual size in the Common Crawl index. For example, `*.com` (\~74% of CC) gets \~736 of 1000 pages, while `*.nz` (\~0.4%) gets \~4. Domain diversity still enforced (max 5 pages per registered domain). English-only filtering.
-   **Bias**: More representative of the actual English web as indexed by Common Crawl, but dominated by `.com` domains.
-   **Use case**: Better approximation of "what the English web looks like" for linguistic analysis.

### Common parameters (both samples)

-   **Years**: 2012-2026 (15 years)
-   **Pages per year**: 1000
-   **Source**: Common Crawl archives
-   **Language filter**: English only (CC `languages` field containing "eng")
-   **Domain cap**: Max 5 pages per registered domain per year
-   **Storage**: SQLite with resume support (re-run scripts to continue interrupted collection)
-   **Text extraction**: HTML fetched via WARC byte-range requests, parsed with rvest, scripts/styles removed

## Tech Stack

-   **Language**: R (tidyverse, tidytext, httr2, rvest, DBI/RSQLite)
-   **Paper**: Quarto (.qmd) rendered to PDF
-   **Presentation**: Quarto RevealJS
-   **Data**: Common Crawl Index API + WARC byte-range fetches from S3
