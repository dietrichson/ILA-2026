# ILA 2026 Conference Paper

## Project Overview

Academic research project for a paper presentation at the 2026 ILA (International Literacy Association) conference. The project involves sampling and analyzing web pages from historical websites using the Wayback Machine and Common Crawl archives.

## Tech Stack

- **Language**: R
- **Analysis**: tidyverse, tidytext
- **Visualization**: ggplot2
- **Paper**: Quarto (.qmd) rendered to PDF
- **Presentation**: Quarto RevealJS (.qmd)
- **Data Collection**: Wayback Machine CDX API, Common Crawl index/WARC files
- **Storage**: SQLite via DBI + RSQLite (shared database for both collection scripts)

## Project Structure

```
ILA2026/
├── CLAUDE.md              # This file
├── R/                     # R scripts and functions
│   ├── 01-collect-wayback.R       # Wayback Machine collection (domain-targeted)
│   ├── 01-collect-commoncrawl.R   # Common Crawl collection (representative sampling)
│   ├── 02-clean.R         # Text cleaning and preprocessing
│   ├── 03-analyze.R       # Analysis with tidytext
│   └── 04-visualize.R     # ggplot2 figures
├── data/
│   ├── raw/               # Raw scraped HTML/text (gitignored)
│   └── processed/
│       ├── web_archive_samples.db   # SQLite database (shared; gitignored due to size)
│       ├── wayback_sample.rds       # Wayback tibble with text
│       ├── wayback_metadata.csv     # Wayback metadata (no text column)
│       ├── commoncrawl_sample.rds   # CC tibble with text
│       └── commoncrawl_metadata.csv # CC metadata (no text column)
├── paper/
│   ├── paper.qmd          # Quarto manuscript
│   ├── references.bib     # BibTeX references
│   └── _quarto.yml        # Quarto project config
├── presentation/
│   ├── slides.qmd         # RevealJS presentation
│   └── _quarto.yml        # Quarto project config
├── figures/               # Generated ggplot2 figures (.png, .pdf)
├── renv.lock              # R dependency lockfile (renv)
└── .Rprofile              # renv activation
```

## Key Commands

```bash
# Render paper
quarto render paper/paper.qmd

# Render presentation
quarto render presentation/slides.qmd

# Preview presentation locally
quarto preview presentation/slides.qmd

# Restore R dependencies
Rscript -e "renv::restore()"

# Run data collection (resume-safe; re-running skips already-collected samples)
Rscript R/01-collect-wayback.R
Rscript R/01-collect-commoncrawl.R

# Syntax-check scripts without running them
Rscript -e "parse(file='R/01-collect-wayback.R')"
Rscript -e "parse(file='R/01-collect-commoncrawl.R')"

# Inspect the SQLite database
Rscript -e "
  library(DBI); library(RSQLite)
  con <- dbConnect(SQLite(), 'data/processed/web_archive_samples.db')
  print(dbGetQuery(con, 'SELECT year, COUNT(*) AS n FROM wayback_samples GROUP BY year'))
  print(dbGetQuery(con, 'SELECT year, COUNT(*) AS n FROM commoncrawl_samples GROUP BY year'))
  dbDisconnect(con)
"
```

## Data Collection Notes

### Wayback Machine (`01-collect-wayback.R`)
- Queries `web.archive.org/cdx/search/cdx` with server-side filters (`statuscode:200`, `mimetype:text/html`) and `.multi = "explode"` to avoid duplicate filter keys.
- Fetches raw HTML from `web.archive.org/web/{timestamp}id_/{url}` (no toolbar injected).
- Domain-targeted: samples from `nytimes.com`, `bbc.co.uk`, `cnn.com`, `washingtonpost.com`, `theguardian.com`.
- Coverage: 1996-2026, 1 page per year/domain combination.

### Common Crawl (`01-collect-commoncrawl.R`)
- **Representative sampling** (no domain restriction): draws pages from the full CC crawl, not a specific set of domains.
- Primary approach: queries `index.commoncrawl.org/{crawl_id}-index` with `url=*` and random page numbers (using `showNumPages=true` first to discover page count).
- Fallback: if `url=*` returns 0 results, iterates over a broad set of URL prefixes (`*.com`, `*.org`, `*.de`, etc.) to approximate random sampling.
- Fetches WARC records from `data.commoncrawl.org` via HTTP byte-range requests (only the exact bytes for each record, not the full WARC file).
- Coverage: 2008-2026, 1 page per year (total).
- Crawl IDs matched via `collinfo.json`; pattern `CC-MAIN-{year}` handles both `CC-MAIN-YYYY-WW` (standard) and `CC-MAIN-YYYY` (early) formats.

### Shared SQLite Backend
- Both scripts write to `data/processed/web_archive_samples.db`.
- Tables: `wayback_samples`, `commoncrawl_samples`.
- Each row is inserted immediately after fetching using `INSERT OR IGNORE` (deduplication via UNIQUE constraint).
- Both scripts check the DB at startup and skip year/domain combinations that already have enough successful samples, making reruns safe and resumable.
- At the end of each script, results are exported to `.rds` (full, with text) and `.csv` (metadata only, text excluded).

## R Package Dependencies

Core packages (manage with renv):
- tidyverse (dplyr, tidyr, stringr, readr, purrr, ggplot2)
- tidytext
- httr2 (HTTP requests for Wayback/CC APIs)
- rvest (HTML parsing)
- jsonlite (API response parsing)
- DBI + RSQLite (SQLite backend for incremental collection and resume)
- here (project-relative paths)
- quarto

## Conventions

- Use tidyverse style (snake_case, pipes)
- ggplot2 themes should be consistent across paper and presentation
- All figures saved to `figures/` in both PNG (presentation) and PDF (paper)
- Use renv for reproducibility
- Commit processed `.rds`/`.csv` exports but not raw HTML or the `.db` file
