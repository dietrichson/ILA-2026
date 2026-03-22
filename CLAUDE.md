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

## Project Structure

```
ILA2026/
├── CLAUDE.md              # This file
├── R/                     # R scripts and functions
│   ├── 01-collect.R       # Web scraping / data collection
│   ├── 02-clean.R         # Text cleaning and preprocessing
│   ├── 03-analyze.R       # Analysis with tidytext
│   └── 04-visualize.R     # ggplot2 figures
├── data/
│   ├── raw/               # Raw scraped HTML/text (gitignored)
│   └── processed/         # Cleaned datasets (.rds, .csv)
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
```

## Data Collection Notes

- **Wayback Machine**: Use the CDX API (`web.archive.org/cdx/search/cdx`) to find archived snapshots, then fetch raw HTML from `web.archive.org/web/{timestamp}id_/{url}`
- **Common Crawl**: Query the index API (`index.commoncrawl.org`) then fetch WARC records from S3
- Goal: sample pages going as far back as possible
- Store raw HTML in `data/raw/` (gitignored due to size)
- Processed text corpora in `data/processed/`

## R Package Dependencies

Core packages (manage with renv):
- tidyverse (dplyr, tidyr, stringr, readr, purrr, ggplot2)
- tidytext
- httr2 (HTTP requests for Wayback/CC APIs)
- rvest (HTML parsing)
- jsonlite (API response parsing)
- quarto

## Conventions

- Use tidyverse style (snake_case, pipes)
- ggplot2 themes should be consistent across paper and presentation
- All figures saved to `figures/` in both PNG (presentation) and PDF (paper)
- Use renv for reproducibility
- Commit processed data but not raw HTML
