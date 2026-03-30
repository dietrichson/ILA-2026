#!/bin/bash
cd /Users/sasha/Desktop/ILA2026

for attempt in $(seq 1 15); do
  echo "=== Attempt $attempt/15 at $(date) ==="
  
  # Check which years still need collection
  DONE=$(Rscript -e '
    suppressPackageStartupMessages({library(DBI);library(RSQLite);library(here)})
    con <- dbConnect(SQLite(), here("data","processed","web_archive_weighted.db"))
    n <- tryCatch(dbGetQuery(con,"SELECT COUNT(DISTINCT year) as n FROM samples WHERE fetch_status=\"success\" GROUP BY year HAVING COUNT(*) >= 1000")$n, error=function(e) 0)
    cat(length(n))
    dbDisconnect(con)
  ' 2>/dev/null)
  echo "Years complete: $DONE / 15"
  
  if [ "$DONE" = "15" ]; then
    echo "All 15 years complete!"
    break
  fi
  
  # Try to run collection
  STATUS=$(curl -s -o /dev/null -w "%{http_code}" --max-time 15 "https://index.commoncrawl.org/CC-MAIN-2026-12-index?url=*.com&showNumPages=true&output=json" 2>/dev/null)
  echo "API status: $STATUS"
  
  if [ "$STATUS" = "200" ]; then
    echo "Running collection..."
    Rscript R/run-collect-weighted.R 2>&1
  else
    echo "API down. Waiting 30 min..."
    sleep 1800
  fi
done
echo "=== Finished at $(date) ==="
