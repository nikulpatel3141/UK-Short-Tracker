# UK Short Tracker

This repository contains code to display metrics for UK disclosed short positions. You can see the output on my website: https://nikulpatel.dev/projects/uk_short_tracker/

The process is relatively straightforward:
1) Download UK Short Disclosures from the FCA website
    - https://www.fca.org.uk/markets/short-selling/notification-disclosure-net-short-positions
2) The disclosures are identified by ISINs, so query OpenFIGI to get the underlying tickers
3) Use these tickers to query for market data from Yahoo Finance
4) For simplicity we save all data to a SQLite database in [data.sqlite](data/)
    - This is for keeping the most recent data for disclosures and shares outstanding since these are updated daily and cannot be historically queried
      - (actually historical disclosures data is available but is difficult to work with. See the Notebooks/hist_scripting.ipynb notebook for more details)
5) Load the data, calculate all required metrics
6) Use `pandas.style` to nicely format the tables as HTML and save as a json in [output/output.json](output/)
7) Commit the changes to `data/` and `output/` to the repository

This procedure is scheduled to run using GitHub actions every day using [this](.github/workflows/run_tracker.yml) workflow file.

## Acknowledgements

(Some) inspiration for this project comes from another short tracker by Castellain Capital: https://shorttracker.co.uk/

## Data Sources

This uses various sourcees of data to compile the report:
- FCA (for the short filings)
- Yahoo Finance (for stock prices + volume + shares outstanding)
- OpenFIGI (to find tickers given ISINs)

AFAIK I'm not breaking any rules by using data in this way. If I am, please let me know.

## TODO
General code cleanup + refactoring.
