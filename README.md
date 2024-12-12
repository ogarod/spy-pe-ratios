# ogarod/spy-pe-ratios

## Introduction
Scripts for calculating SPY PE ratio data, meant to be run as scheduled Cloud
Run Functions in Google Cloud.

The scripts use BigQuery for storage.

Sources for the financial data used are YahooFinance and Financial Modeling
Prep.

## Motivation

I wanted to look at historical valuations for some stocks on the S&P 500 (for
personal research) but was not able to find the metrics I was interested in
online. I therefore wrote these scripts to compute the historical data I
needed. I used Looker Studio to visualize the data.

## Examples

[Here](https://lookerstudio.google.com/reporting/f0edf3a9-c186-4254-8d74-e9c41a0ff0ff)'s
an example of a Looker report based on data computed by the scripts, with
screenshots below: 

Historical PE Ratio Chart:

![Example PE Ratio Chart](https://github.com/ogarod/spy-pe-ratios/blob/main/pe_chart.png)

Historical PE / Mean SPY PE Chart:

![Example PE Ratio To Mean SPY PE Ratio Chart](https://github.com/ogarod/spy-pe-ratios/blob/main/pe_to_mean_spy_pe_chart.png)

## References

- https://www.investopedia.com/terms/p/price-earningsratio.asp
- https://en.wikipedia.org/wiki/SPDR_S%26P_500_ETF_Trust
- https://financialmodelingprep.com/developer/docs/
- https://github.com/ranaroussi/yfinance/tree/main
- https://cloud.google.com/functions/docs/concepts/overview
- https://cloud.google.com/scheduler/docs/overview
- https://cloud.google.com/bigquery/docs/introduction
