"""Fetches today's SPY EPS values from YahooFinance.

See `backfill_spy_eps.py` for how to fetch historical EPS data.
"""
import functions_framework
import datetime
import yfinance as yf
from google.cloud import bigquery
from google.oauth2 import service_account
import time

_EPS_TABLE_ID = "dummy-gcloud-project.spy_components.ttm_eps"
_SPY_COMPONENTS_LIST_TABLE_ID = "dummy-gcloud-project.spy_components.history"


@functions_framework.http
def fetch_today_eps_values(request):
    bq_client = create_bq_client()
    query_result = bq_client.query(
        f"SELECT MAX(date) from {_EPS_TABLE_ID}").result()
    most_recent_date = list(query_result)[0][0]
    today = datetime.date.today()
    if most_recent_date >= datetime.date.today():
        return f"Most recent date in EPS table {most_recent_date} is current."
    query_result = bq_client.query(
        f"SELECT components_list from {_SPY_COMPONENTS_LIST_TABLE_ID} "
        "ORDER BY publish_date desc limit 1").result()
    snapshot = list(query_result)[0][0]
    new_rows = []
    print("Fetching EPS values from YahooFinance...")
    start_time = time.time()
    for entry in snapshot.split(","):
        name = entry.split(":")[0]
        ticker = entry.split(":")[1]
        try:
            # The SPY list on the SSGA website uses period instead of hyphen for
            # some tickers. Yahoo Finance returns a YFTzMissingError for such
            # tickers.
            eps = yf.Ticker(ticker.replace(".", "-")).info["trailingEps"]
            new_rows.append({
                "ticker": ticker,
                "name": name,
                # The Bigquery API does not accept datetime.date objects.
                "date": today.strftime("%Y-%m-%d"),
                "ttm_eps": eps,
            })
        except KeyError:
            # SSGA data is known to have entries that are not recognized by YahooFinance.
            # E.g. on Oct 2 2024: BLACKROCK FUNDING INC/DE:2481632D
            print(f"Failed to find EPS value for {ticker}")
    print(f"EPS lookup took {time.time() - start_time} seconds.")
    job_config = bigquery.LoadJobConfig(
        schema=[
            bigquery.SchemaField("ticker", "STRING", mode="REQUIRED"),
            bigquery.SchemaField("name", "STRING", mode="REQUIRED"),
            bigquery.SchemaField("date", "DATE", mode="REQUIRED"),
            bigquery.SchemaField("ttm_eps", "FLOAT"),
        ],
        write_disposition="WRITE_APPEND",
    )
    job = bq_client.load_table_from_json(
        new_rows, destination=_EPS_TABLE_ID, job_config=job_config)
    print("Waiting for BigQuery write operation to complete...")
    job.result()
    return f"Fetched EPS values for {len(new_rows)} SPY holdings."


def create_bq_client() -> bigquery.Client:
    sa_key = {}  # Google cloud credentials go here.
    credentials = service_account.Credentials.from_service_account_info(sa_key)
    client = bigquery.Client(credentials=credentials,
                             project=credentials.project_id)
    return client
