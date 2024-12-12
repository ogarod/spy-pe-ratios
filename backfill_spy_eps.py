"""Backfills historical EPS values for SPY components from the Financial Modeling Prep API.

As of 10/2024, FMP requires you to pay them ~30 dollars a month to access the EPS data.

You would typically want to run the backfill script once then schedule `fetch_current_spy_eps.py` to
run every day.
"""
import functions_framework
import datetime
import operator
import pandas as pd
from google.cloud import bigquery
from google.oauth2 import service_account
import time
import backoff
import urllib

# Get rid of the SettingWithCopyWarning warning.
pd.options.mode.copy_on_write = True

_API_KEY = "dummy-fmp-key"
_SPY_COMPONENTS_URL = "https://www.ssga.com/us/en/individual/library-content/products/fund-data/etfs/us/holdings-daily-us-en-spy.xlsx"
_TTM_EPS_TABLE_ID = "dummy-gcloud-project.spy_components.ttm_eps"
_SPY_COMPONENTS_LIST_TABLE_ID = "dummy-gcloud-project.spy_components.history"

_EARNINGS_URL_TEMPLATE = f"https://financialmodelingprep.com/api/v3/historical/earning_calendar/%s?apikey={_API_KEY}"

sa_key = {}  # Google cloud credentials go here.


@functions_framework.http
def backfill_from_fmp(request):
    creds = service_account.Credentials.from_service_account_info(sa_key)
    bq_client = bigquery.Client(
        credentials=creds, project="dummy-gcloud-project")
    bq_rows = []
    for i, (ticker, name) in enumerate(fetch_spy_components_from_bq(bq_client)):
        eps_values = fetch_historical_eps(ticker)
        if eps_values.empty:
            print(
                f"Received no results for {ticker}. Will skip it when writing to Bigquery.")
            continue
        raw_ttm_eps_values = convert_to_ttm_eps(eps_values)
        interpolated_ttm_eps_values = interpolate_dates(
            raw_ttm_eps_values, datetime.date.today())
        for date, value in interpolated_ttm_eps_values:
            bq_rows.append({
                "ticker": ticker,
                "name": name,
                # The Bigquery API does not accept datetime.date objects.
                "date": date.strftime("%Y-%m-%d"),
                "ttm_eps": value,
            })
        if (i+1) % 10 == 0:
            print(
                f"Progress: Completed fetching EPS values for {i+1} components")
    job_config = bigquery.LoadJobConfig(
        schema=[
            bigquery.SchemaField("ticker", "STRING", mode="REQUIRED"),
            bigquery.SchemaField("name", "STRING", mode="REQUIRED"),
            bigquery.SchemaField("date", "DATE", mode="REQUIRED"),
            bigquery.SchemaField("ttm_eps", "FLOAT"),
        ],
        # Overwrite the table if it already exists.
        write_disposition="WRITE_TRUNCATE",
    )
    job = bq_client.load_table_from_json(
        bq_rows, destination=_TTM_EPS_TABLE_ID, job_config=job_config)
    print("Waiting for BigQuery write operation to complete...")
    job.result()
    return "Backfill completed successfully."


def _is_valid_eps(eps):
    return eps is not None and eps >= 0


# Retry on 429 (too many requests) error
@backoff.on_exception(backoff.expo,
                      urllib.error.HTTPError,
                      max_time=60)
def fetch_historical_eps(ticker: str) -> pd.DataFrame:
    earnings_df = pd.read_json(
        _EARNINGS_URL_TEMPLATE % ticker, convert_dates=False)
    try:
        # Only interested in these two columns.
        earnings_df = earnings_df[["date", "eps"]]
    except KeyError:
        # This happened when AMTM was new, on Oct 1 2024.
        # Happened again on Oct 2 2024 for a weird entry: BLACKROCK FUNDING INC/DE:2481632D
        print(f"Invalid result from FMP for ticker {ticker}.")
        return pd.DataFrame(columns=["date", "eps"])
    today = datetime.date.today().strftime("%Y-%m-%d")

    # Drop entries for future earnings releases.
    future_rows = earnings_df[earnings_df["date"] > today]
    earnings_df.drop(index=future_rows.index, inplace=True)

    return earnings_df


def convert_to_ttm_eps(
    earnings_df: pd.DataFrame
) -> list[tuple[datetime.date, float]]:
    eps_values = earnings_df["eps"].values
    ttm_eps_values = []
    for i, (earnings_date, eps) in enumerate(earnings_df.itertuples(index=False)):
        if i + 4 > len(eps_values):
            # There are not enough quarters to compute the TTM EPS.
            break

        # If all eps values in the trailing 12 months are valid, add them up,
        # otherwise assign an EPS value of None which should be interpreted as
        # null by the Bigquery API.
        if all([_is_valid_eps(e) for e in eps_values[i:i+4]]):
            ttm_eps = sum(eps_values[i:i+4])
        else:
            ttm_eps = None
        ttm_eps_values.append(
            (datetime.datetime.strptime(earnings_date, "%Y-%m-%d").date(),
             ttm_eps)
        )
    return ttm_eps_values


def interpolate_dates(
    input_data: list[tuple[datetime.date, float]],
    interpolate_to: datetime.date,
) -> list[tuple[datetime.date, float]]:
    """Interpolates missing dates in the provided data.

    Each generated date is given the value of the input date right behind it
    in the input data. The data is not assumed to be sorted.

    Returns the interpolated data as a list of (date, value) tuples, in reverse
    chronological order.
    """
    # Sort chronologically.
    input_data = sorted(input_data, key=operator.itemgetter(0))
    interpolated_data = []
    for i, (date, value) in enumerate(input_data):
        interpolated_data.append((date, value))
        if i == len(input_data) - 1:
            if interpolate_to < date:
                raise ValueError(
                    f"Provided interpolate_to value [{interpolate_to}] is < "
                    f"max date [{date}] in input data.")
            elif interpolate_to == date:
                # Date already added to `interpolated_data`.
                break
            else:
                interpolated_date = date + datetime.timedelta(days=1)
                while interpolated_date <= interpolate_to:
                    interpolated_data.append((interpolated_date, value))
                    interpolated_date += datetime.timedelta(days=1)
        else:
            next_date = input_data[i+1][0]
            interpolated_date = date + datetime.timedelta(days=1)
            while interpolated_date < next_date:
                interpolated_data.append((interpolated_date, value))
                interpolated_date += datetime.timedelta(days=1)

    interpolated_data.reverse()
    return interpolated_data


def fetch_spy_components_from_bq(bq_client):
    query_result = bq_client.query(
        f"SELECT components_list from {_SPY_COMPONENTS_LIST_TABLE_ID} "
        "ORDER BY publish_date desc limit 1").result()
    components = []
    snapshot = list(query_result)[0][0]
    for entry in snapshot.split(","):
        name = entry.split(":")[0]
        ticker = entry.split(":")[1]
        components.append((ticker, name))
    return components
