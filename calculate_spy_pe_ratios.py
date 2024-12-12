"""Calculates SPY PE ratio data based on EPS values already stored in BigQuery."""
import functions_framework
from google.cloud import bigquery
from google.oauth2 import service_account
import pandas as pd
import numpy as np
import yfinance as yf
import datetime
import time
from concurrent import futures
from collections.abc import Iterable, Sequence
import operator
from scipy import stats
import backoff
import urllib

# Get rid of the SettingWithCopyWarning warning.
pd.options.mode.copy_on_write = True

_TTM_EPS_TABLE_ID = "dummy-gcloud-project.spy_components.ttm_eps"
_TTM_PE_RATIO_TABLE_ID = "dummy-gcloud-project.spy_components.ttm_pe_ratio"
_AGGREGATIONS_TABLE_ID = "dummy-gcloud-project.spy_components.pe_ratio_aggregations"

# Minimum number of valid PE ratios to consider for any given day.
_MIN_SAMPLE_SIZE = 50

_MAX_YFINANCE_THREADS = 2


@functions_framework.http
def calculate_spy_pe_data(request):
    start_time = time.time()
    calculate_pe_ratios()
    calculate_spy_aggregations()
    return f"SPY P/E calculations completed in {time.time() - start_time} seconds."


def create_bq_client() -> bigquery.Client:
    sa_key = {}  # Google cloud credentials go here.
    credentials = service_account.Credentials.from_service_account_info(sa_key)
    client = bigquery.Client(credentials=credentials,
                             project=credentials.project_id)
    return client


def fetch_ttm_eps_values(bq_client: bigquery.Client) -> pd.DataFrame:
    query = f"SELECT * FROM `{_TTM_EPS_TABLE_ID}`"
    return bq_client.query_and_wait(query).to_dataframe()


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


def interpolate_price_history(
        price_history: pd.DataFrame, max_date: datetime.date) -> pd.DataFrame:
    history_entries = []
    for date, price in price_history.itertuples(index=False):
        history_entries.append((date, price))
    interpolated_entries = interpolate_dates(history_entries, max_date)
    return pd.DataFrame(interpolated_entries, columns=["date", "price"])


# Retry on 429 (too many requests) error.
@backoff.on_exception(backoff.expo,
                      urllib.error.HTTPError,
                      max_time=60)
def fetch_price_history(ticker: str, eps_values: pd.DataFrame):
    min_date = eps_values[eps_values["ticker"] == ticker]["date"].min()
    max_date = eps_values[eps_values["ticker"] == ticker]["date"].max()
    # The SPY list on the SSGA website uses period instead of hyphen for
    # some tickers. Yahoo Finance returns a YFTzMissingError for such tickers.
    history = yf.Ticker(ticker.replace(".", "-")).history(
        start=min_date,
        # The yfinance library treats end date as exclusive; we want to include
        # the max_date.
        end=max_date + datetime.timedelta(days=1),
        interval="1d",
        raise_errors=True)
    # Remove the DatetimeIndex.
    history["date"] = history.index.date
    history.reset_index(inplace=True)
    history.rename(columns={"Close": "price"}, inplace=True)
    # Return the close prices. Note that for the current trading day that has
    # not ended yfinance makes up a Close price.
    return interpolate_price_history(history[["date", "price"]], max_date)


def fetch_price_histories(
        tickers: Sequence[str],
        eps_values: pd.DataFrame
) -> Sequence[pd.DataFrame]:
    price_histories = []
    with futures.ThreadPoolExecutor(
            max_workers=_MAX_YFINANCE_THREADS) as executor:
        # Start price downloads.
        future_to_ticker = {
            executor.submit(fetch_price_history, ticker, eps_values): ticker
            for ticker in tickers
        }
        for i, future in enumerate(futures.as_completed(future_to_ticker)):
            ticker = future_to_ticker[future]
            try:
                price_history = future.result()
            except Exception as e:
                print(f"{ticker} generated an exception: {e}")
            else:
                # Add ticker column to accompany the price data. The column
                # is needed for a JOIN later.
                price_history["ticker"] = ticker
                price_histories.append(price_history)
            finally:
                if (i+1) % 10 == 0:
                    print(f"Fetched prices for {i + 1} securities so far.")
    return price_histories


def calculate_pe_ratios():
    """Calculates historical PE ratios for SPY holdings.

    Uses TTM EPS values already stored in BigQuery. Currently relies on the
    yfinance library for historical price data. The results are written to a
    different BigQuery table.

    Backfilling the entire price history from periodically should fix historical
    data affected by stock splits (YahooFinance backfills their data after stock
    splits).
    """
    bq_client = create_bq_client()
    print("Fetching EPS values from Bigquery...")
    eps_values = fetch_ttm_eps_values(bq_client)
    print("Starting download of price data from YahooFinance.")
    tickers = eps_values["ticker"].unique()
    download_start = time.time()
    price_histories = fetch_price_histories(tickers, eps_values)
    print(f"Price download took {time.time() - download_start} seconds.")

    # Join individual per-ticker dataframes into one dataframe.
    price_histories = pd.concat(price_histories, ignore_index=True)

    join_start = time.time()
    joined_data = pd.merge(
        left=eps_values,
        right=price_histories,
        on=["date", "ticker"],
        how="left")
    print(f"Joining took {time.time() - join_start} seconds.")
    joined_data.sort_values(by=["ticker", "date"],
                            inplace=True, ignore_index=True)

    # Calculate trailing PE ratio.
    joined_data["ttm_pe_ratio"] = joined_data["price"] / joined_data["ttm_eps"]

    job_config = bigquery.LoadJobConfig(
        schema=[
            bigquery.SchemaField("ticker", "STRING", mode="REQUIRED"),
            bigquery.SchemaField("name", "STRING", mode="REQUIRED"),
            bigquery.SchemaField("date", "DATE", mode="REQUIRED"),
            bigquery.SchemaField("price", "FLOAT"),
            bigquery.SchemaField("ttm_eps", "FLOAT"),
            bigquery.SchemaField("ttm_pe_ratio", "FLOAT"),
        ],
        # Overwrite the table if it already exists.
        write_disposition="WRITE_TRUNCATE",
    )
    job = bq_client.load_table_from_dataframe(
        joined_data, destination=_TTM_PE_RATIO_TABLE_ID, job_config=job_config)
    print("Waiting for BigQuery write operation to complete...")
    job.result()


def _get_valid_ratios(ratios: Iterable) -> Sequence[float]:
    return [ratio for ratio in ratios if pd.notna(ratio) and ratio > 0]


def _zscore_trim_mean(ratios: Sequence[float], max_score: float) -> float:
    scores = np.absolute(stats.zscore(ratios))
    filtered = [ratios[i]
                for i in range(len(ratios)) if scores[i] <= max_score]
    return np.mean(filtered)


def aggregate_pe_ratios(pe_ratios: pd.DataFrame) -> pd.DataFrame:
    ratios_by_date = pe_ratios[["date", "ttm_pe_ratio"]].groupby(
        "date").agg(_get_valid_ratios)
    ratios_by_date.rename(
        columns={"ttm_pe_ratio": "ttm_pe_ratios"}, inplace=True)
    ratios_by_date.sort_values("date", inplace=True, ascending=False)
    ratios_by_date["num_valid_ratios"] = ratios_by_date["ttm_pe_ratios"].apply(
        len)
    sample_size_rejects = ratios_by_date[ratios_by_date["num_valid_ratios"]
                                         < _MIN_SAMPLE_SIZE]
    ratios_by_date.drop(index=sample_size_rejects.index, inplace=True)
    ratios_by_date["mean"] = ratios_by_date["ttm_pe_ratios"].apply(np.mean)
    ratios_by_date["median"] = ratios_by_date["ttm_pe_ratios"].apply(np.median)
    # Exclude upper 2.5% and lower 2.5% percentiles before calculating the mean.
    ratios_by_date["trimmed_mean_2_5"] = ratios_by_date["ttm_pe_ratios"].apply(
        lambda x: stats.trim_mean(x, 0.025))
    # Exclude upper 5% and lower 5% percentiles before calculating the mean.
    ratios_by_date["trimmed_mean_5"] = ratios_by_date["ttm_pe_ratios"].apply(
        lambda x: stats.trim_mean(x, 0.05))
    # Exclude PE ratios that are more than 2 standard deviations away from the arithmetic mean (5% for normal distributions).
    ratios_by_date["zscore_trimmed_mean_2"] = ratios_by_date["ttm_pe_ratios"].apply(
        lambda x: _zscore_trim_mean(x, 2))
    # Exclude PE ratios that are more than 3 standard deviations away from the arithmetic mean (0.3% for normal distributions).
    ratios_by_date["zscore_trimmed_mean_3"] = ratios_by_date["ttm_pe_ratios"].apply(
        lambda x: _zscore_trim_mean(x, 3))
    return ratios_by_date


def calculate_spy_aggregations():
    bq_client = create_bq_client()
    query = f"SELECT * FROM {_TTM_PE_RATIO_TABLE_ID}"
    pe_ratios = bq_client.query_and_wait(query).to_dataframe()
    aggregate_start = time.time()
    print("Calculating aggregations...")
    aggregations = aggregate_pe_ratios(pe_ratios)
    print(
        f"Aggregations calculated in {time.time() - aggregate_start} seconds.")

    job_config = bigquery.LoadJobConfig(
        schema=[
            bigquery.SchemaField("date", "DATE", mode="REQUIRED"),
            bigquery.SchemaField("ttm_pe_ratios", "FLOAT", mode="REPEATED"),
            bigquery.SchemaField("num_valid_ratios",
                                 "INTEGER", mode="REQUIRED"),
            bigquery.SchemaField("mean", "FLOAT"),
            bigquery.SchemaField("median", "FLOAT"),
            bigquery.SchemaField("trimmed_mean_2_5", "FLOAT"),
            bigquery.SchemaField("trimmed_mean_5", "FLOAT"),
            bigquery.SchemaField("zscore_trimmed_mean_2", "FLOAT"),
            bigquery.SchemaField("zscore_trimmed_mean_3", "FLOAT"),
        ],
        # Overwrite the table if it already exists.
        write_disposition="WRITE_TRUNCATE",
    )
    job = bq_client.load_table_from_dataframe(
        aggregations, destination=_AGGREGATIONS_TABLE_ID, job_config=job_config)
    print("Waiting for BigQuery write operation to complete...")
    job.result()
