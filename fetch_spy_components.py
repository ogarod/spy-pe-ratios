"""Snapshots the list of SPY components from the SSGA website."""
import functions_framework
import datetime
import operator
import pandas as pd
from typing import Tuple
from google.cloud import bigquery
from google.oauth2 import service_account
from google import api_core
from absl import logging


_SPY_COMPONENTS_TABLE_ID = "dummy-gcloud-project.spy_components.history"
_SPY_COMPONENTS_URL = "https://www.ssga.com/us/en/individual/library-content/products/fund-data/etfs/us/holdings-daily-us-en-spy.xlsx"

_GET_MOST_RECENT_COMPONENTS_QUERY = f"""
    SELECT publish_date, components_list
    FROM `{_SPY_COMPONENTS_TABLE_ID}`
    ORDER BY publish_date DESC
    LIMIT 1
"""

# google cloud credentials go here.
sa_key = {}


# Triggered from a message on a Cloud Pub/Sub topic.
@functions_framework.cloud_event
def fetch_and_store_spy_components(cloud_event):
    publish_date, live_components = fetch_spy_components()
    creds = service_account.Credentials.from_service_account_info(sa_key)
    bq_client = bigquery.Client(
        credentials=creds, project="dummy-gcloud-project")
    try:
        query_results = bq_client.query(
            _GET_MOST_RECENT_COMPONENTS_QUERY).result()
        last_saved_components = list(query_results)[0].values()[1]
        if live_components == last_saved_components:
            logging.info("No change in the components list since last saved version "
                         f"{publish_date.strftime('%Y-%m-%d')}.")
            return
    except api_core.exceptions.NotFound:
        # Table does not exist, that's ok, just create it.
        pass

    update_spy_bigquery_table(publish_date, live_components, bq_client)


def is_valid_entry(spy_entry):
    # Should not contain these special characters.
    return "," not in spy_entry and ":" not in spy_entry


def fetch_spy_components() -> Tuple[datetime.date, str]:
    """Fetches SPY holdings from the SSGA website, along with the publish date.

    Returns a tuple containing the publish date and a comma-separated string of
    the SPY components (each entry is the ticker followed by a colon then the
    name of the security).
    """

    raw_contents_df = pd.read_excel(_SPY_COMPONENTS_URL)
    title_row_index = 3

    # Create a series that indicates whether rows after the title-row in the
    # spreadsheet are empty.
    row_empty_mask = raw_contents_df.iloc[title_row_index:].isnull().all(
        axis=1)

    # Get the index of the first empty row in the spreadsheet (after the title
    # row).
    first_empty_row_index = row_empty_mask.argmax() + title_row_index

    # Create a data-frame containing SPY components.
    column_names = raw_contents_df.iloc[title_row_index].values
    data_rows = (
        raw_contents_df.iloc[title_row_index+1:first_empty_row_index].values)
    components_df = pd.DataFrame.from_records(data_rows, columns=column_names)

    # Parse the version of the SPY components list.
    raw_publish_date = raw_contents_df.iat[1, 1]
    publish_date = datetime.datetime.strptime(
        raw_publish_date, "As of %d-%b-%Y").date()

    # I assume SPY holds cash, so we drop the corresponding row.
    cash_row = components_df[components_df["Ticker"] == "-"]
    components_df.drop(index=cash_row.index, inplace=True)

    components_df = components_df[["Ticker", "Name"]]
    sorted_components = sorted(
        components_df.itertuples(index=False), key=operator.itemgetter(0))
    logging.info(
        f"Fetched components list containing {len(sorted_components)} "
        "securities from the SSGA website.")
    for name, ticker in sorted_components:
        if not is_valid_entry(name) or not is_valid_entry(ticker):
            raise ValueError(f"SSGA SPY entry is not valid: [{name} {ticker}]")
    components_string = ",".join(
        [f"{ticker}:{name}" for name, ticker in sorted_components])
    return publish_date, components_string


def update_spy_bigquery_table(
        publish_date: datetime.date, components: str, bq_client: bigquery.Client):
    logging.info(
        f"Updating {_SPY_COMPONENTS_TABLE_ID} with a new components "
        f"list, with version {publish_date}.")
    job_config = bigquery.LoadJobConfig(
        schema=[
            bigquery.SchemaField("run_date", "DATE"),
            bigquery.SchemaField("publish_date", "DATE"),
            bigquery.SchemaField("components_list", "STRING"),
        ],
        # The table will be created if it does not exist (container dataset
        # was manually created at the time this code was initially written), and
        # rows are expected to be added at the end of the table (no overwriting).
        write_disposition="WRITE_APPEND",
    )
    bq_row = {
        "run_date": datetime.date.today().strftime("%Y-%m-%d"),
        "publish_date": publish_date.strftime("%Y-%m-%d"),
        # The Bigquery API does not accept datetime.date objects, so we need to
        # convert this to a string.
        "components_list": components,
    }
    job = bq_client.load_table_from_json(
        [bq_row], destination=_SPY_COMPONENTS_TABLE_ID, job_config=job_config)
    job.result()
