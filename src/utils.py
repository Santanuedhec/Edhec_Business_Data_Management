import os
import pandas as pd
import logging
from google.cloud import bigquery
from google.cloud.exceptions import NotFound, BadRequest
from dotenv import load_dotenv


load_dotenv()

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

GCP_CREDENTIALS = os.getenv("GOOGLE_APPLICATION_CREDENTIALS")
PROJECT_ID = os.getenv("GCP_PROJECT_ID", "biqquery-demo-451221")
DATASET_ID = os.getenv("BIGQUERY_DATASET", "groupprojectdata")


if GCP_CREDENTIALS:
    os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = GCP_CREDENTIALS


client = bigquery.Client()

def fetch_bigquery_data(query):
    """
    Executes a SQL query and returns the results as a Pandas DataFrame.

    :param query: SQL query string
    :return: Pandas DataFrame or None if an error occurs
    """
    try:
        query_job = client.query(query)
        df = query_job.to_dataframe()
        return df
    except NotFound:
        logger.error("BigQuery table not found.")
    except BadRequest as e:
        logger.error(f"BigQuery BadRequest error: {e}")
    except Exception as e:
        logger.error(f"Error fetching data: {e}")
    return None

def upload_to_bigquery(df, table_name, write_disposition="WRITE_TRUNCATE"):
    """
    Uploads a Pandas DataFrame to BigQuery.

    Parameters:
    - df: Pandas DataFrame to upload
    - table_name: Name of the table (without dataset prefix)
    - write_disposition: "WRITE_TRUNCATE" to overwrite, "WRITE_APPEND" to add new rows
    """
    table_ref = f"{PROJECT_ID}.{DATASET_ID}.{table_name}"

    
    if df is None or df.empty:
        logger.warning("No data to upload. DataFrame is empty.")
        return

    job_config = bigquery.LoadJobConfig(
        write_disposition=write_disposition,
        autodetect=True 
    )

    try:
        job = client.load_table_from_dataframe(df, table_ref, job_config=job_config)
        job.result() 
        logger.info(f"Data successfully uploaded to {table_ref}")
    except Exception as e:
        logger.error(f"Error uploading data to BigQuery: {e}")
