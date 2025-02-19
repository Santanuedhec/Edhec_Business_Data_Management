import os
import pandas as pd
import logging
from google.cloud import bigquery
from google.cloud.exceptions import NotFound
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Setup Logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Fetch environment variables
GCP_PROJECT_ID = os.getenv("GCP_PROJECT_ID", "biqquery-demo-451221")
BIGQUERY_DATASET = os.getenv("BIGQUERY_DATASET", "groupprojectdata")
BIGQUERY_TABLE = os.getenv("BIGQUERY_TABLE", "your_table_name")

# Ensure Google Cloud Credentials are set
credentials_path = os.getenv("GOOGLE_APPLICATION_CREDENTIALS")
if credentials_path:
    os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = credentials_path

# Initialize BigQuery Client
client = bigquery.Client()

def create_dataset_if_not_exists(dataset_id):
    """
    Checks if a BigQuery dataset exists; if not, creates it.
    """
    try:
        client.get_dataset(f"{GCP_PROJECT_ID}.{dataset_id}")  # Check if dataset exists
        logger.info(f"Dataset '{dataset_id}' already exists.")
    except NotFound:
        logger.info(f"Dataset '{dataset_id}' not found. Creating it now...")
        dataset = bigquery.Dataset(f"{GCP_PROJECT_ID}.{dataset_id}")
        dataset.location = "US"  # Modify location if needed
        client.create_dataset(dataset, exists_ok=True)
        logger.info(f"Dataset '{dataset_id}' created successfully.")

def upload_to_bigquery(df, table_name=BIGQUERY_TABLE):
    """
    Uploads a Pandas DataFrame to BigQuery, creating the dataset if necessary.

    Parameters:
    - df: Pandas DataFrame to upload
    - table_name: Name of the table in BigQuery
    """
    table_full_name = f"{GCP_PROJECT_ID}.{BIGQUERY_DATASET}.{table_name}"

    # Validate DataFrame
    if df is None or df.empty:
        logger.warning("No data to upload. DataFrame is empty.")
        return

    # Step 1: Ensure Dataset Exists
    create_dataset_if_not_exists(BIGQUERY_DATASET)

    # Step 2: Define Job Configuration
    job_config = bigquery.LoadJobConfig(
        write_disposition="WRITE_TRUNCATE",  # Replace table if exists (use "WRITE_APPEND" to append)
        autodetect=True  # Automatically detects schema from DataFrame
    )

    # Step 3: Upload DataFrame to BigQuery
    try:
        job = client.load_table_from_dataframe(df, table_full_name, job_config=job_config)
        job.result()  # Wait for the job to complete
        logger.info(f"Data successfully uploaded to {table_full_name}")
    except Exception as e:
        logger.error(f"Error uploading data to BigQuery: {e}")
