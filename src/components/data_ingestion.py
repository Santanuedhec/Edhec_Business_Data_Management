import os
import pandas as pd
import logging
import traceback
from google.cloud import bigquery
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Setup Logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Fetch environment variables
CSV_FILE_PATH = os.getenv("CSV_FILE_PATH", r"C:\Users\Santanu Pal\Downloads\PM_extract_Jan_2025_10_brands.csv")
GCP_PROJECT_ID = os.getenv("GCP_PROJECT_ID", "biqquery-demo-451221")
BIGQUERY_DATASET = os.getenv("BIGQUERY_DATASET", "luxurydata2502")
BIGQUERY_TABLE = os.getenv("BIGQUERY_TABLE", "price-monitoring-2022")

# Ensure Google Cloud Credentials are set
credentials_path = os.getenv("GOOGLE_APPLICATION_CREDENTIALS")
if credentials_path:
    os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = credentials_path

# Initialize BigQuery Client
client = bigquery.Client()

def load_csv_data(file_path=CSV_FILE_PATH):
    """
    Reads a CSV file and returns a Pandas DataFrame.

    :param file_path: Path to the CSV file
    :return: Pandas DataFrame
    """
    try:
        # Read CSV with semicolon delimiter and skipping bad lines
        df = pd.read_csv(file_path, sep=';', on_bad_lines='skip')

        # Ensure 'brand' column exists before filtering
        if 'brand' in df.columns:
            df = df[df['brand'] == 'Chaumet']  # Filtering only 'Chaumet' brand

        logger.info(f"Successfully loaded and filtered CSV data from {file_path}")
        return df

    except Exception as e:
        logger.error(f"Error reading CSV file: {e}\n{traceback.format_exc()}")
        return None

def load_bigquery_data():
    """
    Fetches data from BigQuery table based on SQL query.
    
    :return: Pandas DataFrame
    """
    try:
        query = f"""
        SELECT * FROM `{GCP_PROJECT_ID}.{BIGQUERY_DATASET}.{BIGQUERY_TABLE}`
        WHERE brand = 'Chaumet'
        """
        
        query_job = client.query(query, location='EU')  # Execute query
        df = query_job.to_dataframe()  # Convert result to Pandas DataFrame

        logger.info("Successfully fetched data from BigQuery")
        return df

    except Exception as e:
        logger.error(f"Error fetching data from BigQuery: {e}\n{traceback.format_exc()}")
        return None
    
if __name__ == "__main__":
    print("Testing data ingestion...")

    # Test CSV Data Loading
    df_csv = load_csv_data()
    if df_csv is not None:
        print(f"CSV Data Loaded Successfully! Shape: {df_csv.shape}")
        print(df_csv.head())  # Print first few rows for verification
    else:
        print("Failed to load CSV data.")

    # Test BigQuery Data Loading
    df_bigquery = load_bigquery_data()
    if df_bigquery is not None:
        print(f"BigQuery Data Loaded Successfully! Shape: {df_bigquery.shape}")
        print(df_bigquery.head())  # Print first few rows for verification
    else:
        print("Failed to load BigQuery data.")

