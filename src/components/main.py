import logging
import sys

from components.data_ingestion import load_csv_data, load_bigquery_data
from components.data_transformation import transform_data
from components.upload_to_bigquery import upload_to_bigquery

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)

def main():
    """Orchestrates the data ingestion, transformation, and optional upload steps."""
    try:
        logger.info("Starting main pipeline...")

        # 1) Load CSV Data
        df_csv = load_csv_data()
        if df_csv is None or df_csv.empty:
            logger.warning("CSV data is empty or failed to load.")
        else:
            logger.info(f"CSV data loaded successfully. Shape: {df_csv.shape}")
        
        # 2) Load BigQuery Data
        df_gcp = load_bigquery_data()
        if df_gcp is None or df_gcp.empty:
            logger.warning("BigQuery data is empty or failed to load.")
        else:
            logger.info(f"BigQuery data loaded successfully. Shape: {df_gcp.shape}")
        
        # If either DataFrame is empty, there's no point proceeding
        if df_csv is None or df_csv.empty or df_gcp is None or df_gcp.empty:
            logger.error("Cannot proceed with transformation; one or both data sources are empty.")
            sys.exit(1)  # Exit the script

        # 3) Transform Data
        logger.info("Starting data transformation...")
        df_transformed = transform_data(df_csv, df_gcp)
        if df_transformed.empty:
            logger.error("Data transformation resulted in an empty DataFrame.")
            sys.exit(1)
        else:
            logger.info(f"Data transformed successfully. Shape: {df_transformed.shape}")
            logger.info(f"Sample rows:\n{df_transformed.head()}")

        # 4) Upload to BigQuery (Optional)
        
        logger.info("Uploading transformed data to BigQuery...")
        upload_to_bigquery(df_transformed)
        logger.info("Upload step completed.")

        logger.info("Main pipeline completed successfully.")

    except Exception as e:
        logger.exception(f"An error occurred in the main pipeline: {e}")
        sys.exit(1)  # Non-zero exit code for errors

if __name__ == "__main__":
    main()

