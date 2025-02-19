import logging
import pandas as pd
from data_ingestion import load_csv_data, load_bigquery_data
from data_transformation import transform_data
from upload_to_bigquery import upload_to_bigquery

# Setup Logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def main():
    """
    Main function to execute the data pipeline:
    1. Load data from CSV & BigQuery
    2. Transform data
    3. Upload to BigQuery
    """
    logger.info("Starting the data pipeline...")

    # Step 1: Load data from CSV
    df_csv = load_csv_data()
    if df_csv is not None and not df_csv.empty:
        logger.info(f"CSV data loaded successfully with {len(df_csv)} records.")
    else:
        logger.warning("No valid data found in CSV file.")

    # Step 2: Load data from BigQuery
    df_bigquery = load_bigquery_data()
    if df_bigquery is not None and not df_bigquery.empty:
        logger.info(f"BigQuery data loaded successfully with {len(df_bigquery)} records.")
    else:
        logger.warning("No valid data found in BigQuery.")

    # Step 3: Merge both datasets (if applicable)
    if df_csv is not None and not df_csv.empty and df_bigquery is not None and not df_bigquery.empty:
        df_combined = pd.concat([df_csv, df_bigquery], ignore_index=True)
        logger.info(f"Merged dataset contains {len(df_combined)} records.")
    elif df_csv is not None and not df_csv.empty:
        df_combined = df_csv
        logger.info("Using only CSV data for transformation.")
    elif df_bigquery is not None and not df_bigquery.empty:
        df_combined = df_bigquery
        logger.info("Using only BigQuery data for transformation.")
    else:
        logger.error("No valid data available for transformation. Exiting pipeline.")
        return

    # Step 4: Transform data
    df_transformed = transform_data(df_combined)
    if df_transformed is not None and not df_transformed.empty:
        logger.info(f"Data transformation successful. Transformed dataset contains {len(df_transformed)} records.")
    else:
        logger.error("Data transformation failed. Exiting pipeline.")
        return

    # Step 5: Upload transformed data to BigQuery
    upload_to_bigquery(df_transformed)

    logger.info("Data pipeline executed successfully.")

if __name__ == "__main__":
    main()
