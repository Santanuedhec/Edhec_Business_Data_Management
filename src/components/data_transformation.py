import pandas as pd
import logging

# Import the exchange-rate logic from your separate module
from components.exchange_rate import fetch_exchange_rates, convert_price_to_eur

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)

def transform_data(df_csv: pd.DataFrame, df_gcp: pd.DataFrame) -> pd.DataFrame:
    
    try:
        logger.info("Starting data transformation...")

        
        # 1) Truncate reference_code in df_gcp
        df_gcp["reference_code"] = df_gcp["reference_code"].astype(str).str[1:6]
        logger.info("Trimmed reference_code in df_gcp to positions [1:6].")

        
        # 2) Group df_csv by ['category', 'reference_code']
        df_grouped = df_csv.groupby(["category", "reference_code"]).size().reset_index(name="count")
        logger.info("Grouped df_csv by ['category', 'reference_code'].")

        
        # 3) Convert reference_code to string if needed, define get_category, apply to df_gcp
        df_gcp["reference_code"] = df_gcp["reference_code"].astype(str)
        df_grouped["reference_code"] = df_grouped["reference_code"].astype(str)

        def get_category(ref_code):
            match = df_grouped[df_grouped["reference_code"] == ref_code]["category"]
            return match.iloc[0] if not match.empty else "Unknown"

        # The user’s steps mention applying this to df_cgcp, presumably df_gcp is intended
        df_gcp["category"] = df_gcp["reference_code"].apply(get_category)
        logger.info("Applied get_category() to df_gcp, added 'category' column.")

    
        # 4) Concatenate df_csv and df_gcp while keeping only selected columns
        selected_columns = [
            "brand",
            "collection",
            "reference_code",
            "price",
            "currency",
            "country",
            "category"
        ]

        # Ensure all selected columns exist in df_csv & df_gcp to avoid errors
        for col in selected_columns:
            if col not in df_csv.columns:
                df_csv[col] = None
            if col not in df_gcp.columns:
                df_gcp[col] = None

        df_combined = pd.concat(
            [df_gcp[selected_columns], df_csv[selected_columns]],
            ignore_index=True
        )
        logger.info("Concatenated df_gcp and df_csv into df_combined.")

        
        # 5) Map currency -> country
        currency_to_country = {
            "CNY": "China",
            "EUR": "Europe",
            "JPY": "Japan",
            "AED": "United Arab Emirates",
            "KRW": "South Korea",
            "HKD": "Hong Kong",
            "GBP": "United Kingdom",
            "SGD": "Singapore",
            "TWD": "Taiwan"
        }
        df_combined["country"] = df_combined["currency"].map(currency_to_country)
        logger.info("Updated 'country' column based on 'currency' using map().")

        
        # 6) Drop rows with missing values
        df_combined_2 = df_combined.dropna()
        logger.info("Dropped rows with missing values -> df_combined_2.")

    
        # 7) Fetch exchange rates and convert to EUR
        exchange_rates = fetch_exchange_rates()
        if exchange_rates is not None:
            df_combined_2["price_Eur"] = df_combined_2.apply(
                lambda row: convert_price_to_eur(row["price"], row["currency"], exchange_rates),
                axis=1
            )
            logger.info("Converted price to EUR and stored in 'price_Eur' column.")
        else:
            logger.warning("Failed to fetch exchange rates; price conversion skipped.")

        
        # 8) Reset index
        df_combined_2.reset_index(drop=True, inplace=True)
        logger.info("Reset index in final DataFrame -> df_combined_2.")

        logger.info("Data transformation completed successfully.")
        return df_combined_2

    except Exception as e:
        logger.error(f"Error in data transformation: {e}", exc_info=True)
        return pd.DataFrame()  # Return empty DataFrame on error


if __name__ == "__main__":
    # Optional test code
    from components.data_ingestion import load_csv_data, load_bigquery_data

    print("Testing data transformation...")

    # Load sample CSV and BigQuery data
    df_csv_test = load_csv_data()
    df_gcp_test = load_bigquery_data()

    if df_csv_test is not None and not df_csv_test.empty and df_gcp_test is not None and not df_gcp_test.empty:
        print(f"Loaded sample CSV data: {df_csv_test.shape}")
        print(f"Loaded sample GCP data: {df_gcp_test.shape}")

        # Transform
        df_transformed = transform_data(df_csv_test, df_gcp_test)

        if not df_transformed.empty:
            print(f"Data transformation successful. Shape: {df_transformed.shape}")
            print(df_transformed.head())
        else:
            print("Data transformation resulted in an empty DataFrame.")
    else:
        print("Could not load valid CSV/GCP data for transformation.")
