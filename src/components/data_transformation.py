import pandas as pd
import logging
from components.exchange_rate import fetch_exchange_rates, convert_price_to_eur

# Configure Logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)

def transform_data(df):
    """
    Cleans and transforms the input DataFrame based on business logic.

    :param df: Pandas DataFrame (raw input)
    :return: Transformed Pandas DataFrame
    """
    try:
        logger.info(" Starting data transformation...")

        # Remove duplicates
        df = df.drop_duplicates()
        logger.info(" Removed duplicate rows.")

        # Fill missing values with 'Unknown' (EXCEPT for 'price')
        for col in df.columns:
            if col != "price":  # Avoid replacing 'price' with "Unknown"
                df[col].fillna("Unknown", inplace=True)

        # Ensure 'price' column exists and is numeric
        if "price" in df.columns:
            df["price"] = pd.to_numeric(df["price"], errors="coerce")  # Convert non-numeric values to NaN
            df["price"].fillna(0, inplace=True)  # Replace NaN with 0 (default)
            df["price"] = df["price"].astype(float)  # Ensure it's a float type
            logger.info(" Converted 'price' column to numeric format.")

        # Convert column names to lowercase and replace spaces with underscores
        df.columns = df.columns.str.lower().str.replace(" ", "_")
        logger.info(" Standardized column names.")

        # Ensure 'reference_code' column exists and transform it
        if "reference_code" in df.columns:
            df["reference_code"] = df["reference_code"].astype(str).str[1:6]
            logger.info(" Transformed 'reference_code'.")

        # Ensure 'category' and 'reference_code' exist before grouping
        if "category" in df.columns and "reference_code" in df.columns:
            df_grouped = df.groupby(["category", "reference_code"]).size().reset_index(name="count")
            logger.info(" Grouped data by 'category' and 'reference_code'.")
        else:
            df_grouped = df
            logger.warning(" 'category' or 'reference_code' column missing; skipping grouping.")

        # Define function to fetch category based on reference_code
        def get_category(ref_code):
            match = df_grouped[df_grouped["reference_code"] == ref_code]["category"]
            return match.iloc[0] if not match.empty else "Unknown"

        # Apply function to create a new category column
        if "reference_code" in df.columns:
            df["category"] = df["reference_code"].apply(get_category)
            logger.info(" Created new 'category' column.")

        # Fetch latest exchange rates
        exchange_rates = fetch_exchange_rates()
        if not exchange_rates:
            logger.warning(" Failed to fetch exchange rates.")

        # Convert price to EUR if applicable
        if exchange_rates and "currency" in df.columns and "price" in df.columns:
            df["price_eur"] = df.apply(
                lambda row: convert_price_to_eur(row["price"], row["currency"], exchange_rates), axis=1
            )
            logger.info(" Converted prices to EUR.")

        logger.info(" Data transformation completed successfully.")
        return df

    except Exception as e:
        logger.error(f" Error in data transformation: {e}", exc_info=True)
        return None

if __name__ == "__main__":
    import pandas as pd
    from data_ingestion import load_csv_data

    print("Testing data transformation...")

    # Load sample data from CSV for testing
    df = load_csv_data()
    if df is not None and not df.empty:
        print(f" Loaded sample CSV data for transformation. Shape: {df.shape}")
        print(df.head())

        # Transform the data
        df_transformed = transform_data(df)

        if df_transformed is not None and not df_transformed.empty:
            print(f" Data transformation successful! Shape: {df_transformed.shape}")
            print(df_transformed.head())  # Show sample transformed data
        else:
            print(" Data transformation failed. Output is empty.")
    else:
        print(" Could not load sample data for transformation.")
