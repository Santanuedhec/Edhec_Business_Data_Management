import pandas as pd
from exchange_rate import fetch_exchange_rates, convert_price_to_eur

def transform_data(df):
    """
    Cleans and transforms the input DataFrame based on business logic.

    :param df: Pandas DataFrame (raw input)
    :return: Transformed Pandas DataFrame
    """
    try:
        # Remove duplicates
        df = df.drop_duplicates()

        # Fill NaN values to avoid NULL issues in BigQuery
        df = df.fillna("Unknown")  # Replace missing values instead of dropping

        # Convert column names to lowercase and replace spaces with underscores
        df.columns = df.columns.str.lower().str.replace(" ", "_")

        # Ensure 'reference_code' column exists and transform it
        if 'reference_code' in df.columns:
            df['reference_code'] = df['reference_code'].astype(str).str[1:6]

        # Ensure 'category' and 'reference_code' exist before grouping
        if 'category' in df.columns and 'reference_code' in df.columns:
            df_grouped = df.groupby(['category', 'reference_code']).size().reset_index(name='count')
        else:
            df_grouped = df  # If columns are missing, return as-is

        # Define function to fetch category based on reference_code
        def get_category(ref_code):
            match = df_grouped[df_grouped['reference_code'] == ref_code]['category']
            return match.iloc[0] if not match.empty else 'Unknown'  # Default value if not found

        # Apply function to create a new category column
        if 'reference_code' in df.columns:
            df['category'] = df['reference_code'].apply(get_category)

        # Fetch latest exchange rates
        exchange_rates = fetch_exchange_rates()

        # Convert price to EUR if applicable
        if exchange_rates and 'currency' in df.columns and 'price' in df.columns:
            df['price_eur'] = df.apply(lambda row: convert_price_to_eur(row['price'], row['currency'], exchange_rates), axis=1)

        print(" Data transformation completed successfully.")
        return df

    except Exception as e:
        print(f" Error in data transformation: {e}")
        return None

