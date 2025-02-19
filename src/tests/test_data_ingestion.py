import unittest
from components.data_ingestion import load_csv_data, load_bigquery_data

class TestDataIngestion(unittest.TestCase):
    def test_load_csv_data(self):
        """Test if CSV data loads properly"""
        df = load_csv_data()
        self.assertIsNotNone(df, "CSV data should not be None")
        self.assertFalse(df.empty, "CSV DataFrame should not be empty")
        print("CSV Data Loaded Successfully!")
        print(df.head())

    def test_load_bigquery_data(self):
        """Test if BigQuery data loads properly"""
        df = load_bigquery_data()
        self.assertIsNotNone(df, "BigQuery data should not be None")
        self.assertFalse(df.empty, "BigQuery DataFrame should not be empty")
        print("BigQuery Data Loaded Successfully!")
        print(df.head())

if __name__ == '__main__':
    unittest.main()
