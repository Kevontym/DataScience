import pandas as pd
import os

class StructuredExtractor:
    @staticmethod
    def from_csv(file_path, **kwargs):
        """Extract data from CSV files"""
        try:
            df = pd.read_csv(file_path, **kwargs)
            print(f"✅ Loaded {len(df)} rows from {file_path}")
            return df
        except Exception as e:
            print(f"❌ Error loading CSV: {e}")
            return pd.DataFrame()
    
    @staticmethod
    def from_excel(file_path, sheet_name=0):
        """Extract data from Excel files"""
        try:
            df = pd.read_excel(file_path, sheet_name=sheet_name)
            print(f"✅ Loaded {len(df)} rows from {file_path}")
            return df
        except Exception as e:
            print(f"❌ Error loading Excel: {e}")
            return pd.DataFrame()