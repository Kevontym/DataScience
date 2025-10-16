import pandas as pd
import numpy as np
import re
from sklearn.impute import SimpleImputer

class MLDataRepair:
    def __init__(self):
        self.repair_log = []
    
    def smart_repair_anomalies(self, df, anomaly_scores_column, repair_strategy='auto'):
        """Intelligently repair detected anomalies"""
        
        # Identify anomalies (top 5% highest scores)
        threshold = df[anomaly_scores_column].quantile(0.95)
        anomalies_mask = df[anomaly_scores_column] >= threshold
        
        print(f"🔧 Repairing {anomalies_mask.sum()} anomalies...")
        
        if repair_strategy == 'auto':
            repaired_df = self._auto_repair_strategy(df, anomalies_mask)
        else:
            repaired_df = df.copy()
        
        self._print_repair_report()
        return repaired_df
    
    def _auto_repair_strategy(self, df, anomalies_mask):
        """Automated repair based on data patterns"""
        repaired_df = df.copy()
        
        # For each column, apply appropriate repair to anomalous rows only
        for column in df.columns:
            if column in ['anomaly_score', 'is_anomaly', 'ensemble_anomaly_score']:
                continue
                
            # Get only anomalous rows for this column
            anomalous_rows = anomalies_mask & (~df[column].isna())
            
            if df[column].dtype == 'object':  # Text columns
                repaired_df.loc[anomalous_rows, column] = self._repair_text_column(
                    df.loc[anomalous_rows, column], column
                )
            elif np.issubdtype(df[column].dtype, np.number):  # Numeric columns
                repaired_df.loc[anomalous_rows, column] = self._repair_numeric_column(
                    df.loc[anomalous_rows, column], column, df[column]
                )
        
        return repaired_df
    
    def _repair_text_column(self, series, column_name):
        """Repair anomalous text data"""
        repaired_series = series.copy()
        
        # For SKU-like patterns, remove suspicious suffixes
        if any(pattern in column_name.lower() for pattern in ['sku', 'id', 'code']):
            repaired_series = repaired_series.astype(str).apply(
                lambda x: re.sub(r'_[a-z]+\d+$', '', x) if re.search(r'_[a-z]+\d+$', x) else x
            )
            self.repair_log.append(f"Removed SKU suffixes from {column_name}")
        
        # Remove repeated characters (aaaa → aa)
        repaired_series = repaired_series.astype(str).apply(
            lambda x: re.sub(r'(.)\1{2,}', r'\1\1', x)
        )
        
        # Remove excessive special characters
        repaired_series = repaired_series.astype(str).apply(
            lambda x: re.sub(r'[^\w\s]', ' ', x) if len(re.findall(r'[^\w\s]', x)) / len(x) > 0.3 else x
        )
        
        # Trim extremely long text
        avg_length = series.astype(str).apply(len).mean()
        repaired_series = repaired_series.astype(str).apply(
            lambda x: x[:int(avg_length * 2)] if len(x) > avg_length * 3 else x
        )
        
        return repaired_series
    
    def _repair_numeric_column(self, anomalous_series, column_name, full_series):
        """Repair anomalous numeric data"""
        # Use median of non-anomalous data for repair
        median_value = full_series.median()
        return anomalous_series.apply(lambda x: median_value if abs(x - median_value) > 3 * full_series.std() else x)
    
    def _print_repair_report(self):
        """Print repair operations performed"""
        if self.repair_log:
            print("\n🔧 REPAIR OPERATIONS PERFORMED:")
            for log_entry in self.repair_log:
                print(f"   - {log_entry}")