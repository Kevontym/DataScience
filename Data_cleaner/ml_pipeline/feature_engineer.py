import pandas as pd
import numpy as np
import re

class MLFeatureEngineer:
    """Simplified feature engineer for pandas DataFrames"""
    
    def create_anomaly_features(self, df, text_columns, numeric_columns):
        """Create features for anomaly detection"""
        df_enhanced = df.copy()
        
        for col in text_columns:
            if col in df.columns:
                # Basic text features
                df_enhanced[f'{col}_length'] = df[col].astype(str).apply(len)
                df_enhanced[f'{col}_word_count'] = df[col].astype(str).apply(lambda x: len(x.split()))
                df_enhanced[f'{col}_special_ratio'] = df[col].astype(str).apply(
                    lambda x: len(re.findall(r'[^\w\s]', x)) / max(len(x), 1)
                )
                
                # SKU anomaly pattern
                df_enhanced[f'{col}_has_sku_anomaly'] = df[col].astype(str).apply(
                    lambda x: 1 if re.search(r'_[a-z]+\d+$', x) else 0
                )
                
                # Repeated characters pattern
                df_enhanced[f'{col}_has_repeated_chars'] = df[col].astype(str).apply(
                    lambda x: 1 if re.search(r'(.)\1{3,}', x) else 0
                )
        
        for col in numeric_columns:
            if col in df.columns:
                # Z-score for outliers
                mean_val = df[col].mean()
                std_val = df[col].std()
                df_enhanced[f'{col}_zscore'] = (df[col] - mean_val) / (std_val + 1e-8)
        
        return df_enhanced