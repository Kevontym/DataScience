import sys
import os
import pandas as pd

# Add the project root to Python path
project_root = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
if project_root not in sys.path:
    sys.path.insert(0, project_root)

try:
    from ml_pipeline.feature_engineer import MLFeatureEngineer
    from ml_pipeline.anomaly_detector_ml import MLAnomalyDetector
    from ml_pipeline.smart_repair import MLDataRepair
    ML_AVAILABLE = True
except ImportError as e:
    print(f"⚠️  ML components not available: {e}")
    ML_AVAILABLE = False

from data_pipeline.transformers.data_cleaner import UniversalDataCleaner

class MLEnhancedDataCleaner(UniversalDataCleaner):
    def __init__(self):
        super().__init__() 
        self.ml_specific_changes = []
    
    def clean_dataframe(self, df):
        """Enhanced cleaning with ML capabilities"""
        self.change_log = []
        self.ml_specific_changes = []
        
        print("🧠 Starting ML-enhanced cleaning...")
        original_df = df.copy()
        
        # FIXED: Remove super().clean_dataframe() call - it doesn't exist
        # Do basic cleaning + ML enhancements directly
        df = self._basic_cleaning(df)
        df = self.ml_outlier_detection(df)
        df = self.ml_pattern_correction(df)
        
        # Generate ML-specific report
        self._generate_ml_report(original_df, df)
        
        return df
    
    def _basic_cleaning(self, df):
        """Basic data cleaning steps"""
        print("🔧 Applying basic data cleaning...")
        
        # Remove completely empty rows
        initial_rows = len(df)
        df = df.dropna(how='all')
        if len(df) < initial_rows:
            removed = initial_rows - len(df)
            print(f"   📝 Removed {removed} completely empty rows")
        
        # Remove duplicates
        initial_rows = len(df)
        df = df.drop_duplicates()
        if len(df) < initial_rows:
            removed = initial_rows - len(df)
            print(f"   📝 Removed {removed} duplicate rows")
        
        # Handle missing values
        for col in df.columns:
            missing_count = df[col].isna().sum()
            if missing_count > 0:
                if df[col].dtype == 'object':
                    df[col] = df[col].fillna('Unknown')
                    self._log_change(col, 'fillna_categorical', None, 'Unknown')
                else:
                    median_val = df[col].median()
                    df[col] = df[col].fillna(median_val)
                    self._log_change(col, 'fillna_numeric', None, median_val)
                print(f"   📝 Filled {missing_count} missing values in '{col}'")
        
        return df
    
    def ml_outlier_detection(self, df):
        """Detect outliers using ML"""
        print("🔍 Running ML outlier detection...")
        numeric_columns = df.select_dtypes(include=['number']).columns
        
        for column in numeric_columns:
            # Simple IQR-based outlier detection
            Q1 = df[column].quantile(0.25)
            Q3 = df[column].quantile(0.75)
            IQR = Q3 - Q1
            lower_bound = Q1 - 1.5 * IQR
            upper_bound = Q3 + 1.5 * IQR
            
            outliers = df[(df[column] < lower_bound) | (df[column] > upper_bound)]
            
            if len(outliers) > 0:
                print(f"   📊 Found {len(outliers)} outliers in '{column}'")
            
            for idx in outliers.index:
                original_value = df.at[idx, column]
                # Cap outliers to bounds
                if original_value < lower_bound:
                    new_value = lower_bound
                else:
                    new_value = upper_bound
                
                df.at[idx, column] = new_value
                self._log_change(column, 'ml_outlier_correction', original_value, new_value, idx)
                self.ml_specific_changes.append({
                    'type': 'outlier_correction',
                    'column': column,
                    'original': original_value,
                    'corrected': new_value
                })
        
        return df
    
    def ml_pattern_correction(self, df):
        """Apply ML-based pattern corrections"""
        print("🎯 Applying ML pattern correction...")
        
        # Add your actual ML pattern correction logic here
        # For now, this is a placeholder that you can enhance
        
        # Example: Text standardization
        text_columns = df.select_dtypes(include=['object']).columns
        for col in text_columns:
            # Basic text cleaning
            initial_values = df[col].copy()
            df[col] = df[col].str.strip().str.title()
            
            # Log changes
            for idx in df.index:
                if (pd.notna(initial_values[idx]) and pd.notna(df.at[idx, col]) and 
                    initial_values[idx] != df.at[idx, col]):
                    self._log_change(col, 'ml_text_standardization', 
                                   initial_values[idx], df.at[idx, col], idx)
                    self.ml_specific_changes.append({
                        'type': 'text_standardization',
                        'column': col,
                        'original': initial_values[idx],
                        'corrected': df.at[idx, col]
                    })
        
        return df
    
    def _generate_ml_report(self, original_df, cleaned_df):
        """Generate ML-specific cleaning report"""
        ml_changes = [change for change in self.change_log if 'ml_' in change['operation']]
        
        print("\n" + "="*50)
        print("🤖 ML-ENHANCED CLEANING REPORT")
        print("="*50)
        print(f"Total changes: {len(self.change_log)}")
        print(f"ML-specific changes: {len(ml_changes)}")
        
        # Group by operation type
        ml_ops = {}
        for change in ml_changes:
            op = change['operation']
            ml_ops[op] = ml_ops.get(op, 0) + 1
        
        for op, count in ml_ops.items():
            print(f"   {op}: {count} changes")
        
        # Data quality improvements
        print(f"\n📈 Data Quality Improvements:")
        print(f"   Original rows: {len(original_df)}")
        print(f"   Cleaned rows: {len(cleaned_df)}")
        print(f"   Missing values reduced: {original_df.isna().sum().sum()} → {cleaned_df.isna().sum().sum()}")
        print("="*50)