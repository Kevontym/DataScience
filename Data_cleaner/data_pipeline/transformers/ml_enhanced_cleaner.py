import sys
import os

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
import pandas as pd

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
        
        # Traditional cleaning first
        df = super().clean_dataframe(df)
        
        # ML-enhanced cleaning
        df = self.ml_outlier_detection(df)
        df = self.ml_pattern_correction(df)
        
        # Generate ML-specific report
        self._generate_ml_report(original_df, df)
        
        return df
    
    def ml_outlier_detection(self, df):
        """Detect outliers using ML"""
        numeric_columns = df.select_dtypes(include=['number']).columns
        
        for column in numeric_columns:
            # Simple IQR-based outlier detection (replace with your ML model)
            Q1 = df[column].quantile(0.25)
            Q3 = df[column].quantile(0.75)
            IQR = Q3 - Q1
            lower_bound = Q1 - 1.5 * IQR
            upper_bound = Q3 + 1.5 * IQR
            
            outliers = df[(df[column] < lower_bound) | (df[column] > upper_bound)]
            
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
    
    def _generate_ml_report(self, original_df, cleaned_df):
        """Generate ML-specific cleaning report"""
        ml_changes = [change for change in self.change_log if 'ml_' in change['operation']]
        
        if ml_changes:
            print("\n" + "="*50)
            print("🤖 ML-ENHANCED CLEANING REPORT")
            print("="*50)
            print(f"ML-specific changes: {len(ml_changes)}")
            
            # Group by operation type
            ml_ops = {}
            for change in ml_changes:
                op = change['operation']
                ml_ops[op] = ml_ops.get(op, 0) + 1
            
            for op, count in ml_ops.items():
                print(f"   {op}: {count} changes")
            print("="*50)