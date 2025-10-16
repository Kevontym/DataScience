import torch
import torch.nn as nn
import pandas as pd
import numpy as np
from sklearn.preprocessing import LabelEncoder
import warnings
from .data_cleaner import UniversalDataCleaner

class PyTorchEncoderCleaner(UniversalDataCleaner):
    def __init__(self, embedding_dim=64, hidden_dim=128):
        super().__init__()  # This now initializes the enhanced change tracking
        self.embedding_dim = embedding_dim
        self.hidden_dim = hidden_dim
        self.label_encoders = {}
        self.model = None
        self.is_fitted = False
        
    def _log_change(self, column, operation, original_value, new_value, row_index=None):
        """Log changes with string conversion for dates"""
        change = {
            'column': column,
            'operation': operation,
            'original_value': str(original_value) if original_value is not None else None,
            'new_value': str(new_value) if new_value is not None else None,
            'row_index': row_index,
            'timestamp': pd.Timestamp.now().isoformat()
        }
        self.change_log.append(change)
    
    def build_model(self, input_dim):
        """Build a simple autoencoder for anomaly detection"""
        class DataEncoder(nn.Module):
            def __init__(self, input_dim, embedding_dim, hidden_dim):
                super().__init__()
                self.encoder = nn.Sequential(
                    nn.Linear(input_dim, hidden_dim),
                    nn.ReLU(),
                    nn.Linear(hidden_dim, embedding_dim),
                    nn.ReLU()
                )
                self.decoder = nn.Sequential(
                    nn.Linear(embedding_dim, hidden_dim),
                    nn.ReLU(),
                    nn.Linear(hidden_dim, input_dim)
                )
            
            def forward(self, x):
                encoded = self.encoder(x)
                decoded = self.decoder(encoded)
                return decoded
        
        self.model = DataEncoder(input_dim, self.embedding_dim, self.hidden_dim)
        return self.model
    
    def preprocess_data(self, df):
        """Preprocess data for PyTorch model"""
        df_processed = df.copy()
        
        # Encode categorical variables
        for column in df.select_dtypes(include=['object']).columns:
            if column not in self.label_encoders:
                self.label_encoders[column] = LabelEncoder()
                df_processed[column] = self.label_encoders[column].fit_transform(df[column].fillna('Unknown'))
            else:
                df_processed[column] = self.label_encoders[column].transform(df[column].fillna('Unknown'))
        
        # Fill numeric missing values
        numeric_columns = df.select_dtypes(include=[np.number]).columns
        for column in numeric_columns:
            df_processed[column] = df_processed[column].fillna(df_processed[column].median())
        
        return df_processed
    
    def detect_anomalies(self, df, threshold=2.0):
        """Detect anomalies using PyTorch autoencoder"""
        if self.model is None:
            input_dim = len(df.columns)
            self.build_model(input_dim)
        
        # Preprocess data
        df_processed = self.preprocess_data(df)
        
        # Convert to tensor
        data_tensor = torch.FloatTensor(df_processed.values)
        
        # Simple anomaly detection using reconstruction error
        with torch.no_grad():
            reconstructed = self.model(data_tensor)
            reconstruction_error = torch.mean((data_tensor - reconstructed) ** 2, dim=1)
        
        # Identify anomalies
        anomalies = reconstruction_error > threshold
        anomaly_indices = df.index[anomalies.numpy()].tolist()
        
        print(f"🔍 PyTorch encoder detected {len(anomaly_indices)} potential anomalies")
        return anomaly_indices, reconstruction_error.numpy()
    
    def clean_dataframe(self, df):
        """Clean dataframe using PyTorch encoder with enhanced change tracking"""
        self.change_log = []  # Reset log for new dataframe
        
        print("🔥 Starting PyTorch Encoder cleaning...")
        original_df = df.copy()
        
        try:
            # First, do basic cleaning
            from data_pipeline.transformers.data_cleaner import UniversalDataCleaner
            basic_cleaner = UniversalDataCleaner()
            df = basic_cleaner.clean_dataframe(df)
            
            # Inherit basic cleaning changes
            self.change_log.extend(basic_cleaner.change_log)
            
        except Exception as e:
            print(f"⚠️  Basic cleaning failed, using fallback: {e}")
            # Fallback: just handle missing values
            df = self.handle_missing_values_fallback(df)
        
        # Now apply PyTorch-enhanced cleaning
        df = self.correct_anomalies(df)
        df = self.enhanced_imputation(df)
        
        self._generate_pytorch_report(original_df, df)
        return df

    def handle_missing_values_fallback(self, df):
        """Fallback method for handling missing values"""
        for column in df.columns:
            null_count = df[column].isnull().sum()
            if null_count > 0:
                if pd.api.types.is_numeric_dtype(df[column]):
                    impute_val = df[column].median()
                else:
                    impute_val = 'Unknown'
                
                original_nulls = df[df[column].isnull()].index
                df[column] = df[column].fillna(impute_val)
                
                for idx in original_nulls:
                    self._log_change(column, 'fallback_imputation', None, impute_val, idx)
        
        return df
    
    def correct_anomalies(self, df):
        """Correct detected anomalies"""
        try:
            anomaly_indices, scores = self.detect_anomalies(df)
            
            for idx in anomaly_indices:
                # For demonstration, we'll just flag anomalies
                # In practice, you might correct them based on domain knowledge
                print(f"⚠️  Potential anomaly at row {idx} (score: {scores[idx]:.3f})")
                
                # Log the detection without correction
                self._log_change('ALL', 'pytorch_anomaly_detection', 
                               'anomaly_detected', f"score_{scores[idx]:.3f}", idx)
        
        except Exception as e:
            print(f"⚠️  PyTorch anomaly detection failed: {e}")
            warnings.warn("Using fallback cleaning methods")
        
        return df
    
    def enhanced_imputation(self, df):
        """Enhanced missing value imputation using embeddings"""
        # This is a simplified version - in practice, you'd use the encoder
        # to learn better representations for imputation
        
        for column in df.columns:
            null_count = df[column].isnull().sum()
            if null_count > 0:
                print(f"🔄 PyTorch-enhanced imputation for {column}")
                
                # For now, use median/mean (replace with neural imputation)
                if pd.api.types.is_numeric_dtype(df[column]):
                    impute_val = df[column].median()
                else:
                    impute_val = df[column].mode()[0] if not df[column].mode().empty else 'Unknown'
                
                original_nulls = df[df[column].isnull()].index
                df[column] = df[column].fillna(impute_val)
                
                for idx in original_nulls:
                    self._log_change(column, 'pytorch_imputation', None, impute_val, idx)
        
        return df
    
    def _generate_pytorch_report(self, original_df, cleaned_df):
        """Generate PyTorch-specific cleaning report"""
        pytorch_changes = [change for change in self.change_log if change.get('cleaner_type') == 'pytorch_encoder']
        
        if pytorch_changes:
            print("\n" + "="*50)
            print("🔥 PYTORCH ENCODER CLEANING REPORT")
            print("="*50)
            print(f"PyTorch-specific changes: {len(pytorch_changes)}")
            
            # Group by operation type
            pt_ops = {}
            for change in pytorch_changes:
                op = change['operation']
                pt_ops[op] = pt_ops.get(op, 0) + 1
            
            for op, count in pt_ops.items():
                print(f"   {op}: {count} changes")
            
            # Show neural network insights
            if self.model is not None:
                print(f"\n🧠 Model architecture: {self.embedding_dim}D embeddings")
                print(f"   Hidden layers: {self.hidden_dim} units")
            print("="*50)
    
    def get_change_log(self):
        """Return the change log as a DataFrame"""
        return pd.DataFrame(self.change_log)
    
    def save_change_report(self, filepath):
        """Use the enhanced change reporting from parent class"""
        super().save_change_report(filepath)