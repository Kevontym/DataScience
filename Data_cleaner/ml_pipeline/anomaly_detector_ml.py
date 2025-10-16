from sklearn.ensemble import IsolationForest
from sklearn.cluster import KMeans
from sklearn.preprocessing import StandardScaler
import pandas as pd
import numpy as np

class MLAnomalyDetector:
    """Simplified ML anomaly detector using scikit-learn"""
    
    def detect_anomalies_unsupervised(self, df, feature_columns, method='isolation_forest'):
        """Detect anomalies using ML"""
        
        # Select only the feature columns
        features = df[feature_columns].fillna(0)
        
        if method == 'isolation_forest':
            detector = IsolationForest(contamination=0.1, random_state=42)
            anomalies = detector.fit_predict(features)
            scores = detector.decision_function(features)
            
        elif method == 'kmeans':
            scaler = StandardScaler()
            scaled_features = scaler.fit_transform(features)
            
            kmeans = KMeans(n_clusters=5, random_state=42)
            clusters = kmeans.fit_predict(scaled_features)
            
            # Distance to cluster center as anomaly score
            distances = np.min(kmeans.transform(scaled_features), axis=1)
            scores = -distances  # Higher distance = more anomalous
            anomalies = (scores > np.percentile(scores, 90)).astype(int)
        
        # Add results to dataframe
        df_result = df.copy()
        df_result['anomaly_score'] = scores
        df_result['is_anomaly'] = anomalies
        
        return df_result