import os
from pathlib import Path

# Base paths
PROJECT_ROOT = Path(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
DATA_DIR = PROJECT_ROOT / 'data'
RAW_DATA_DIR = DATA_DIR / 'raw'
PROCESSED_DATA_DIR = DATA_DIR / 'processed'
MODELS_DIR = PROJECT_ROOT / 'models'
LOGS_DIR = PROJECT_ROOT / 'logs'

# Data file paths
CUSTOMER_DATA_CSV = RAW_DATA_DIR / 'customer_data.csv'
REVIEWS_DIR = RAW_DATA_DIR / 'reviews'
OUTPUT_CSV = PROCESSED_DATA_DIR / 'cleaned_customer_data.csv'

# Create directories if they don't exist
for directory in [RAW_DATA_DIR, PROCESSED_DATA_DIR, REVIEWS_DIR, MODELS_DIR, LOGS_DIR]:
    directory.mkdir(parents=True, exist_ok=True)

# ML Settings
ML_CONFIG = {
    'anomaly_threshold': 0.95,
    'min_samples_for_ml': 100,
    'contamination': 0.1,
    'random_state': 42
}

# Cleaning Settings
CLEANING_CONFIG = {
    'max_text_length': 1000,
    'default_rating': -1,
    'unknown_value': 'Unknown',
    'date_format': '%Y-%m-%d'
}