# Data Processing & ML Pipeline

A scalable data processing and machine learning pipeline for customer data analysis, built with PyTorch and containerized with Docker.

## 🚀 Features

- **Data Processing**: Extract, transform, and clean customer data
- **Machine Learning**: Anomaly detection and feature engineering
- **Smart Data Repair**: Automated data quality improvement
- **Containerized**: Fully Dockerized for easy deployment
- **Modular Architecture**: Extensible pipeline components

## 📁 Project Structure
DATA_CLEANER/
├── data/
│ ├── processed/ # Cleaned data files
│ └── raw/ # Raw input data
├── data_pipeline/ # ETL pipeline components
│ ├── extractors/ # Data extraction modules
│ ├── transformers/ # Data transformation logic
│ ├── loaders/ # Data loading modules
│ └── utils/ # Utility functions
├── ml_pipeline/ # Machine learning components
│ ├── anomaly_detector_ml.py
│ ├── feature_engineer.py
│ └── smart_repair.py
├── Dockerfile # Container configuration
├── docker-compose.yml # Multi-service setup
├── requirements.txt # Python dependencies
├── main.py # Main application entry point
├── run.py # Alternative runner script
└── run-docker.sh # Docker execution script




## 🛠️ Quick Start

### Prerequisites

- Docker and Docker Compose
- Python 3.8+ (for local development)

### Using Docker (Recommended)

1. **Clone the repository**
## Quick Start
# DataClean Pro - NLP Data Cleaning & SQL Analysis

> **Note**: This project is part of a larger DataScience repository. 
> You only need the `Data_cleaner/` folder to run this tool.

## 🚀 Quick Start (Just This Project)

### Option 1: Download & Run (Easiest):
1. **Download only the `Data_cleaner` folder** from this repository
2. Navigate to the folder: `cd Data_cleaner`  
3. Run: `python run.py`

### Option 2: Git Sparse Checkout:
```bash
mkdir my-cleaner && cd my-cleaner
git clone --filter=blob:none --sparse https://github.com/Kevontym/DataScience
cd DataScience
git sparse-checkout set Data_cleaner
cd Data_cleaner
python run.py

# 📝 License
This project is licensed under the MIT License - see the LICENSE file for details.