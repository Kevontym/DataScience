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
   ```bash
   git clone <your-repo-url>
   cd DATA_CLEANER

    # Or
   ./run-docker.sh


Set up Python environment

bash
python -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate
Install dependencies

bash
pip install -r requirements.txt
Run the pipeline

bash
python main.py
# or
python run.py



Data Format
The pipeline processes customer data files with the naming convention:

cleaned_customer_data_pytorch_YYYYMM.csv

Processed data is stored in the data/processed/ directory with timestamps.

🔧 Configuration
Environment Variables
Create a .env file for configuration:

env
DATA_PATH=/app/data
DB_PATH=/app/report_gen.db
LOG_LEVEL=INFO
Docker Configuration
Modify docker-compose.yml for:

Resource limits

Volume mounts

Network settings

🧪 Testing
Generate sample data for testing:

bash
python create_sample_data.py
📈 ML Pipeline Components
Anomaly Detection
Identifies unusual patterns in customer data

# Configurable detection thresholds

###Feature Engineering
###Automated feature creation and selection

# Support for temporal features

Smart Repair
Automated data correction

Missing value imputation

Data validation rules

# 🗃️ Database
The application uses SQLite (report_gen.db) for storing:

Processing metadata

Pipeline execution logs

Analysis reports

# 🚢 Deployment
Production Deployment
Build the image

bash
docker build -t data-cleaner:latest .
Run with production settings

bash
docker-compose -f docker-compose.prod.yml up -d
Kubernetes (Optional)
See the k8s/ directory for Kubernetes manifests.

# 🤝 Contributing
Fork the repository

Create a feature branch (git checkout -b feature/amazing-feature)

Commit your changes (git commit -m 'Add amazing feature')

Push to the branch (git push origin feature/amazing-feature)

Open a Pull Request

# 📝 License
This project is licensed under the MIT License - see the LICENSE file for details.