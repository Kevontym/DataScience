FROM python:3.9-slim

# Set working directory
WORKDIR /app

# Install system dependencies
RUN apt-get update && apt-get install -y \
    sqlite3 \
    && rm -rf /var/lib/apt/lists/*

# Copy requirements first (for better caching)
COPY requirements.txt .

# Install Python dependencies
RUN pip install --no-cache-dir -r requirements.txt

# Copy project files
COPY . .

# Create necessary directories
RUN mkdir -p data/raw data/processed data_pipeline/utils

# Make run.py executable
RUN chmod +x run.py

# Set Python path
ENV PYTHONPATH=/app

# Default command
CMD ["python", "run.py", "--interactive"]