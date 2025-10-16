from abc import ABC, abstractmethod
import pandas as pd
import logging
from datetime import datetime

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class BaseDataPipeline(ABC):
    def __init__(self):
        self.raw_data = None
        self.cleaned_data = None
        
    @abstractmethod
    def extract(self, source):
        """Extract data from source"""
        pass
    
    @abstractmethod
    def transform(self):
        """Clean and transform data"""
        pass
    
    @abstractmethod
    def load(self, destination):
        """Load data to destination"""
        pass
    
    def run_pipeline(self, source, destination):
        """Execute full ETL process"""
        logger.info(f"Starting pipeline for {source}")
        
        # Extract
        self.extract(source)
        logger.info(f"Extracted {len(self.raw_data) if hasattr(self.raw_data, '__len__') else 'unknown'} records")
        
        # Transform
        self.transform()
        logger.info("Data transformation completed")
        
        # Load
        self.load(destination)
        logger.info("Data loaded successfully")