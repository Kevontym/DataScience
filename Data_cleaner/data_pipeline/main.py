from extractors.StructuredExtractor import StructuredExtractor
from extractors.UnstructuredExtractor import UnstructuredExtractor
from transformers.data_cleaner import UniversalDataCleaner
from transformers.ml_enhanced_cleaner import MLEnhancedDataCleaner
from sqlalchemy import create_engine
import pandas as pd
import os
import argparse
import sys

# data_pipeline/main.py
import sys
import os

# Set up paths - this ensures imports work regardless of how we're called
current_dir = os.path.dirname(os.path.abspath(__file__))
project_root = os.path.dirname(current_dir)

if project_root not in sys.path:
    sys.path.insert(0, project_root)
if current_dir not in sys.path:
    sys.path.insert(0, current_dir)

# Now imports should work consistently
from data_pipeline.extractors.StructuredExtractor import StructuredExtractor
from data_pipeline.extractors.UnstructuredExtractor import UnstructuredExtractor
from data_pipeline.transformers.data_cleaner import UniversalDataCleaner
from data_pipeline.transformers.ml_enhanced_cleaner import MLEnhancedDataCleaner
from sqlalchemy import create_engine
import pandas as pd

class CustomerFeedbackPipeline:
    def __init__(self, use_ml=False):
        # Choose cleaner based on use_ml parameter
        if use_ml:
            print("🧠 Initializing ML-Powered Cleaner...")
            self.cleaner = MLEnhancedDataCleaner()
        else:
            print("⚡ Initializing Traditional Cleaner...")
            self.cleaner = UniversalDataCleaner()
        
        self.all_data = pd.DataFrame()
    
    def add_structured_data(self, source_path, source_type='csv'):
        """Add structured data to the pipeline"""
        print(f"🔄 Adding structured data from {source_path}")
        
        if source_type == 'csv':
            data = StructuredExtractor.from_csv(source_path)
        elif source_type == 'excel':
            data = StructuredExtractor.from_excel(source_path)
        else:
            raise ValueError(f"Unsupported source type: {source_type}")
        
        if not data.empty:
            cleaned_data = self.cleaner.clean_dataframe(data)
            self.all_data = pd.concat([self.all_data, cleaned_data], ignore_index=True)
    
    def add_unstructured_data(self, source_path, source_type='text'):
        """Add unstructured data to the pipeline"""
        print(f"🔄 Adding unstructured data from {source_path}")
        
        if source_type == 'text':
            raw_data = UnstructuredExtractor.from_text_files(source_path)
        elif source_type == 'json':
            raw_data = UnstructuredExtractor.from_json(source_path)
        else:
            raise ValueError(f"Unsupported source type: {source_type}")
        
        if not raw_data.empty:
            # Transform unstructured data to match our structured schema
            transformed_data = self._transform_unstructured_to_schema(raw_data)
            cleaned_data = self.cleaner.clean_dataframe(transformed_data)
            self.all_data = pd.concat([self.all_data, cleaned_data], ignore_index=True)

    def _transform_unstructured_to_schema(self, unstructured_df):
        """Transform unstructured text data to match our customer feedback schema"""
        if unstructured_df.empty:
            return unstructured_df
        
        # Create a new DataFrame with our target schema
        structured_like_df = pd.DataFrame()
        
        # Map the content to review_text (the main field we care about)
        structured_like_df['review_text'] = unstructured_df['content']
        
        # Add metadata about the source
        structured_like_df['source_type'] = unstructured_df['source_type']
        structured_like_df['filename'] = unstructured_df['filename']
        
        # Add missing columns with default values to match our schema
        start_id = len(self.all_data) + 1 if not self.all_data.empty else 1
        structured_like_df['id'] = range(start_id, start_id + len(unstructured_df))
        structured_like_df['name'] = 'Unknown'
        structured_like_df['email'] = 'Unknown'
        structured_like_df['rating'] = None
        structured_like_df['date'] = None
        
        print(f"✅ Transformed {len(unstructured_df)} unstructured records to schema")
        return structured_like_df
    



    def ask_user_for_sql_interactive():
        """Ask user if they want to open SQL interactive terminal"""
        print("\n" + "="*50)
        print("🖥️  SQL INTERACTIVE TERMINAL")
        print("="*50)
        print("Would you like to open an interactive SQL terminal to explore your data?")
        print("This allows you to:")
        print("   • Run custom SQL queries on all pipeline runs")
        print("   • Analyze cleaning patterns across multiple executions")
        print("   • Export data for further analysis")
        print()
        
        while True:
            choice = input("\nOpen SQL terminal? (y/n) [n]: ").strip().lower() or 'n'
            if choice in ['y', 'yes', 'n', 'no']:
                return choice in ['y', 'yes']
            else:
                print("❌ Please enter 'y' or 'n'")

    def open_sql_interactive_terminal():
        """Open an interactive SQL terminal for the report database"""
        import subprocess
        import os
        
        db_path = "report_gen.db"
        
        if not os.path.exists(db_path):
            print(f"❌ Database file not found: {db_path}")
            print("   Run the pipeline with --sql-storage first to create the database")
            return
        
        print(f"\n🚀 Opening SQLite interactive terminal...")
        print(f"📁 Database: {db_path}")
        print("💡 Useful commands:")
        print("   .tables                         - Show all tables")
        print("   .schema pipeline_runs          - Show table structure")
        print("   .headers on                    - Show column headers")
        print("   .mode column                   - Better output formatting")
        print("   .exit                          - Exit SQLite")
        print()
        print("📊 Try these queries:")
        print("   SELECT * FROM pipeline_runs ORDER BY timestamp DESC LIMIT 5;")
        print("   SELECT cleaner_type, AVG(total_changes) as avg_changes FROM pipeline_runs GROUP BY cleaner_type;")
        print("   SELECT column_name, COUNT(*) as changes FROM changes GROUP BY column_name ORDER BY changes DESC LIMIT 10;")
        print()
        print("Press Ctrl+D to exit")
        
        try:
            # Launch interactive SQLite terminal
            subprocess.run(['sqlite3', db_path])
        except FileNotFoundError:
            print("❌ sqlite3 command not found. Please install SQLite:")
            print("   macOS: Already installed")
            print("   Linux: sudo apt-get install sqlite3")
            print("   Windows: Download from https://sqlite.org/download.html")
        except Exception as e:
            print(f"❌ Failed to open SQL terminal: {e}")



    def save_to_database(self, connection_string, table_name):
        """Save cleaned data to SQL database"""
        if self.all_data.empty:
            print("❌ No data to save!")
            return
        
        try:
            engine = create_engine(connection_string)
            self.all_data.to_sql(table_name, engine, if_exists='replace', index=False)
            print(f"✅ Successfully saved {len(self.all_data)} records to {table_name}")
        except Exception as e:
            print(f"❌ Error saving to database: {e}")
    
    def save_to_csv(self, file_path):
        """Save cleaned data to CSV"""
        if self.all_data.empty:
            print("❌ No data to save!")
            return
        
        # Create directory if it doesn't exist
        os.makedirs(os.path.dirname(file_path), exist_ok=True)
        self.all_data.to_csv(file_path, index=False)
        print(f"✅ Successfully saved {len(self.all_data)} records to {file_path}")

def ask_user_for_cleaner():
    """Interactive prompt for cleaner selection"""
    print("\n" + "="*50)
    print("🛠️  CHOOSE DATA CLEANER TYPE")
    print("="*50)
    print("1. 🤖 ML-Powered Cleaner (Recommended for large datasets)")
    print("   - Uses machine learning to detect anomalies")
    print("   - Better for complex patterns and large data")
    print()
    print("2. ⚡ Traditional Cleaner (Recommended for small datasets)")
    print("   - Uses rule-based cleaning")
    print("   - Faster for small to medium datasets")
    print("="*50)
    
    while True:
        try:
            choice = input("\nEnter your choice (1 or 2): ").strip()
            if choice == '1':
                print("✅ Using ML-Powered Cleaner")
                return True
            elif choice == '2':
                print("✅ Using Traditional Cleaner")
                return False
            else:
                print("❌ Please enter 1 or 2")
        except KeyboardInterrupt:
            print("\n👋 Exiting...")
            sys.exit(0)
        except Exception as e:
            print(f"❌ Error: {e}")

def main(use_ml=False):
    """Main pipeline function - now takes use_ml as parameter"""
    # Initialize pipeline with chosen cleaner
    pipeline = CustomerFeedbackPipeline(use_ml=use_ml)
    
    print("🚀 Starting Customer Feedback Pipeline...")
    
    # Run the pipeline
    pipeline.add_structured_data('data/raw/customer_data.csv', 'csv')
    pipeline.add_unstructured_data('data/raw/reviews/', 'text')
    pipeline.save_to_csv('data/processed/cleaned_customer_data.csv')
    
    print("🎉 Pipeline completed successfully!")

# Keep this for backward compatibility
if __name__ == "__main__":
    # If run directly, use interactive mode
    use_ml = ask_user_for_cleaner()
    main(use_ml)