#!/usr/bin/env python3
"""
Main launcher for Data Cleaner Project
Handles all path issues and provides consistent entry point
"""
import sys
import os
import argparse

def setup_environment():
    """Set up Python path and environment - COMPLETELY PORTABLE"""
    # Get the project root directory (where run.py is located)
    project_root = os.path.dirname(os.path.abspath(__file__))
    
    # Add project root to Python path
    if project_root not in sys.path:
        sys.path.insert(0, project_root)
    
    print(f"🚀 Project root: {project_root}")
    return project_root

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
    print()
    print("3. 🔥 PyTorch Encoder Cleaner (Experimental)")
    print("   - Uses neural networks for anomaly detection")
    print("   - Best for complex pattern recognition")
    print("="*50)
    
    while True:
        try:
            choice = input("\nEnter your choice (1, 2, or 3): ").strip()
            if choice == '1':
                print("✅ Using ML-Powered Cleaner")
                return 'ml'
            elif choice == '2':
                print("✅ Using Traditional Cleaner")
                return 'traditional'
            elif choice == '3':
                print("✅ Using PyTorch Encoder Cleaner")
                return 'pytorch'
            else:
                print("❌ Please enter 1, 2, or 3")
        except KeyboardInterrupt:
            print("\n👋 Exiting...")
            sys.exit(0)
        except Exception as e:
            print(f"❌ Error: {e}")


def ask_user_for_files(default_structured, default_unstructured, default_output):
    """Interactive prompt for file paths with data type selection"""
    print("\n" + "="*50)
    print("📁 DATA PROCESSING CONFIGURATION")
    print("="*50)
    
    # Let user choose what type of data to process
    print("\n🔍 WHAT WOULD YOU LIKE TO PROCESS?")
    print("1. 📊 Structured data only (CSV/Excel files)")
    print("2. 📁 Unstructured data only (text files, documents)")
    print("3. 🔄 Both structured and unstructured data")
    print("4. 🤔 Not sure - show me what's available")
    
    while True:
        data_choice = input("\nChoose option (1-4) [3]: ").strip() or "3"
        
        if data_choice == "1":
            return process_structured_only(default_structured, default_output)
        elif data_choice == "2":
            return process_unstructured_only(default_unstructured, default_output)
        elif data_choice == "3":
            return process_both_types(default_structured, default_unstructured, default_output)
        elif data_choice == "4":
            return discover_and_choose(default_structured, default_unstructured, default_output)
        else:
            print("❌ Please choose 1-4")

def process_structured_only(default_structured, default_output):
    """Handle structured data only processing"""
    print("\n" + "="*40)
    print("📊 PROCESSING STRUCTURED DATA ONLY")
    print("="*40)
    
    structured_path = choose_structured_file(default_structured)
    unstructured_path = None  # Skip unstructured
    output_path = input(f"Output file [{default_output}]: ").strip() or default_output
    
    # Ensure output path is not empty
    if not output_path:
        output_path = default_output
        print(f"⚠️  Using default output path: {output_path}")
    
    return structured_path, unstructured_path, output_path

def process_unstructured_only(default_unstructured, default_output):
    """Handle unstructured data only processing"""
    print("\n" + "="*40)
    print("📁 PROCESSING UNSTRUCTURED DATA ONLY")
    print("="*40)
    
    structured_path = None  # Skip structured
    unstructured_path = choose_unstructured_directory(default_unstructured)
    output_path = input(f"Output file [{default_output}]: ").strip() or default_output
    
    # Ensure output path is not empty
    if not output_path:
        output_path = default_output
        print(f"⚠️  Using default output path: {output_path}")
    
    return structured_path, unstructured_path, output_path

def process_both_types(default_structured, default_unstructured, default_output):
    """Handle both structured and unstructured data"""
    print("\n" + "="*40)
    print("🔄 PROCESSING BOTH DATA TYPES")
    print("="*40)
    
    structured_path = choose_structured_file(default_structured)
    unstructured_path = choose_unstructured_directory(default_unstructured)
    output_path = input(f"Output file [{default_output}]: ").strip() or default_output
    
    # Ensure output path is not empty
    if not output_path:
        output_path = default_output
        print(f"⚠️  Using default output path: {output_path}")
    
    return structured_path, unstructured_path, output_path

def discover_and_choose(default_structured, default_unstructured, default_output):
    """Discover available files and let user choose"""
    print("\n" + "="*40)
    print("🔍 DISCOVERING AVAILABLE FILES...")
    print("="*40)
    
    # Discover structured files
    csv_files = find_files('.csv', ['data/raw', '.', 'data'])
    excel_files = find_files('.xlsx', ['data/raw', '.', 'data'])
    
    # Discover unstructured directories
    text_dirs = find_directories_with_files(['.txt', '.log'], ['data/raw', '.', 'data'])
    
    structured_path = None
    unstructured_path = None
    
    # Let user choose from discovered files
    if csv_files or excel_files:
        print("\n📊 Discovered structured files:")
        all_structured = csv_files + excel_files
        for i, file in enumerate(all_structured, 1):
            print(f"   {i}. {file}")
        
        choice = input(f"\nChoose structured file (1-{len(all_structured)}) or Enter to skip: ").strip()
        if choice.isdigit() and 1 <= int(choice) <= len(all_structured):
            structured_path = all_structured[int(choice)-1]
    
    if text_dirs:
        print("\n📁 Discovered unstructured directories:")
        for i, dir_path in enumerate(text_dirs, 1):
            file_count = len([f for f in os.listdir(dir_path) if f.endswith(('.txt', '.log'))])
            print(f"   {i}. {dir_path}/ ({file_count} text files)")
        
        choice = input(f"\nChoose directory (1-{len(text_dirs)}) or Enter to skip: ").strip()
        if choice.isdigit() and 1 <= int(choice) <= len(text_dirs):
            unstructured_path = text_dirs[int(choice)-1]
    
    # If nothing chosen, use defaults
    if not structured_path and not unstructured_path:
        print("⚠️  No files selected. Using defaults...")
        structured_path = default_structured
        unstructured_path = default_unstructured
    elif not structured_path:
        print("ℹ️  Skipping structured data processing.")
    elif not unstructured_path:
        print("ℹ️  Skipping unstructured data processing.")
    
    output_path = input(f"Output file [{default_output}]: ").strip() or default_output
    
    return structured_path, unstructured_path, output_path

def choose_structured_file(default_structured):
    """Let user choose a structured data file"""
    csv_files = find_files('.csv', ['data/raw', '.', 'data'])
    excel_files = find_files('.xlsx', ['data/raw', '.', 'data'])
    all_files = csv_files + excel_files
    
    if all_files:
        print("\n📊 Available structured files:")
        for i, file in enumerate(all_files, 1):
            print(f"   {i}. {file}")
        choice = input(f"\nChoose file (1-{len(all_files)}) or enter custom path [{default_structured}]: ").strip()
        if choice.isdigit() and 1 <= int(choice) <= len(all_files):
            return all_files[int(choice)-1]
        else:
            return choice or default_structured
    else:
        return input(f"Structured data file [{default_structured}]: ").strip() or default_structured

def choose_unstructured_directory(default_unstructured):
    """Let user choose an unstructured data directory"""
    text_dirs = find_directories_with_files(['.txt', '.log'], ['data/raw', '.', 'data'])
    
    if text_dirs:
        print("\n📁 Available unstructured directories:")
        for i, dir_path in enumerate(text_dirs, 1):
            file_count = len([f for f in os.listdir(dir_path) if f.endswith(('.txt', '.log'))])
            print(f"   {i}. {dir_path}/ ({file_count} text files)")
        choice = input(f"\nChoose directory (1-{len(text_dirs)}) or enter custom path [{default_unstructured}]: ").strip()
        if choice.isdigit() and 1 <= int(choice) <= len(text_dirs):
            return text_dirs[int(choice)-1]
        else:
            return choice or default_unstructured
    else:
        return input(f"Unstructured data directory [{default_unstructured}]: ").strip() or default_unstructured

def find_files(extension, search_paths):
    """Find files with given extension in search paths"""
    files = []
    for path in search_paths:
        if os.path.exists(path):
            for file in os.listdir(path):
                if file.endswith(extension):
                    full_path = os.path.join(path, file)
                    files.append(full_path)
    return sorted(files)

def find_directories_with_files(file_extensions, search_paths):
    """Find directories that contain files with specified extensions"""
    dirs_with_files = []
    for path in search_paths:
        if os.path.exists(path):
            for item in os.listdir(path):
                full_path = os.path.join(path, item)
                if os.path.isdir(full_path) and not item.startswith('.'):
                    # Check if directory contains relevant files
                    for file in os.listdir(full_path):
                        if any(file.endswith(ext) for ext in file_extensions):
                            dirs_with_files.append(full_path)
                            break
    return sorted(dirs_with_files)




def create_sample_data():
    """Create sample data if it doesn't exist"""
    try:
        # Create directories
        os.makedirs('data/raw/reviews', exist_ok=True)
        os.makedirs('data/processed', exist_ok=True)
        
        # Create sample CSV data
        csv_content = """id,name,email,rating,review_text,date
1,John Doe,john@email.com,5,"Great product, loved it!",2024-01-15
2,Jane Smith,jane@email.com,3,"It was okay, could be better",2024-01-16
3,Bob Wilson,bob@email.com,4,"Pretty good overall",2024-01-17
4,Alice Brown,alice@email.com,2,"The product was fine but shipping was slow",2024-01-18
5,Charlie Davis,,5,"Excellent quality and fast delivery",
6,Diana Evans,diana@email.com,1,"Terrible experience, product didn't work",2024-01-19"""
        
        with open('data/raw/customer_data.csv', 'w') as f:
            f.write(csv_content)
        
        # Create sample review files
        reviews = [
            "The customer service was excellent and very helpful! The representative went above and beyond to solve my issue quickly.",
            "Product arrived damaged, very disappointed. The packaging was inadequate and the item was broken upon arrival.",
            "Amazing product quality and fast shipping. I will definitely purchase from this company again. Five stars!",
            "Average experience. The product works as described but the setup instructions were confusing and customer support was slow to respond."
        ]
        
        for i, review in enumerate(reviews, 1):
            with open(f'data/raw/reviews/review{i}.txt', 'w') as f:
                f.write(review)
        
        print("✅ Sample data created successfully!")
        
    except Exception as e:
        print(f"❌ Error creating sample data: {e}")



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



def ask_user_for_sql_storage():
    """Ask user if they want to use SQL storage for results."""
    while True:
        response = input("💾 Would you like to store results in SQL database? (y/n): ").lower().strip()
        if response in ['y', 'yes']:
            return True
        elif response in ['n', 'no']:
            return False
        else:
            print("❌ Please enter 'y' or 'n'")

    
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
    
    while True:
        choice = input("\nStore in SQL database? (y/n) [n]: ").strip().lower() or 'n'
        if choice in ['y', 'yes', 'n', 'no']:
            return choice in ['y', 'yes']
        else:
            print("❌ Please enter 'y' or 'n'")


def run_pipeline(cleaner_type='traditional', structured_path='data/raw/customer_data.csv', 
                unstructured_path='data/raw/reviews/', output_path='data/processed/cleaned_customer_data.csv',
                use_sql_storage=False):
    """Run the main data pipeline - PORTABLE VERSION"""
    
    # Import pandas at the TOP level
    import pandas as pd
    import datetime
    import os
    
    # Track start time for duration calculation
    start_time = pd.Timestamp.now()
    
    try:
        # Import the pipeline components
        from data_pipeline.extractors.StructuredExtractor import StructuredExtractor
        from data_pipeline.extractors.UnstructuredExtractor import UnstructuredExtractor
        from data_pipeline.transformers.data_cleaner import UniversalDataCleaner
        from data_pipeline.transformers.ml_enhanced_cleaner import MLEnhancedDataCleaner
        from data_pipeline.transformers.pytorch_encoder_cleaner import PyTorchEncoderCleaner
        
        # Import SQL manager if needed - FIXED LOGIC
        sql_manager = None
        if use_sql_storage:
            try:
                from data_pipeline.utils.sql_manager import ChangeReportManager
                sql_manager = ChangeReportManager()
                print("🗃️  SQL storage enabled - results will be saved to report_gen.db")
            except ImportError as e:
                print(f"⚠️  SQL storage unavailable: {e}")
                use_sql_storage = False
        
        class CustomerFeedbackPipeline:
            def __init__(self, cleaner_type='traditional'):
                # Choose cleaner based on cleaner_type parameter
                if cleaner_type == 'ml':
                    print("🧠 Initializing ML-Powered Cleaner...")
                    self.cleaner = MLEnhancedDataCleaner()
                elif cleaner_type == 'pytorch':
                    print("🔥 Initializing PyTorch Encoder Cleaner...")
                    self.cleaner = PyTorchEncoderCleaner()
                else:
                    print("⚡ Initializing Traditional Cleaner...")
                    self.cleaner = UniversalDataCleaner()
                
                self.all_data = pd.DataFrame()
                self.cleaner_type = cleaner_type
            
            def save_change_report(self, filepath):
                """Save detailed change report in multiple formats"""
                if hasattr(self.cleaner, 'save_change_report'):
                    # The cleaner now handles all four formats automatically
                    self.cleaner.save_change_report(filepath)
                else:
                    print("⚠️  Cleaner doesn't support enhanced change reporting")

            
            def add_structured_data(self, source_path, source_type='csv'):
                """Add structured data to the pipeline"""
                print(f"🔄 Adding structured data from {source_path}")
                
                # Check if file exists
                if not os.path.exists(source_path):
                    print(f"⚠️  File not found: {source_path}")
                    return
                
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
                
                # Check if directory exists
                if not os.path.exists(source_path):
                    print(f"⚠️  Directory not found: {source_path}")
                    return
                
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
            
            def save_to_csv(self, file_path):
                """Save cleaned data to CSV"""
                if self.all_data.empty:
                    print("❌ No data to save!")
                    return
                
                try:
                    # Create directory if it doesn't exist
                    output_dir = os.path.dirname(file_path)
                    if output_dir:  # Only create directory if path has a directory component
                        os.makedirs(output_dir, exist_ok=True)
                    
                    self.all_data.to_csv(file_path, index=False)
                    print(f"✅ Successfully saved {len(self.all_data)} records to {file_path}")
                    
                except Exception as e:
                    print(f"❌ Error saving to {file_path}: {e}")
                    # Fallback: save to current directory
                    fallback_path = "cleaned_data_output.csv"
                    self.all_data.to_csv(fallback_path, index=False)
                    print(f"✅ Saved to fallback location: {fallback_path}")
        
        # Initialize and run the pipeline
        pipeline = CustomerFeedbackPipeline(cleaner_type=cleaner_type)
        
        print("🚀 Starting Customer Feedback Pipeline...")
        
        # Only process structured data if path is provided
        if structured_path and os.path.exists(structured_path):
            pipeline.add_structured_data(structured_path, 'csv')
        elif structured_path:
            print(f"⚠️  Structured data file not found: {structured_path}")
        
        # Only process unstructured data if path is provided and exists  
        if unstructured_path and os.path.exists(unstructured_path):
            pipeline.add_unstructured_data(unstructured_path, 'text')
        elif unstructured_path:
            print(f"⚠️  Unstructured data directory not found: {unstructured_path}")
        
        # Check if we have any data to save
        if pipeline.all_data.empty:
            print("❌ No data processed! Please check your file paths.")
            return
        
        # Validate output path
        if not output_path or output_path.strip() == '':
            print("⚠️  Output path is empty, using default...")
            output_path = 'data/processed/cleaned_customer_data.csv'
        
        # Ensure output path has .csv extension
        if not output_path.endswith('.csv'):
            output_path += '.csv'
        
        # GENERATE UNIQUE FILENAME RIGHT BEFORE SAVING
        # Extract base filename and directory
        output_dir = os.path.dirname(output_path)
        base_name = os.path.basename(output_path)
        
        # Remove .csv extension if present
        if base_name.endswith('.csv'):
            base_name = base_name[:-4]
        
        # Add timestamp and cleaner type
        timestamp = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
        unique_output_path = os.path.join(output_dir, f"{base_name}_{cleaner_type}_{timestamp}.csv")
        
        print(f"💾 Saving results to: {unique_output_path}")
        
        # Save with unique filename
        pipeline.save_to_csv(unique_output_path)
        
        # Save change report with matching unique filename
        pipeline.save_change_report(unique_output_path)
        
        # ✅ FIXED: STORE IN SQL DATABASE IF REQUESTED
        if use_sql_storage and sql_manager:
            try:
                # Prepare run data for SQL storage
                run_data = {
                    'timestamp': start_time.isoformat(),
                    'cleaner_type': cleaner_type,
                    'input_file': str(structured_path) if structured_path else None,
                    'output_file': str(unique_output_path),
                    'total_records': int(len(pipeline.all_data)),
                    'total_changes': int(len(pipeline.cleaner.change_log)),
                    'duration_seconds': float((pd.Timestamp.now() - start_time).total_seconds()),
                }
                
                # Convert change log to DataFrame for storage
                changes_df = pd.DataFrame(pipeline.cleaner.change_log)
                
                # Store in database
                run_id = sql_manager.store_pipeline_run(run_data, changes_df)
                
                if run_id:
                    print(f"📊 Run stored in SQL database with ID: {run_id}")
                else:
                    print("⚠️  Failed to store run in SQL database")
                    
            except Exception as e:
                print(f"⚠️  SQL storage failed: {e}")
                # Don't crash the pipeline if SQL storage fails
        
        print("🎉 Pipeline completed successfully!")
        
    except Exception as e:
        print(f"❌ Error running pipeline: {e}")
        import traceback
        traceback.print_exc()

def main():
    parser = argparse.ArgumentParser(description='Data Cleaning Pipeline')
    parser.add_argument('--ml', action='store_true', help='Use ML-enhanced cleaning')
    parser.add_argument('--traditional', action='store_true', help='Use traditional cleaning')
    parser.add_argument('--pytorch', action='store_true', help='Use PyTorch encoder cleaning')
    parser.add_argument('--interactive', action='store_true', help='Choose cleaner interactively')
    parser.add_argument('--sample-data', action='store_true', help='Create sample data before running')
    parser.add_argument('--sql-storage', action='store_true', help='Store results in SQL database')
    parser.add_argument('--sql-terminal', action='store_true', help='Open SQL interactive terminal after run')
    
    # ADD THESE MISSING ARGUMENTS:
    parser.add_argument('--structured', type=str, default='data/raw/customer_data.csv', 
                       help='Path to structured data file')
    parser.add_argument('--unstructured', type=str, default='data/raw/reviews/',
                       help='Path to unstructured data directory')
    parser.add_argument('--output', type=str, default='data/processed/cleaned_customer_data.csv',
                       help='Output file path')
    
    args = parser.parse_args()
    
    # Use the provided paths
    structured_path = args.structured
    unstructured_path = args.unstructured
    output_path = args.output
    
    # Create sample data if requested
    if args.sample_data:
        print("📝 Creating sample data...")
        create_sample_data()
    
    # Determine which cleaner to use
    if args.interactive:
        cleaner_type = ask_user_for_cleaner()
    elif args.ml:
        cleaner_type = 'ml'
    elif args.pytorch:
        cleaner_type = 'pytorch'
    elif args.traditional:
        cleaner_type = 'traditional'
    else:
        # Default behavior - ask for everything interactively
        structured_path, unstructured_path, output_path = ask_user_for_files(
            structured_path, unstructured_path, output_path
        )
        cleaner_type = ask_user_for_cleaner()
    
    # Determine SQL storage preference
    if args.sql_storage:
        use_sql_storage = True
    else:
        # Only ask interactively if not specified via CLI
        use_sql_storage = ask_user_for_sql_storage()
    
    # Run the pipeline with SQL storage option
    run_pipeline(cleaner_type, structured_path, unstructured_path, output_path, use_sql_storage)
    
    # ✅ NEW: Open SQL terminal if requested or ask interactively
    if args.sql_terminal:
        open_sql_interactive_terminal()
    elif use_sql_storage and os.path.exists("report_gen.db"):
        # Only ask if we actually stored data in SQL
        if ask_user_for_sql_interactive():
            open_sql_interactive_terminal()

if __name__ == "__main__":
    main()