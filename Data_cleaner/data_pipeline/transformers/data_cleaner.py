import pandas as pd
import re
import numpy as np
from datetime import datetime
import json
import sqlite3
from pathlib import Path

class UniversalDataCleaner:
    def __init__(self):
        self.change_log = []

    def clean_dataframe(self, df):
        """Basic cleaning method for traditional cleaner"""
        print("⚡ Starting traditional cleaning...")
        original_df = df.copy()
        
        # Apply basic cleaning steps
        df = self._basic_cleaning(df)
        
        # Generate simple report
        self._generate_traditional_report(original_df, df)
        
        return df
    
    def _prepare_change_log_for_export(self):
        """Convert change log to export-safe formats (convert ALL timestamps to strings)"""
        if not self.change_log:
            return pd.DataFrame()
        
        print(f"🔍 Debug: First change log entry timestamp type: {type(self.change_log[0]['timestamp'])}")
        print(f"🔍 Debug: First timestamp value: {self.change_log[0]['timestamp']}")
        
        # Create a copy of the change log with serializable timestamps
        exportable_changes = []
        for i, change in enumerate(self.change_log):
            export_change = change.copy()
            
            # Convert timestamp to string using multiple approaches
            timestamp_value = export_change['timestamp']
            
            # Approach 1: Direct conversion for known types
            if isinstance(timestamp_value, (pd.Timestamp, datetime, np.datetime64)):
                export_change['timestamp'] = timestamp_value.isoformat()
            # Approach 2: Use isoformat method if available
            elif hasattr(timestamp_value, 'isoformat'):
                export_change['timestamp'] = timestamp_value.isoformat()
            # Approach 3: String conversion as last resort
            else:
                export_change['timestamp'] = str(timestamp_value)
                
            exportable_changes.append(export_change)
        
        change_df = pd.DataFrame(exportable_changes)
        
        # Final safety check - convert entire column to string
        if 'timestamp' in change_df.columns:
            change_df['timestamp'] = change_df['timestamp'].astype(str)
        
        print(f"🔍 Debug: After conversion, first timestamp: {change_df['timestamp'].iloc[0]}")
        print(f"🔍 Debug: Timestamp dtype: {change_df['timestamp'].dtype}")
        
        return change_df

    def _basic_cleaning(self, df):
        """Basic data cleaning steps"""
        print("🔧 Applying basic data cleaning...")
        
        # Remove completely empty rows
        initial_rows = len(df)
        df = df.dropna(how='all')
        if len(df) < initial_rows:
            removed = initial_rows - len(df)
            print(f"   📝 Removed {removed} completely empty rows")
        
        # Remove duplicates
        initial_rows = len(df)
        df = df.drop_duplicates()
        if len(df) < initial_rows:
            removed = initial_rows - len(df)
            print(f"   📝 Removed {removed} duplicate rows")
        
        # Handle missing values
        for col in df.columns:
            missing_count = df[col].isna().sum()
            if missing_count > 0:
                if df[col].dtype == 'object':
                    df[col] = df[col].fillna('Unknown')
                    self._log_change(col, 'fillna_categorical', None, 'Unknown')
                else:
                    median_val = df[col].median()
                    df[col] = df[col].fillna(median_val)
                    self._log_change(col, 'fillna_numeric', None, median_val)
                print(f"   📝 Filled {missing_count} missing values in '{col}'")
        
        return df
    
    def _generate_traditional_report(self, original_df, cleaned_df):
        """Generate traditional cleaning report"""
        print("\n" + "="*50)
        print("⚡ TRADITIONAL CLEANING REPORT")
        print("="*50)
        print(f"Total changes: {len(self.change_log)}")
        
        # Group by operation type
        ops = {}
        for change in self.change_log:
            op = change['operation']
            ops[op] = ops.get(op, 0) + 1
        
        for op, count in ops.items():
            print(f"   {op}: {count} changes")
        
        # Data quality improvements
        print(f"\n📈 Data Quality Improvements:")
        print(f"   Original rows: {len(original_df)}")
        print(f"   Cleaned rows: {len(cleaned_df)}")
        print(f"   Missing values reduced: {original_df.isna().sum().sum()} → {cleaned_df.isna().sum().sum()}")
        print("="*50)
    
    # KEEP ONLY ONE _log_change METHOD - REMOVE THE DUPLICATE!
    def _log_change(self, column, operation, original_value, new_value, row_index=None):
        """Log changes made during cleaning"""
        # Create timestamp as string immediately
        timestamp_str = pd.Timestamp.now().isoformat()
        
        # Handle original_value conversion for dates/timestamps
        if original_value is not None:
            if isinstance(original_value, (pd.Timestamp, datetime)):
                original_str = original_value.isoformat()
            elif hasattr(original_value, 'isoformat'):
                original_str = original_value.isoformat()
            else:
                original_str = str(original_value)
        else:
            original_str = None
        
        # Handle new_value conversion for dates/timestamps  
        if new_value is not None:
            if isinstance(new_value, (pd.Timestamp, datetime)):
                new_str = new_value.isoformat()
            elif hasattr(new_value, 'isoformat'):
                new_str = new_value.isoformat()
            else:
                new_str = str(new_value)
        else:
            new_str = None
        
        change = {
            'column': column,
            'operation': operation,
            'original_value': original_str,
            'new_value': new_str,
            'row_index': row_index,
            'timestamp': timestamp_str
        }
        self.change_log.append(change)
    
    def save_change_report(self, filepath):
        """
        Save changes in multiple formats for different use cases
        - Parquet: Full detailed changes (analysts, debugging)
        - SQLite: Queryable database (investigations, reporting) 
        - JSON: Executive overview (quick insights)
        - CSV: Human-readable sample (quick review)
        """
        if not self.change_log:
            print("📊 No changes to report")
            return
        
        # Ensure output directory exists
        output_dir = Path(filepath).parent
        output_dir.mkdir(parents=True, exist_ok=True)
        
        # Use a different base name for change reports
        data_base_name = Path(filepath).stem
        change_base_name = f"{data_base_name}_changes"
        
        # FIX: Use the export-safe change log
        change_df = self._prepare_change_log_for_export()
        
        print(f"📊 Saving change report for {len(change_df):,} changes...")
        
        # 1. PARQUET: Full detailed changes
        parquet_path = output_dir / f"{change_base_name}.parquet"
        try:
            change_df.to_parquet(parquet_path, index=False, compression='snappy')
            parquet_size = parquet_path.stat().st_size / 1024
            print(f"   📁 Parquet (full): {parquet_path} ({parquet_size:.1f} KB)")
        except Exception as e:
            print(f"   ❌ Parquet failed: {e}")
            parquet_path = None
        
        # 2. SQLITE: Queryable database
        db_path = output_dir / f"{change_base_name}.db"
        try:
            self._save_to_sqlite(change_df, db_path)
            db_size = db_path.stat().st_size / 1024
            print(f"   🗃️  SQLite (queryable): {db_path} ({db_size:.1f} KB)")
        except Exception as e:
            print(f"   ❌ SQLite failed: {e}")
            db_path = None
        
        # 3. JSON SUMMARY: Executive overview
        summary_path = output_dir / f"{change_base_name}_summary.json"
        try:
            summary = self._generate_comprehensive_summary(change_df)
            with open(summary_path, 'w') as f:
                json.dump(summary, f, indent=2, default=str)
            summary_size = summary_path.stat().st_size / 1024
            print(f"   📋 JSON (summary): {summary_path} ({summary_size:.1f} KB)")
        except Exception as e:
            print(f"   ❌ JSON summary failed: {e}")
            summary_path = None
        
        # 4. CSV SAMPLE: Human-readable sample
        sample_path = output_dir / f"{change_base_name}_sample.csv"
        try:
            sample_size = min(1000, len(change_df))
            change_df.head(sample_size).to_csv(sample_path, index=False)
            sample_file_size = sample_path.stat().st_size / 1024
            print(f"   🔍 CSV (sample): {sample_path} ({sample_file_size:.1f} KB, {sample_size} rows)")
        except Exception as e:
            print(f"   ❌ CSV sample failed: {e}")
            sample_path = None
        
        # Print usage instructions
        print("\n💡 Usage:")
        if db_path:
            print(f"   Query: sqlite3 '{db_path}' \"SELECT * FROM changes LIMIT 5;\"")
        if parquet_path:
            print(f"   Analyze: pd.read_parquet('{parquet_path}')")
        if summary_path:
            print(f"   Overview: cat '{summary_path}' | jq .")
    
    def _save_to_sqlite(self, change_df, db_path):
        """Save to SQLite with optimized queries and indexes"""
        conn = sqlite3.connect(db_path)
        
        # Save main changes table
        change_df.to_sql('changes', conn, if_exists='replace', index=False)
        
        # Create optimized indexes for fast querying
        conn.executescript("""
            CREATE INDEX IF NOT EXISTS idx_changes_operation ON changes(operation);
            CREATE INDEX IF NOT EXISTS idx_changes_column ON changes(column);
            CREATE INDEX IF NOT EXISTS idx_changes_timestamp ON changes(timestamp);
            CREATE INDEX IF NOT EXISTS idx_changes_row ON changes(row_index);
            
            -- Summary view for quick analytics
            CREATE VIEW IF NOT EXISTS change_summary AS
            SELECT 
                operation,
                column,
                COUNT(*) as change_count,
                MIN(timestamp) as first_change,
                MAX(timestamp) as last_change
            FROM changes 
            GROUP BY operation, column
            ORDER BY change_count DESC;
            
            -- Recent changes view
            CREATE VIEW IF NOT EXISTS recent_changes AS
            SELECT * FROM changes 
            ORDER BY timestamp DESC 
            LIMIT 100;
            
            -- Most changed columns view
            CREATE VIEW IF NOT EXISTS top_changed_columns AS
            SELECT 
                column,
                COUNT(*) as total_changes,
                COUNT(DISTINCT operation) as unique_operations
            FROM changes 
            GROUP BY column 
            ORDER BY total_changes DESC;
        """)
        
        conn.close()
    
    def _generate_comprehensive_summary(self, change_df):
        """Generate comprehensive summary statistics with JSON-safe data types"""
        # Convert pandas objects to JSON-safe Python types
        def to_json_safe(obj):
            if hasattr(obj, 'item'):  # Handle pandas/numpy types
                return obj.item()
            elif hasattr(obj, 'isoformat'):  # Handle timestamps
                return obj.isoformat()
            return obj

        # FIX: Handle string timestamps for min/max calculations
        if 'timestamp' in change_df.columns and len(change_df) > 0:
            # Convert string timestamps back to datetime for calculations
            timestamp_series = pd.to_datetime(change_df['timestamp'])
            time_range = {
                "first_change": to_json_safe(timestamp_series.min()),
                "last_change": to_json_safe(timestamp_series.max())
            }
        else:
            time_range = {
                "first_change": None,
                "last_change": None
            }

        # Basic statistics
        summary = {
            "report_metadata": {
                "total_changes": len(change_df),
                "report_generated": pd.Timestamp.now().isoformat(),
                "time_range": time_range
            },
            "changes_by_operation": change_df['operation'].value_counts().to_dict(),
            "changes_by_column": change_df['column'].value_counts().to_dict(),
            "data_quality_metrics": {
                "missing_values_filled": len(change_df[change_df['operation'].str.contains('fillna', na=False)]),
                "anomalies_detected": len(change_df[change_df['operation'].str.contains('anomaly', na=False)]),
                "standardizations": len(change_df[change_df['operation'].str.contains('standardize', na=False)]),
                "conversions": len(change_df[change_df['operation'].str.contains('convert', na=False)]),
                "corrections": len(change_df[change_df['operation'].str.contains('correct', na=False)])
            }
        }
        
        # Handle most_active_rows safely
        most_active = change_df['row_index'].value_counts().head(10)
        summary["most_active_rows"] = {str(k): to_json_safe(v) for k, v in most_active.items()}
        
        # Handle change_patterns safely
        operations_per_col = change_df.groupby('column')['operation'].nunique()
        summary["change_patterns"] = {
            "operations_per_column": {str(k): to_json_safe(v) for k, v in operations_per_col.items()},
            "most_common_transformations": {}
        }
        
        # Handle most_common_transformations safely
        common_transforms = change_df.groupby(['column', 'operation']).size().nlargest(10)
        for (col, op), count in common_transforms.items():
            key = f"{col}_{op}"
            summary["change_patterns"]["most_common_transformations"][key] = to_json_safe(count)
        
        # Add sample of changes for context
        if len(change_df) > 0:
            summary["sample_changes"] = change_df.head(5).to_dict('records')
        
        return summary
    
    def get_change_log(self):
        """Return the change log as a DataFrame"""
        return pd.DataFrame(self.change_log)