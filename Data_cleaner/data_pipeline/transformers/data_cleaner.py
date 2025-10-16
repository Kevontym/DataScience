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
    
    def _log_change(self, column, operation, original_value, new_value, row_index=None):
        """Log changes made during cleaning"""
        change = {
            'column': column,
            'operation': operation,
            'original_value': str(original_value) if original_value is not None else None,
            'new_value': str(new_value) if new_value is not None else None,
            'row_index': row_index,
            'timestamp': pd.Timestamp.now().isoformat()
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
        change_base_name = f"{data_base_name}_changes"  # ← Add "_changes" suffix
        
        change_df = pd.DataFrame(self.change_log)
        
        print(f"📊 Saving change report for {len(change_df):,} changes...")
        
        # 1. PARQUET: Full detailed changes
        parquet_path = output_dir / f"{change_base_name}.parquet"  # ← Use change_base_name
        try:
            change_df.to_parquet(parquet_path, index=False, compression='snappy')
            parquet_size = parquet_path.stat().st_size / 1024
            print(f"   📁 Parquet (full): {parquet_path} ({parquet_size:.1f} KB)")
        except Exception as e:
            print(f"   ❌ Parquet failed: {e}")
            parquet_path = None
        
        # 2. SQLITE: Queryable database
        db_path = output_dir / f"{change_base_name}.db"  # ← Use change_base_name
        try:
            self._save_to_sqlite(change_df, db_path)
            db_size = db_path.stat().st_size / 1024
            print(f"   🗃️  SQLite (queryable): {db_path} ({db_size:.1f} KB)")
        except Exception as e:
            print(f"   ❌ SQLite failed: {e}")
            db_path = None
        
        # 3. JSON SUMMARY: Executive overview
        summary_path = output_dir / f"{change_base_name}_summary.json"  # ← Use change_base_name
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
        sample_path = output_dir / f"{change_base_name}_sample.csv"  # ← Use change_base_name
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

        # Basic statistics
        summary = {
            "report_metadata": {
                "total_changes": len(change_df),
                "report_generated": pd.Timestamp.now().isoformat(),
                "time_range": {
                    "first_change": to_json_safe(change_df['timestamp'].min()),
                    "last_change": to_json_safe(change_df['timestamp'].max())
                }
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