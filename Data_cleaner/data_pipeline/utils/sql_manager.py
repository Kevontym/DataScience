import sqlite3
import pandas as pd
from pathlib import Path
import json
from datetime import datetime

class ChangeReportManager:
    def __init__(self, db_path="report_gen.db"):
        self.db_path = Path(db_path)
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        self._init_database()
    
    def _init_database(self):
        """Initialize the database with required tables"""
        conn = sqlite3.connect(self.db_path)
        
        conn.executescript("""
            -- Main runs table to track each pipeline execution
            CREATE TABLE IF NOT EXISTS pipeline_runs (
                run_id INTEGER PRIMARY KEY AUTOINCREMENT,
                timestamp TEXT NOT NULL,
                cleaner_type TEXT NOT NULL,
                input_file TEXT,
                output_file TEXT,
                total_records INTEGER,
                total_changes INTEGER,
                duration_seconds REAL,
                status TEXT DEFAULT 'completed'
            );
            
            -- Changes table to store all changes from all runs
            CREATE TABLE IF NOT EXISTS changes (
                change_id INTEGER PRIMARY KEY AUTOINCREMENT,
                run_id INTEGER,
                timestamp TEXT NOT NULL,
                column_name TEXT NOT NULL,
                operation TEXT NOT NULL,
                original_value TEXT,
                new_value TEXT,
                row_index INTEGER,
                FOREIGN KEY (run_id) REFERENCES pipeline_runs (run_id)
            );
            
            -- Create indexes for fast querying
            CREATE INDEX IF NOT EXISTS idx_changes_run_id ON changes(run_id);
            CREATE INDEX IF NOT EXISTS idx_changes_column ON changes(column_name);
            CREATE INDEX IF NOT EXISTS idx_changes_operation ON changes(operation);
            CREATE INDEX IF NOT EXISTS idx_runs_timestamp ON pipeline_runs(timestamp);
        """)
        
        conn.close()
        print(f"🗃️  Report database initialized: {self.db_path}")
    
    def _convert_to_sql_safe(self, value):
        """Convert values to SQL-safe types"""
        if value is None:
            return None
        elif isinstance(value, (int, float, str)):
            return value
        elif isinstance(value, (list, dict)):
            return json.dumps(value)
        elif hasattr(value, 'isoformat'):  # Handle timestamps
            return value.isoformat()
        else:
            return str(value)
    
    def store_pipeline_run(self, run_data, changes_df):
        """Store a complete pipeline run in the database"""
        conn = sqlite3.connect(self.db_path)
        
        try:
            # 1. Insert main run record with safe type conversion
            cursor = conn.execute("""
                INSERT INTO pipeline_runs 
                (timestamp, cleaner_type, input_file, output_file, total_records, total_changes, duration_seconds)
                VALUES (?, ?, ?, ?, ?, ?, ?)
            """, (
                self._convert_to_sql_safe(run_data['timestamp']),
                self._convert_to_sql_safe(run_data['cleaner_type']),
                self._convert_to_sql_safe(run_data.get('input_file')),
                self._convert_to_sql_safe(run_data.get('output_file')),
                self._convert_to_sql_safe(run_data.get('total_records')),
                self._convert_to_sql_safe(run_data.get('total_changes')),
                self._convert_to_sql_safe(run_data.get('duration_seconds'))
            ))
            
            run_id = cursor.lastrowid
            
            # 2. Store all changes with safe type conversion
            if not changes_df.empty and 'column' in changes_df.columns:
                for _, change in changes_df.iterrows():
                    conn.execute("""
                        INSERT INTO changes 
                        (run_id, timestamp, column_name, operation, original_value, new_value, row_index)
                        VALUES (?, ?, ?, ?, ?, ?, ?)
                    """, (
                        run_id,
                        self._convert_to_sql_safe(change.get('timestamp', datetime.now().isoformat())),
                        self._convert_to_sql_safe(change.get('column')),
                        self._convert_to_sql_safe(change.get('operation')),
                        self._convert_to_sql_safe(change.get('original_value')),
                        self._convert_to_sql_safe(change.get('new_value')),
                        self._convert_to_sql_safe(change.get('row_index'))
                    ))
            
            conn.commit()
            print(f"💾 Pipeline run stored in database (Run ID: {run_id})")
            
            # Show some useful queries
            self._print_usage_queries(run_id)
            return run_id
            
        except Exception as e:
            conn.rollback()
            print(f"❌ Failed to store pipeline run: {e}")
            import traceback
            traceback.print_exc()
            return None
        finally:
            conn.close()
    
    def _print_usage_queries(self, run_id):
        """Print useful queries for the user"""
        print(f"\n💡 CENTRAL SQL DATABASE: {self.db_path}")
        print(f"   All runs: sqlite3 {self.db_path} \"SELECT run_id, timestamp, cleaner_type, total_records, total_changes FROM pipeline_runs ORDER BY timestamp DESC LIMIT 5;\"")
        print(f"   Run {run_id} info: sqlite3 {self.db_path} \"SELECT * FROM pipeline_runs WHERE run_id = {run_id};\"")
        print(f"   Run {run_id} changes: sqlite3 {self.db_path} \"SELECT column_name, operation, COUNT(*) FROM changes WHERE run_id = {run_id} GROUP BY column_name, operation ORDER BY COUNT(*) DESC LIMIT 10;\"")
        print(f"   Interactive: sqlite3 {self.db_path}")

    def get_recent_runs(self, limit=10):
        """Get recent pipeline runs"""
        conn = sqlite3.connect(self.db_path)
        query = """
            SELECT run_id, timestamp, cleaner_type, input_file, total_records, total_changes
            FROM pipeline_runs 
            ORDER BY timestamp DESC 
            LIMIT ?
        """
        df = pd.read_sql(query, conn, params=(limit,))
        conn.close()
        return df