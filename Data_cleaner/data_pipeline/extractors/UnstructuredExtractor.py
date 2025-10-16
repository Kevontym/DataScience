import pandas as pd
import json
import os

class UnstructuredExtractor:
    @staticmethod
    def from_text_files(folder_path):
        """Extract text from multiple .txt files"""
        data = []
        
        if not os.path.exists(folder_path):
            print(f"⚠️  Directory not found: {folder_path}")
            return pd.DataFrame()
            
        for filename in os.listdir(folder_path):
            if filename.endswith('.txt'):
                try:
                    with open(os.path.join(folder_path, filename), 'r', encoding='utf-8') as file:
                        content = file.read()
                        data.append({
                            'filename': filename,
                            'content': content,
                            'source_type': 'text_file'
                        })
                except Exception as e:
                    print(f"❌ Error reading {filename}: {e}")
        
        print(f"✅ Loaded {len(data)} text files from {folder_path}")
        return pd.DataFrame(data)
    
    @staticmethod
    def from_json(file_path):
        """Extract data from JSON files"""
        try:
            with open(file_path, 'r', encoding='utf-8') as file:
                data = json.load(file)
            
            if isinstance(data, list):
                df = pd.json_normalize(data)
            else:
                df = pd.DataFrame([data])
            
            print(f"✅ Loaded {len(df)} rows from JSON")
            return df
        except Exception as e:
            print(f"❌ Error loading JSON: {e}")
            return pd.DataFrame()