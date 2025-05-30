import yaml
from supabase import create_client, Client
import pandas as pd
import logging
from datetime import datetime
import json

class SupabaseUploader:
    def __init__(self, config_path="config.yml"):
        try:
            with open(config_path, "r") as f:
                config = yaml.safe_load(f)["supabase"]

            self.url = config["url"]
            self.key = config["key"]
            self.table = config["table"]
            self.schema = config.get("schema", "public")
            
            # Initialize Supabase client
            self.client: Client = create_client(self.url, self.key)
            
            # Setup logging
            logging.basicConfig(
                level=logging.INFO,
                format='%(asctime)s - %(levelname)s - %(message)s',
                handlers=[
                    logging.FileHandler(f'logs/supabase_{datetime.now().strftime("%Y%m%d")}.log'),
                    logging.StreamHandler()
                ]
            )
            self.logger = logging.getLogger(__name__)
            
        except Exception as e:
            raise Exception(f"Failed to initialize SupabaseUploader: {str(e)}")

    def _convert_data_types(self, df: pd.DataFrame) -> pd.DataFrame:
        """Convert DataFrame columns to Supabase-compatible types"""
        df = df.copy()
        
        # Convert datetime columns to ISO format strings
        for col in df.select_dtypes(include=['datetime64']).columns:
            df[col] = df[col].dt.strftime('%Y-%m-%dT%H:%M:%S.%fZ')
            
        # Convert any remaining non-serializable types to strings
        for col in df.columns:
            if df[col].dtype == 'object':
                df[col] = df[col].astype(str)
                
        return df

    def upload_dataframe(self, df: pd.DataFrame, batch_size: int = 100):
        """
        Upload DataFrame to Supabase in batches
        
        Args:
            df: pandas DataFrame to upload
            batch_size: Number of records to upload at once
        """
        try:
            # Remove duplicated columns
            df = df.loc[:, ~df.columns.duplicated()]
            
            # Convert data types
            df = self._convert_data_types(df)
            
            # Convert DataFrame to list of dicts
            records = df.to_dict(orient="records")
            total_records = len(records)
            
            # Process in batches
            for i in range(0, total_records, batch_size):
                batch = records[i:i + batch_size]
                try:
                    response = self.client.table(self.table).insert(batch).execute()
                    
                    self.logger.info(f"Successfully inserted batch {i//batch_size + 1} ({len(batch)} records)")
                        
                except Exception as e:
                    self.logger.error(f"Error processing batch {i//batch_size + 1}: {str(e)}")
                    # Log the problematic records for debugging
                    self.logger.debug(f"Problematic records: {json.dumps(batch, default=str)}")
                    
        except Exception as e:
            self.logger.error(f"Error in upload_dataframe: {str(e)}")
            raise

    def fetch_all(self) -> list:
        """Fetch all records from the table"""
        try:
            response = self.client.table(self.table).select("*").execute()
            if response.error:
                self.logger.error(f"Error fetching data: {response.error}")
                return []
            return response.data
        except Exception as e:
            self.logger.error(f"Error in fetch_all: {str(e)}")
            return []
