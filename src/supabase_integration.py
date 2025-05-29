import yaml
from supabase import create_client, Client
import pandas as pd

class SupabaseUploader:
    def __init__(self, config_path="config.yaml"):
        with open(config_path, "r") as f:
            config = yaml.safe_load(f)["supabase"]

        self.url = config["url"]
        self.key = config["key"]
        self.table = config["table"]
        self.schema = config.get("schema", "public")  # default to "public"

        self.client: Client = create_client(self.url, self.key)

    def upload_dataframe(self, df: pd.DataFrame):
        records = df.to_dict(orient="records")
        for record in records:
            response = self.client.table(self.table).insert(record).execute()
            if response.status_code >= 300:
                print(f"[ERROR] Failed to insert record: {response.data}")
            else:
                print(f"[SUCCESS] Inserted: {record}")

    def fetch_all(self):
        response = self.client.table(self.table).select("*").execute()
        return response.data
