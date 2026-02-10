import os
from glob import glob
from google.cloud import bigquery

# 1. Load .env (simple parser) and set credentials path
workspace_dir = os.path.dirname(os.path.abspath(__file__))
env_path = os.path.join(workspace_dir, ".env")

if os.path.exists(env_path):
    with open(env_path, "r", encoding="utf-8") as env_file:
        for line in env_file:
            raw = line.strip()
            if not raw or raw.startswith("#") or "=" not in raw:
                continue
            key, value = raw.split("=", 1)
            key = key.strip()
            value = value.strip().strip('"').strip("'")
            os.environ.setdefault(key, value)

credentials_path = os.getenv("GOOGLE_APPLICATION_CREDENTIALS")
if not credentials_path:
    raise ValueError(
        "GOOGLE_APPLICATION_CREDENTIALS is not set. Set it in .env or your shell."
    )
if not os.path.exists(credentials_path):
    raise FileNotFoundError(f"Credentials file not found: {credentials_path}")

client = bigquery.Client()

# 2. Define your destination (Update these with your actual IDs)
project_id = os.getenv("PROJECT_ID")   # The ID from your Google Cloud Dashboard
dataset_id = os.getenv("DATASET_ID")
table_id = os.getenv("TABLE_ID")

missing = [name for name, value in [("PROJECT_ID", project_id), ("DATASET_ID", dataset_id), ("TABLE_ID", table_id)] if not value]
if missing:
    raise ValueError(f"Missing required env vars: {', '.join(missing)}")

full_table_path = f"{project_id}.{dataset_id}.{table_id}"

# 3. Configure the Load Job
job_config = bigquery.LoadJobConfig(
    source_format=bigquery.SourceFormat.PARQUET,
    write_disposition=bigquery.WriteDisposition.WRITE_APPEND, # Adds new data to the end
)

# 4. Find the latest parquet file
parquet_candidates = sorted(
    glob(os.path.join(workspace_dir, "hn_tech_news_*.parquet"))
)
if not parquet_candidates:
    raise FileNotFoundError("No hn_tech_news_*.parquet files found in workspace.")

file_path = parquet_candidates[-1]

# 5. Ensure dataset exists
dataset_ref = bigquery.Dataset(f"{project_id}.{dataset_id}")
dataset_ref.location = "South-Africa"  # Update if your dataset is in a different location
client.create_dataset(dataset_ref, exists_ok=True)

with open(file_path, "rb") as source_file:
    print(f"⬆️ Uploading {os.path.basename(file_path)} to BigQuery...")
    job = client.load_table_from_file(source_file, full_table_path, job_config=job_config)

job.result()  # Wait for the upload to finish

print(f"✅ Success! Table {table_id} now has {client.get_table(full_table_path).num_rows} total rows.")
