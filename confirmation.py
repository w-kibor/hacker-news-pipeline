import pandas as pd

# Load the file
df = pd.read_parquet('hn_tech_news_20260210_13.parquet')

# 1. See the data
print("--- FIRST 5 ROWS ---")
print(df.head())

# 2. See the 'Schema' (Data Types)
print("\n--- DATA TYPES ---")
print(df.dtypes)

# 3. See the file size difference (Optional but cool)
import os
print(f"\nFile Size: {os.path.getsize('hn_tech_news_20260210_13.parquet') / 1024:.2f} KB")