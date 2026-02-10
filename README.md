#  Hacker News ETL Pipeline

This project is part of my **Data Engineering Learning Journey**. It demonstrates a complete "Extract, Load" (EL) pipeline that pulls live tech news data, optimizes it for big data storage, and automates the ingestion into a cloud data warehouse.

##  Project Overview
The goal of this project was to move away from static CSV files and build a pipeline that handles real-world API data, manages schema types, and utilizes industry-standard storage formats.

### Tech Stack
* **Language:** Python 3.x
* **Data Source:** Hacker News API (Firebase)
* **Storage Format:** Apache Parquet (Columnar storage)
* **Cloud Warehouse:** Google BigQuery
* **Libraries:** `pandas`, `pyarrow`, `google-cloud-bigquery`, `requests`

---

##  Pipeline Architecture

### 1. Data Ingestion (Extract)
I used the Hacker News API to fetch the top 20 trending stories. 
* **Challenge:** The API returns a list of IDs, requiring nested requests to get individual story details.
* **Handling:** Implemented a Python script that maps JSON responses into a structured format, including timestamps, scores, and URLs.

### 2. Optimization (Storage)
Instead of saving as a CSV, I converted the data into **Apache Parquet**.
* **Why Parquet?** It is a columnar storage format that provides superior compression and faster query performance in big data environments compared to row-based formats.

### 3. Data Loading (Load)
Using a Google Cloud Service Account, I automated the upload of the Parquet files into **Google BigQuery**.
* **Schema Auto-detection:** BigQuery automatically recognized the Parquet schema, ensuring `timestamp` and `integer` fields were correctly typed.
* **Write Disposition:** Configured to `WRITE_APPEND` to allow for historical data accumulation over time.



