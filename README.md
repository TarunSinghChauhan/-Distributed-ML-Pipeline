# Distributed ML Training Data Pipeline

[![Spark](https://img.shields.io/badge/Apache_Spark-3.4-E25A1C?logo=apachespark&logoColor=white)](https://spark.apache.org/)
[![Airflow](https://img.shields.io/badge/Apache_Airflow-2.7-017CEE?logo=apacheairflow&logoColor=white)](https://airflow.apache.org/)
[![MinIO](https://img.shields.io/badge/MinIO-S3_Compatible-00BFFF?logo=minio&logoColor=white)](https://min.io/)
[![Python](https://img.shields.io/badge/Python-3.9-3776AB?logo=python&logoColor=white)](https://www.python.org/)

A production-grade, distributed data engineering pipeline designed to collect, validate, and process 5,000,000+ documents (50GB+) for Large Language Model (LLM) pre-training.

## 🏗️ Architecture

```text
       [ DATA SOURCES ]
    (Web, DB, API, Docs)
            |
            v
    [ MINIO LANDING ZONE ] <--- S3 Compatible Storage
            |
            v
    [ PYSPARK INGESTION ]  <--- Schema Validation (Pandera Style)
            |                   Quarantine Logic
            v
    [ BRONZE LAYER ]       <--- Raw Validated Parquet
            |
            v
    [ PYSPARK DEDUPE ]     <--- MinHash LSH (Near-Duplicate Detect)
            |
            v
    [ SILVER LAYER ]       <--- Deduplicated Documents
            |
            v
    [ QUALITY FILTERING ]  <--- Language Detect, Perplexity, Toxicity
            |
            v
    [ VERSIONED DATASET ]  <--- DVC Style Manifests & Hashing
            |
            v
    [ STREAMLIT DASHBOARD ] <--- Monitoring & Analytics
```

## 🛠️ Technology Stack
- **Orchestration**: Apache Airflow 2.7
- **Processing**: Apache Spark 3.4 (1 Master, 2 Workers)
- **Storage**: MinIO (S3 Compatible)
- **Monitoring**: Streamlit (Pro UI), Flower, Redis
- **Data Quality**: Great Expectations, Pandera Integration
- **Deduplication**: MinHash LSH (DataSketch)

## 🚀 Fast Start
1. **Initialize Infrastructure**:
   ```bash
   cd docker
   docker-compose up -d
   ```
2. **Generate Synthetic Data**:
   ```bash
   pip install faker tqdm numpy pandas pyarrow
   python producers/data_generator.py
   ```
3. **Access Interfaces**:
   - **Airflow**: `http://localhost:8081` (admin/admin)
   - **MinIO**: `http://localhost:9001` (admin/password123)
   - **Spark UI**: `http://localhost:8080`
   - **Dashboard**: `http://localhost:8501`

## 📊 Pipeline Stages
1. **Ingestion**: Parallel reading of multi-source Parquet files with automatic quarantine for schema violations.
2. **Deduplication**: MinHash LSH with 128 hash functions and 20 bands to identify near-duplicates with 0.8 Jaccard threshold.
3. **Quality Filtering**: Sequential stages for language detection (English), perplexity-based noise removal, and keyword toxicity blocking.
4. **Versioning**: Immutable dataset publishing with manifest generation and deterministic dataset-wide SHA256 checksums.

## 📈 Performance Optimizations
- **Broadcast Joins**: Used for small lookup tables (blocklists).
- **Zstd Compression**: Optimized for text density vs processing speed.
- **AQE**: Enabled Adaptive Query Execution for dynamic partition coalescing.
- **Local MinIO S3A**: Optimized Hadoop configuration for sub-second S3 access.

---
*Created by [Your Name] for Senior Data Engineer Portfolio.*
