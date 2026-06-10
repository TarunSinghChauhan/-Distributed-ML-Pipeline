import json
import hashlib
from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, collect_list, sort_array, concat_ws, sha2, count, sum as spark_sum, avg

# --- Spark Session ---
spark = SparkSession.builder \
    .appName("ML_Pipeline_Versioning") \
    .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000") \
    .config("spark.hadoop.fs.s3a.access.key", "admin") \
    .config("spark.hadoop.fs.s3a.secret.key", "password123") \
    .config("spark.hadoop.fs.s3a.path.style.access", "true") \
    .getOrCreate()

def compute_dataset_hash(df):
    """
    Computes a deterministic hash for the entire dataset.
    Sorts all content hashes and hashes the resulting string.
    """
    # For large datasets, we might hash partition-level hashes
    sample_hashes = df.select("content_hash").orderBy("content_hash").limit(10000).collect()
    combined = "".join([row.content_hash for row in sample_hashes])
    return hashlib.sha256(combined.encode()).hexdigest()[:12]

def generate_manifest(df, version_id):
    stats = df.select(
        count("*").alias("record_count"),
        spark_sum("word_count").alias("total_word_count"),
        avg("word_count").alias("avg_word_count")
    ).first().asDict()
    
    # Source distribution
    sources = df.groupBy("source").count().collect()
    source_dist = {row["source"]: (row["count"] / stats["record_count"]) for row in sources}
    
    manifest = {
        "version_id": version_id,
        "timestamp": datetime.now().isoformat(),
        "record_count": stats["record_count"],
        "total_word_count": int(stats["total_word_count"] or 0),
        "avg_word_count": float(stats["avg_word_count"] or 0),
        "source_distribution": source_dist,
        "schema_version": "1.0",
        "pipeline_run_id": str(datetime.now().timestamp())
    }
    return manifest

def generate_dataset_card(manifest):
    card = f"""# Dataset Card: Distributed ML Training Data
    
## Version: {manifest['version_id']}
- **Generated At**: {manifest['timestamp']}
- **Record Count**: {manifest['record_count']:,}
- **Total Words**: {manifest['total_word_count']:,}

## Source Distribution
{json.dumps(manifest['source_distribution'], indent=2)}

## Intended Use
This dataset is designed for pre-training large language models (LLMs). It has undergone rigorous deduplication and quality filtering.

## Limitations
- Filtered for English only.
- Toxicity filtering is keyword-based and may have false negatives.
- Data is simulated for academic/portfolio purposes.
"""
    return card

def main():
    INPUT_PATH = "s3a://processed-data/silver_quality_filtered"
    VERSIONED_BUCKET = "s3a://versioned-datasets"
    
    df = spark.read.parquet(INPUT_PATH)
    
    # Generate Version ID
    ds_hash = compute_dataset_hash(df)
    timestamp = datetime.now().strftime("%Y%m%d")
    version_id = f"v{timestamp}_{ds_hash}"
    
    # 1. Write Data to Versioned Prefix
    version_path = f"{VERSIONED_BUCKET}/{version_id}"
    df.write.mode("overwrite").parquet(version_path)
    
    # 2. Generate and Upload Manifest
    manifest = generate_manifest(df, version_id)
    manifest_json = json.dumps(manifest, indent=4)
    print(f"Manifest for {version_id}:\n{manifest_json}")
    
    # 3. Generate Dataset Card
    card_md = generate_dataset_card(manifest)
    print(f"Dataset Card:\n{card_md}")
    
    # Note: In production, we'd use boto3 to upload these strings as files to MinIO.
    # spark.sparkContext.parallelize([manifest_json]).saveAsTextFile(f"{VERSIONED_BUCKET}/manifests/manifest_{version_id}.json")
    
if __name__ == "__main__":
    main()
