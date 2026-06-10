import os
import json
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, avg, count, when, expr
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, TimestampType, DoubleType
import datetime

# --- Spark Session Configuration for MinIO ---
spark = SparkSession.builder \
    .appName("ML_Pipeline_Ingestion") \
    .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000") \
    .config("spark.hadoop.fs.s3a.access.key", "admin") \
    .config("spark.hadoop.fs.s3a.secret.key", "password123") \
    .config("spark.hadoop.fs.s3a.path.style.access", "true") \
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false") \
    .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262") \
    .getOrCreate()

# --- Paths ---
LANDING_ZONE = "s3a://landing-zone"
BRONZE_LAYER = "s3a://processed-data/bronze"
QUARANTINE_ZONE = "s3a://processed-data/quarantine"

def validate_schema(df, source_name):
    """
    Simulating Pandera-like validation in PySpark. 
    Checks for document_id nulls, invalid word_counts, and hash formats.
    """
    # 1. document_id presence
    # 2. word_count > 0
    # 3. hash length == 64 (SHA256)
    
    # Identify key columns based on source
    id_col = "document_id" if "web_crawl" in source_name else \
             "record_id" if "structured_db" in source_name else \
             "item_id" if "api_export" in source_name else "doc_id"
    
    txt_col = "extracted_text" if "web_crawl" in source_name else \
              "content_text" if "structured_db" in source_name else \
              "body_text" if "api_export" in source_name else "content_text"

    # Add validation flags
    validated_df = df.withColumn("is_valid", 
        (col(id_col).isNotNull()) & 
        (col("word_count") > 0) & 
        (expr("length(content_hash) = 64"))
    ).withColumn("rejection_reason", 
        when(col(id_col).isNull(), "Missing ID")
        .when(col("word_count") <= 0, "Invalid Word Count")
        .when(expr("length(content_hash) != 64"), "Invalid SHA256 Hash")
        .otherwise(None)
    )
    
    valid_df = validated_df.filter(col("is_valid") == True).drop("is_valid", "rejection_reason")
    invalid_df = validated_df.filter(col("is_valid") == False).drop("is_valid")
    
    return valid_df, invalid_df

def process_source(source_name):
    print(f"Reading source: {source_name}")
    raw_df = spark.read.parquet(f"{LANDING_ZONE}/{source_name}")
    
    # Normalize schema to a common structure for the Bronze layer
    # document_id, text, source, metadata, timestamp, word_count, content_hash
    if "web_crawl" in source_name:
        clean_df = raw_df.select(
            col("document_id"),
            col("extracted_text").alias("text"),
            lit("web_crawl").alias("source"),
            col("crawl_timestamp").alias("timestamp"),
            col("word_count"),
            col("content_hash"),
            col("date")
        )
    elif "structured_db" in source_name:
        clean_df = raw_df.select(
            col("record_id").alias("document_id"),
            col("content_text").alias("text"),
            lit("structured_db").alias("source"),
            col("created_at").alias("timestamp"),
            # Compute word count if missing or use existing
            expr("size(split(content_text, ' '))").alias("word_count"),
            # Compute hash if missing or use logic
            expr("sha2(content_text, 256)").alias("content_hash"),
            col("date")
        )
    # ... Simplified for other sources for brevity in this job
    else:
        # Fallback for API and Internal
        clean_df = raw_df.select(
            col(raw_df.columns[0]).alias("document_id"),
            col(raw_df.columns[2]).alias("text"),
            lit(source_name).alias("source"),
            col("date")
        ).withColumn("timestamp", lit(datetime.datetime.now())) \
         .withColumn("word_count", expr("size(split(text, ' '))")) \
         .withColumn("content_hash", expr("sha2(text, 256)"))

    valid, invalid = validate_schema(clean_df, source_name)
    
    # Write to Bronze
    valid.write.mode("append").partitionBy("source", "date").parquet(BRONZE_LAYER)
    
    # Write to Quarantine if any
    if invalid.count() > 0:
        invalid.write.mode("append").parquet(f"{QUARANTINE_ZONE}/{source_name}")
        
    return clean_df.count(), valid.count(), invalid.count()

def main():
    sources = ["web_crawl_data", "structured_db_export", "api_export_data", "internal_documents"]
    report = {}
    
    for s in sources:
        total, valid, invalid = process_source(s)
        report[s] = {
            "total_records": total,
            "valid_records": valid,
            "invalid_records": invalid,
            "quality_score": (valid / total) if total > 0 else 0
        }
    
    # Generate Stats
    stats = spark.read.parquet(BRONZE_LAYER).groupBy("source").agg(
        count("*").alias("count"),
        avg("word_count").alias("avg_word_count")
    ).collect()
    
    report["global_stats"] = [row.asDict() for row in stats]
    
    # Save Report to MinIO
    report_json = json.dumps(report, indent=4)
    # Using small hack to write text to S3 via Spark or just print for now
    print("--- INGESTION QUALITY REPORT ---")
    print(report_json)
    
    # For a real pipeline, we'd save this JSON to a results bucket
    # spark.sparkContext.parallelize([report_json]).saveAsTextFile(f"{BRONZE_LAYER}/_reports/{datetime.datetime.now().strftime('%Y%m%d%H%M%S')}")

if __name__ == "__main__":
    main()
