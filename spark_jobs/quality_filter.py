from pyspark.sql import SparkSession
from pyspark.sql.functions import col, udf, lit, length, expr, count
from pyspark.sql.types import StringType, DoubleType, BooleanType
import math
from collections import Counter

# --- Spark Session ---
spark = SparkSession.builder \
    .appName("ML_Pipeline_Quality_Filtering") \
    .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000") \
    .config("spark.hadoop.fs.s3a.access.key", "admin") \
    .config("spark.hadoop.fs.s3a.secret.key", "password123") \
    .config("spark.hadoop.fs.s3a.path.style.access", "true") \
    .getOrCreate()

# --- Configuration & External Assets ---
BLOCKLIST = ["toxic_word1", "harmful_phrase2", "explicit_content3"] # Mock blocklist

# --- UDFs ---

def detect_language(text):
    # Mock langdetect logic
    if not text: return "unknown"
    # Basic heuristic: if 'the' in text, assume English
    if "the" in text.lower(): return "en"
    return "other"

lang_udf = udf(detect_language, StringType())

def compute_perplexity(text):
    """
    Simulated character-level trigram perplexity.
    Documents with very repetitive or random text have high/low perplexity extremes.
    """
    if not text or len(text) < 10: return 999.0
    text = text.lower()
    chars = [c for c in text if c.isalpha() or c.isspace()]
    if len(chars) < 3: return 999.0
    
    # Just a mock statistical measure: variance of character distribution
    counts = Counter(chars)
    freqs = [v/len(chars) for v in counts.values()]
    entropy = -sum(f * math.log2(f) for f in freqs)
    # Perplexity 2^entropy
    return math.pow(2, entropy)

perplexity_udf = udf(compute_perplexity, DoubleType())

def toxicity_score(text):
    if not text: return 0.0
    text_lower = text.lower()
    matches = sum(1 for word in BLOCKLIST if word in text_lower)
    return float(matches)

toxicity_udf = udf(toxicity_score, DoubleType())

def main():
    INPUT_PATH = "s3a://processed-data/silver_deduped"
    OUTPUT_PATH = "s3a://processed-data/silver_quality_filtered"
    REJECTION_PATH = "s3a://processed-data/rejected_documents"
    
    df = spark.read.parquet(INPUT_PATH)
    
    # --- STAGE 1: Language Filter ---
    df = df.withColumn("lang", lang_udf(col("text")))
    lang_filtered = df.filter(col("lang") != "en").withColumn("filter_reason", lit("NON_ENGLISH")).withColumn("filter_stage", lit("LANGUAGE"))
    df = df.filter(col("lang") == "en")
    
    # --- STAGE 2: Length Filter ---
    # 50 words to 100,000 words
    length_filtered = df.filter((col("word_count") < 50) | (col("word_count") > 100000)) \
                        .withColumn("filter_reason", lit("INVALID_LENGTH")) \
                        .withColumn("filter_stage", lit("LENGTH"))
    df = df.filter((col("word_count") >= 50) & (col("word_count") <= 100000))
    
    # --- STAGE 3: Toxicity Filter ---
    df = df.withColumn("toxicity_score", toxicity_udf(col("text")))
    toxic_filtered = df.filter(col("toxicity_score") > 0) \
                       .withColumn("filter_reason", lit("TOXIC_CONTENT")) \
                       .withColumn("filter_stage", lit("TOXICITY"))
    df = df.filter(col("toxicity_score") == 0)
    
    # --- STAGE 4: Perplexity Filter ---
    df = df.withColumn("perplexity", perplexity_udf(col("text")))
    # Filter 90th percentile (Mock: say perplexity > 4.5 is too high/random)
    perp_filtered = df.filter(col("perplexity") > 4.5) \
                      .withColumn("filter_reason", lit("HIGH_PERPLEXITY")) \
                      .withColumn("filter_stage", lit("PERPLEXITY"))
    df = df.filter(col("perplexity") <= 4.5)
    
    # --- STAGE 5: Exact Dedupe (Residual) ---
    # Final check on content_hash
    df = df.dropDuplicates(["content_hash"])
    
    # --- Materialize ---
    df.write.mode("overwrite").parquet(OUTPUT_PATH)
    
    # Route Rejections
    all_rejections = lang_filtered.select("document_id", "filter_reason", "filter_stage") \
        .union(length_filtered.select("document_id", "filter_reason", "filter_stage")) \
        .union(toxic_filtered.select("document_id", "filter_reason", "filter_stage")) \
        .union(perp_filtered.select("document_id", "filter_reason", "filter_stage"))
        
    all_rejections.write.mode("append").partitionBy("filter_stage").parquet(REJECTION_PATH)
    
    print("Quality Filtering Complete.")
    print(f"Accepted records: {df.count()}")
    print(f"Rejected records: {all_rejections.count()}")

if __name__ == "__main__":
    main()
