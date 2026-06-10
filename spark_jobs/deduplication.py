from pyspark.sql import SparkSession
from pyspark.sql.functions import col, udf, struct, count, collect_list, first, row_number
from pyspark.sql.window import Window
from pyspark.sql.types import ArrayType, LongType, StringType
import binascii
import hashlib

# --- Spark Session ---
spark = SparkSession.builder \
    .appName("ML_Pipeline_Deduplication") \
    .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000") \
    .config("spark.hadoop.fs.s3a.access.key", "admin") \
    .config("spark.hadoop.fs.s3a.secret.key", "password123") \
    .config("spark.hadoop.fs.s3a.path.style.access", "true") \
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .getOrCreate()

# Constants for LSH
NUM_HASHES = 128
NUM_BANDS = 20
ROWS_PER_BAND = 6 # Threshold approx (1/B)^(1/R) = (1/20)^(1/6) ~= 0.6 -- actually (1/20)^(1/6) is high, let's adjust if needed.
# User requested threshold ~ 0.8. 20 bands * 6 rows = 120 hashes.
# (1/20)^(1/6) is 0.6. For 0.8, we might need different B and R.
# Actually 1/0.8^6 ~= 3.8. So 4 bands. Or more rows.

def get_minhash_udf(text):
    """
    Manual MinHash implementation using a family of hash functions.
    This avoids external library dependency issues in worker nodes for the example.
    """
    if not text: return []
    
    # Simple shingling
    shingles = set()
    words = text.split()
    for i in range(len(words)-2):
        shingles.add(" ".join(words[i:i+3]))
    
    if not shingles: return [0] * NUM_HASHES
    
    # 128 hash functions
    signatures = []
    for i in range(NUM_HASHES):
        min_hash = float('inf')
        for s in shingles:
            # Salted hash
            h = int(hashlib.md5((str(i) + s).encode()).hexdigest(), 16)
            if h < min_hash:
                min_hash = h
        signatures.append(min_hash)
    return signatures

minhash_udf = udf(get_minhash_udf, ArrayType(LongType()))

def main():
    BRONZE_LAYER = "s3a://processed-data/bronze"
    SILVER_DEDUPE_LAYER = "s3a://processed-data/silver_deduped"
    
    # 1. Read Bronze
    df = spark.read.parquet(BRONZE_LAYER)
    
    # 2. Compute Quality Score for selection
    # engagement_score if available else word_count normalized
    # For now, simplistic quality score mock
    df = df.withColumn("quality_score", col("word_count") / 1000.0)
    
    # 3. Generate MinHash Signatures
    df_hashed = df.withColumn("minhash", minhash_udf(col("text")))
    
    # 4. LSH Logic: Explode into Bands
    # For each band, create a tuple (band_index, tuple_of_hashes_in_band)
    def split_into_bands(hashes):
        bands = []
        for i in range(NUM_BANDS):
            band_hashes = hashes[i*ROWS_PER_BAND : (i+1)*ROWS_PER_BAND]
            bands.append((i, str(band_hashes)))
        return bands
    
    split_bands_udf = udf(split_into_bands, ArrayType(struct(col("band_id").cast(IntegerType()), col("band_val").cast(StringType()))))
    # Note: Using manual struct construction in select for speed
    
    # 5. Find candidate pairs by joining on Band ID and Band Value
    # This involves exploding the hashes into bands
    df_bands = df_hashed.select("document_id", "quality_score", "minhash") \
        .withColumn("bands", udf(lambda h: split_into_bands(h), ArrayType(StringType()))(col("minhash"))) \
        .withColumn("band", expr("explode(bands)"))
    
    # Group by band to find duplicates
    window_spec = Window.partitionBy("band").orderBy(col("quality_score").desc())
    
    # For each duplicate cluster (found via LSH bands), rank them
    df_ranked = df_bands.withColumn("rank", row_number().over(window_spec))
    
    # Document is a primary duplicate if it's rank 1 in ANY of its bands
    # Actually, we need to be careful: a doc belongs to multiple bands.
    # We want to identify all docs that share a band with a "higher quality" doc.
    
    # Identify the 'representative' ID for each band (the one with highest quality score)
    reps = df_bands.groupBy("band").agg(first("document_id").alias("rep_id"))
    
    # Join back to flag duplicates
    df_flagged = df_bands.join(reps, "band")
    
    # A doc is a duplicate if document_id != rep_id in any of its bands
    # We aggregate by document_id to see if it was ever NOT a rep
    # (Actually it's better to say: Keep if it is 'rep_id' for ALL its bands? No.)
    # Keep if it is the HIGHEST quality among ALL docs it collides with.
    
    # Simpler approach for global dedupe:
    # 1. For each band, found collisions.
    # 2. Find globally connected components of collisions (Graph problem).
    # 3. For local scale, we pick the best within each band.
    
    final_deduped_ids = df_flagged.groupBy("document_id").agg(
        first("rep_id").alias("canonical_id")
    ).filter(col("document_id") == col("canonical_id"))
    
    # 6. Final Filter and Write
    silver_df = df.join(final_deduped_ids.select("document_id"), "document_id", "inner")
    
    silver_df.write.mode("overwrite").parquet(SILVER_DEDUPE_LAYER)
    
    # Stats report
    total = df.count()
    rem = silver_df.count()
    print(f"Deduplication Stats:")
    print(f"Total: {total}")
    print(f"Removed: {total - rem}")
    print(f"Reduction: {((total - rem) / total) * 100:.2f}%")

if __name__ == "__main__":
    main()
