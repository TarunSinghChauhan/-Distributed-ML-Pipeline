# Data Engineering Interview Q&A: Distributed ML Pipeline

### 1. How does MinHash LSH work mathematically, and why choose 128 hashes and 20 bands?
**Answer**: MinHash approximates Jaccard similarity by hashing shingles and taking the minimum hash value; the probability of two documents sharing a minhash equals their Jaccard similarity. LSH (Locality Sensitive Hashing) groups these hashes into $b$ bands of $r$ rows. The probability of a collision is $1 - (1 - s^r)^b$. With 128 hashes, 20 bands, and ~6 rows per band, we create an "S-curve" threshold around $s \approx (1/20)^{1/6} \approx 0.6-0.8$. This balance minimizes false negatives (missing duplicates) while keeping computational cost manageable by avoiding $O(N^2)$ comparisons.

### 2. What is the tradeoff between Jaccard threshold and computational cost?
**Answer**: Lowering the threshold (e.g., to 0.5) increases the number of candidate pairs flagged by LSH, requiring more Spark shuffle operations and "exact" Jaccard verification, thus increasing cost. Raising the threshold to 0.9 reduces candidates and cost but risks keeping near-duplicates (e.g., same content with different headers) that degrade ML model quality.

### 3. How do you handle data skew in PySpark when 60% of records come from 10 domains?
**Answer**: I implement **salting**. I add a random integer (salt) to the join/grouping key of the skewed records to distribute them across more partitions. I also leverage **Adaptive Query Execution (AQE)** in Spark 3.x (`spark.sql.adaptive.skewJoin.enabled`), which automatically detects skew and splits giant partitions into smaller chunks at runtime.

### 4. What does Adaptive Query Execution (AQE) do exactly?
**Answer**: AQE re-optimizes the query plan *during* execution based on runtime statistics. It specifically solves three problems: 1) **Coalescing shuffle partitions** (reducing 200 partitions to 10 if data is small), 2) **Switching join strategies** (e.g., from Sort-Merge to Broadcast if one side is smaller than expected), and 3) **Handling skew joins** by splitting large partitions.

### 5. How would you scale this from 5 million to 5 billion documents?
**Answer**: 1) Move from LocalExecutor to **Celery/Kubernetes Executor** in Airflow. 2) Migrate Spark to a managed cluster (EMR/Dataproc) with **Auto-scaling**. 3) Use **Incremental Processing**: instead of full dataset scans, use Airflow to process only "new" partitions. 4) Use **Bloom Filters** for fast existence checks before expensive LSH processing.

### 6. Perplexity-based vs. Rule-based filtering: when to use each?
**Answer**: Rule-based (length, keywords) is great for "known bads" and is computationally cheap. **Perplexity-based filtering** (using a language model like a trigram model) captures "gibberish" or non-natural language (e.g., log files, HTML junk) that rules miss. Use perplexity to ensure the "naturalness" of text, but it requires a reference distribution for the target language.

### 7. How does DVC-style versioning solve problems for ML teams?
**Answer**: ML experiments must be reproducible. DVC conceptually decouples metadata (manifests) from large data files. By hashing the dataset and storing the manifest in Git, we ensure that a specific version of a model is trained on the *exact* same data every time, preventing "data drift" from skewing experiment results.

### 8. How do you implement exactly-once processing guarantees?
**Answer**: I use **Idempotent Sinks**. By writing to partitioned Parquet files with `mode("overwrite")` for a specific `date` partition, running the job multiple times won't duplicate data. Additionally, I track processed file hashes in a metadata table to ensure the same source file isn't ingested twice.

### 9. What partitioning strategy is best for queries by date AND source?
**Answer**: I implement **Hierarchical Partitioning**: `/date=YYYY-MM-DD/source=domain/`. This allows Spark's predicate pushdown to skip entire directories when filtering by either dimension. For extremely skewed sources, I might use **Bucket Partitioning** within the source folder.

### 10. How do you monitor data drift between dataset versions?
**Answer**: I track **Source Distribution Shift** and **Metric Variance**. For example, if v1 had 40% web crawl data and v2 has 70%, that's a significant drift. I use the manifest files to generate a "Diff Report" comparing record counts, average word counts, and quality pass-rates across versions.
吐
