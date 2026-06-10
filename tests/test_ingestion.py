import pytest
from pyspark.sql import SparkSession
from spark_jobs.ingestion import validate_schema

@pytest.fixture(scope="session")
def spark():
    return SparkSession.builder.master("local[1]").appName("Tests").getOrCreate()

def test_validate_schema_invalid_id(spark):
    data = [
        (None, "Sample text", 100, "a"*64), # Null ID
        ("id1", "Short", 5, "a"*64),       # Valid
    ]
    df = spark.createDataFrame(data, ["document_id", "text", "word_count", "content_hash"])
    
    valid, invalid = validate_schema(df, "web_crawl")
    
    assert valid.count() == 1
    assert invalid.count() == 1
    assert invalid.collect()[0]["rejection_reason"] == "Missing ID"

def test_validate_schema_invalid_hash(spark):
    data = [
        ("id1", "Sample", 100, "short_hash") # Invalid hash length
    ]
    df = spark.createDataFrame(data, ["document_id", "text", "word_count", "content_hash"])
    
    valid, invalid = validate_schema(df, "web_crawl")
    
    assert invalid.count() == 1
    assert "Invalid SHA256 Hash" in invalid.collect()[0]["rejection_reason"]
吐
