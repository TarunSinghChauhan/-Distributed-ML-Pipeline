import os
import uuid
import hashlib
import random
import numpy as np
import pandas as pd
from faker import Faker
from tqdm import tqdm
from datetime import datetime, timedelta
import pyarrow as pa
import pyarrow.parquet as pq

# Configuration
TOTAL_DOCUMENTS = 5000000
RATIOS = {
    'web_crawl': 0.40,
    'structured_db': 0.20,
    'api_export': 0.20,
    'internal_docs': 0.20
}
DATE_RANGE = (datetime(2022, 1, 1), datetime(2024, 12, 31))
NEAR_DUP_RATE = 0.15
TOXIC_RATE = 0.08
NON_ENGLISH_RATE = 0.12
DOMAINS = {
    'news': 0.30,
    'blogs': 0.25,
    'forums': 0.20,
    'academic': 0.15,
    'other': 0.10
}

fake = Faker()
OUTPUT_DIR = "landing_zone"

def get_random_date():
    delta = DATE_RANGE[1] - DATE_RANGE[0]
    random_days = random.randrange(delta.days)
    return DATE_RANGE[0] + timedelta(days=random_days)

def generate_text(word_count):
    return " ".join(fake.words(nb=word_count))

def get_content_hash(text):
    return hashlib.sha256(text.encode()).hexdigest()

def perturb_text(text):
    """Simulate near-duplicates by adding minor noise."""
    words = text.split()
    if len(words) > 5:
        idx = random.randint(0, len(words) - 5)
        words[idx] = words[idx] + "_perturb"
    return " ".join(words)

def generate_web_crawl_batch(size):
    data = []
    for _ in range(size):
        # Log-normal distribution: mean 800 words
        word_count = int(np.random.lognormal(mean=np.log(800), sigma=0.5))
        text = generate_text(word_count)
        dt = get_random_date()
        domain_cat = np.random.choice(list(DOMAINS.keys()), p=list(DOMAINS.values()))
        data.append({
            'document_id': str(uuid.uuid4()),
            'url': f"https://{fake.domain_name()}/{fake.slug()}",
            'raw_html': f"<html><body>{text}</body></html>",
            'extracted_text': text,
            'crawl_timestamp': dt,
            'domain': domain_cat,
            'language_detected': np.random.choice(['en', 'es', 'fr', 'de'], p=[1-NON_ENGLISH_RATE, 0.05, 0.04, 0.03]),
            'word_count': word_count,
            'content_hash': get_content_hash(text),
            'date': dt.strftime('%Y-%m-%d')
        })
    return data

def generate_structured_db_batch(size):
    data = []
    for _ in range(size):
        # Uniform 100-500 words
        word_count = random.randint(100, 500)
        text = generate_text(word_count)
        dt = get_random_date()
        data.append({
            'record_id': str(uuid.uuid4()),
            'source_table': random.choice(['users_feedback', 'product_reviews', 'support_tickets']),
            'content_text': text,
            'metadata_json': '{"source": "legacy_crm", "version": "2.1"}',
            'created_at': dt,
            'data_category': random.choice(['feedback', 'review', 'technical']),
            'quality_flag': 1 if random.random() > TOXIC_RATE else 0,
            'date': dt.strftime('%Y-%m-%d')
        })
    return data

def generate_api_export_batch(size):
    data = []
    for _ in range(size):
        # Normal distribution mean 300 words
        word_count = max(50, int(np.random.normal(300, 50)))
        text = generate_text(word_count)
        dt = get_random_date()
        data.append({
            'item_id': str(uuid.uuid4()),
            'api_source': random.choice(['twitter_v2', 'reddit_api', 'github_events']),
            'title': fake.sentence(),
            'body_text': text,
            'tags': ",".join(fake.words(nb=3)),
            'published_at': dt,
            'author_id': str(uuid.uuid4()),
            'engagement_score': random.uniform(0, 100),
            'license_type': random.choice(['MIT', 'Apache-2.0', 'CC-BY-4.0']),
            'date': dt.strftime('%Y-%m-%d')
        })
    return data

def generate_internal_batch(size):
    data = []
    for _ in range(size):
        # Uniform 200-2000 words
        word_count = random.randint(200, 2000)
        text = generate_text(word_count)
        dt = get_random_date()
        data.append({
            'doc_id': str(uuid.uuid4()),
            'department': random.choice(['Engineering', 'Legal', 'HR', 'R&D']),
            'document_type': random.choice(['Whitepaper', 'Spec', 'Policy', 'Memo']),
            'content_text': text,
            'created_by': fake.name(),
            'created_at': dt,
            'sensitivity_level': random.choice(['Public', 'Internal', 'Confidential']),
            'file_format': 'PDF',
            'date': dt.strftime('%Y-%m-%d')
        })
    return data

def save_to_parquet(data, source_name):
    df = pd.DataFrame(data)
    os.makedirs(f"{OUTPUT_DIR}/{source_name}", exist_ok=True)
    table = pa.Table.from_pandas(df)
    pq.write_to_dataset(table, root_path=f"{OUTPUT_DIR}/{source_name}", partition_cols=['date'])

def main():
    print(f"🚀 Starting synthetic data generation: {TOTAL_DOCUMENTS} documents total.")
    batch_size = 10000
    
    sources = [
        ('web_crawl_data', generate_web_crawl_batch),
        ('structured_db_export', generate_structured_db_batch),
        ('api_export_data', generate_api_export_batch),
        ('internal_documents', generate_internal_batch)
    ]

    for name, generator in sources:
        count = int(TOTAL_DOCUMENTS * RATIOS[name.replace('_data', '').replace('_export', '').replace('_documents', 'web_crawl' if 'crawl' in name else 'structured_db' if 'db' in name else 'api_export' if 'api' in name else 'internal_docs')]) # Simplified mapping logic
        # Correctly map names to RATIOS keys
        ratio_key = 'web_crawl' if 'web_crawl' in name else \
                    'structured_db' if 'structured_db' in name else \
                    'api_export' if 'api_export' in name else 'internal_docs'
        count = int(TOTAL_DOCUMENTS * RATIOS[ratio_key])
        
        print(f"Generating {count} records for {name}...")
        
        num_batches = (count // batch_size) + 1
        for _ in tqdm(range(num_batches), desc=name):
            current_batch_size = min(batch_size, count)
            batch = generator(current_batch_size)
            
            # Near-duplicate simulation (15%)
            dup_count = int(current_batch_size * NEAR_DUP_RATE)
            if dup_count > 0:
                dupes = random.sample(batch, dup_count)
                for d in dupes:
                    d_copy = d.copy()
                    # Change ID so it's a "new" record but content is same/similar
                    first_key = [k for k in d_copy.keys() if 'id' in k][0]
                    d_copy[first_key] = str(uuid.uuid4())
                    
                    # Perturb text in 50% of duplicates to make them "near" duplicates
                    text_key = [k for k in d_copy.keys() if 'text' in k or 'raw_html' in k][0]
                    if random.random() > 0.5:
                         d_copy[text_key] = perturb_text(d_copy[text_key])
                    
                    batch.append(d_copy)
            
            save_to_parquet(batch, name)
            count -= current_batch_size
            if count <= 0: break

    print("✅ Data generation complete. Files saved to 'landing_zone/' partitioned by date.")

if __name__ == "__main__":
    main()
