import json
import random
import time
import boto3
from datetime import datetime

# CONFIGURATION
BUCKET_NAME = "nexus-fintech-data-lake-jose" 
REGION = "us-east-2"

class NexusDataGenerator:
    def __init__(self, mode="healthy"):
        self.mode = mode # "healthy" or "chaos"

    def generate_transaction(self):
        tx_id = f"TX-{random.randint(1000, 9999)}"
        amount = random.uniform(10.0, 5000.0)
        
        # --- CIRCUIT BREAKER LOGIC (CHAOS MODE) ---
        if self.mode == "chaos":
            error_type = random.choice(['negative', 'duplicate', 'null_id'])
            if error_type == 'negative':
                amount = -100.0  # Critical financial error
            elif error_type == 'duplicate':
                tx_id = "TX-9999-DUPLICATE" # Will trigger dbt unique test failure
            elif error_type == 'null_id':
                tx_id = None # Will trigger dbt not_null test failure


            return {
                "transaction_id": tx_id,
                "user_id": f"USER-{random.randint(1, 500)}",
                "amount": round(amount, 2),
                "currency": "USD",
                "status": random.choice(["completed", "completed", "failed"]), # <--- AGREGA ESTO
                "method": random.choice(["STRIPE", "BANK_TRANSFER"]),
                "event_timestamp": datetime.now().isoformat()
            }

def upload_to_s3(filename, content, current_mode):
    try:
        s3 = boto3.resource('s3')
        s3.Object(BUCKET_NAME, f"raw/transactions/{filename}").put(Body=json.dumps(content))
        print(f"🚀 File {filename} successfully uploaded to S3. Mode: [{current_mode.upper()}]")
    except Exception as e:
        print(f"❌ Error uploading to S3: {e}")

if __name__ == "__main__":
    # SIMULATION SEQUENCE
    # Batch 1 & 2: Healthy data | Batch 3: Chaos/Corrupted data
    for i in range(1, 4):
        current_mode = "healthy" if i < 3 else "chaos"
        generator = NexusDataGenerator(mode=current_mode)
        
        batch = [generator.generate_transaction() for _ in range(10)]
        file_name = f"batch_{i}_{current_mode}.json"
        
        upload_to_s3(file_name, batch, current_mode)
        
        if i < 3:
            print(f"Waiting for the next ingestion cycle...")
            time.sleep(10)
    print("--- Ingestion Process Completed") 
     
