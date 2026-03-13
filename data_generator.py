import json
import random
import time
import boto3
from datetime import datetime, timedelta

# CONFIGURATION
BUCKET_NAME = "nexus-fintech-data-lake-jose" 
REGION = "us-east-2"



CURRENT_MODE = "healthy" 

class NexusDataGenerator:
    def __init__(self, mode="healthy"):
        self.mode = mode

    def generate_transaction(self, specific_date):
       
        tx_id = f"TX-{random.randint(1000, 9999)}"
        amount = random.uniform(10.0, 5000.0)
        
        if self.mode == "chaos":
            error_type = random.choice(['negative', 'duplicate', 'null_id'])
            if error_type == 'negative': amount = -100.0
            elif error_type == 'duplicate': tx_id = "TX-9999-DUPLICATE"
            elif error_type == 'null_id': tx_id = None

        return {
            "transaction_id": tx_id,
            "user_id": f"USER-{random.randint(1, 500)}",
            "amount": round(amount, 2),
            "currency": "USD",
            "status": random.choice(["completed", "completed", "failed"]),
            "method": random.choice(["STRIPE", "BANK_TRANSFER"]),
            "event_timestamp": specific_date.isoformat()
        }

    def upload_to_s3(self, filename, content):
        
        try:
            s3 = boto3.resource('s3')
            s3.Object(BUCKET_NAME, f"raw/transactions/{filename}").put(Body=json.dumps(content))
            print(f"🚀 {filename} uploaded | Mode: {self.mode.upper()}")
        except Exception as e:
            print(f"❌ S3 Error: {e}")

if __name__ == "__main__":
    print(f"--- STARTING IN {CURRENT_MODE.upper()} MODE ---")

 
    backfill_gen = NexusDataGenerator(mode="chaos" if CURRENT_MODE == "chaos" else "healthy")
    
    for i in range(10, -1, -1):
        past_date = datetime.now() - timedelta(days=i)
        batch = [backfill_gen.generate_transaction(past_date) for _ in range(random.randint(30, 60))]
        backfill_gen.upload_to_s3(f"transactions_{past_date.strftime('%Y-%m-%d')}.json", batch)

  
    if CURRENT_MODE in ["mix", "chaos"]:
        for i in range(1, 4):
            # In 'mix', only the 3rd batch triggers the chaos logic in generate_transaction
            sim_mode = "chaos" if (CURRENT_MODE == "chaos" or i == 3) else "healthy"
            sim_gen = NexusDataGenerator(mode=sim_mode)
            sim_batch = [sim_gen.generate_transaction(datetime.now()) for _ in range(10)]
            sim_gen.upload_to_s3(f"sim_batch_{i}_{sim_mode}.json", sim_batch)
            time.sleep(1)
