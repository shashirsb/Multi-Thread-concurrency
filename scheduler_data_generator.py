import pymongo
from pymongo import MongoClient
from datetime import datetime, timedelta
import uuid
import random

# MongoDB connection
MONGO_URI = "mongodb+srv://main_user:main_user1@cluster0.kssen.mongodb.net/?retryWrites=true&w=majority&appName=Cluster0"
DB_NAME = "scheduler_demo"
COLLECTION_NAME = "jobs"

NUM_JOBS = 10000  # Number of synthetic jobs to generate

def generate_jobs():
    client = MongoClient(MONGO_URI)
    db = client[DB_NAME]
    jobs = db[COLLECTION_NAME]

    # Clean the collection
    jobs.drop()

    now = datetime.utcnow()
    job_list = []

    for i in range(NUM_JOBS):
        # Random trigger_time in the next 0 to 20 minutes
        trigger_offset = random.randint(0, 60)
        trigger_time = now + timedelta(minutes=trigger_offset)

        job = {
            "scheduler_id": str(uuid.uuid4()),
            "job_name": f"Job_{i + 1}",
            "status": "pending",  # pending, locked, completed
            "created_at": now,
            "trigger_time": trigger_time,
            "payload": {
                "type": random.choice(["email", "backup", "report"]),
                "priority": random.randint(1, 5)
            },
            "locked_by": None,
            "locked_at": None
        }
        job_list.append(job)

    result = jobs.insert_many(job_list)
    print(f"✅ Inserted {len(result.inserted_ids)} future-scheduled jobs into MongoDB.")

if __name__ == "__main__":
    generate_jobs()
