import time
import uuid
from datetime import datetime
from pymongo import MongoClient, ReturnDocument

# MongoDB connection string (update this as needed)
MONGO_URI = "mongodb+srv://main_user:main_user1@cluster0.kssen.mongodb.net/?retryWrites=true&w=majority&appName=Cluster0"
DB_NAME = "scheduler_demo"
COLLECTION_NAME = "jobs"

# Configuration
BATCH_SIZE = 10
PROCESSING_TIME = 15  # seconds to simulate job processing

# Generate a UUID for this pod/worker instance
WORKER_ID = str(uuid.uuid4())

# Connect to MongoDB
client = MongoClient(MONGO_URI)
db = client[DB_NAME]
jobs = db[COLLECTION_NAME]

def lock_and_process_jobs():
    print(f"[{WORKER_ID}] Checking for jobs...")

    locked_jobs = []

    # Attempt to lock up to BATCH_SIZE jobs
    for _ in range(BATCH_SIZE):
        job = jobs.find_one_and_update(
            {"status": "pending"},
            {
                "$set": {
                    "status": "locked",
                    "locked_by": WORKER_ID,
                    "locked_at": datetime.utcnow()
                }
            },
            return_document=ReturnDocument.AFTER
        )

        if job:
            locked_jobs.append(job)
        else:
            break  # No more pending jobs to lock

    if not locked_jobs:
        print(f"[{WORKER_ID}] No jobs found.")
        return

    for job in locked_jobs:
        scheduler_id = job['scheduler_id']
        print(f"[{WORKER_ID}] Processing {scheduler_id} ({job['job_name']})...")
        time.sleep(PROCESSING_TIME)

        # Mark job as completed and release lock
        jobs.update_one(
            {"_id": job["_id"]},
            {
                "$set": {
                    "status": "pending"
                },
                "$unset": {
                    "locked_by": "",
                    "locked_at": ""
                }
            }
        )
        print(f"[{WORKER_ID}] Completed {scheduler_id}.")

def run():
    print(f"[{WORKER_ID}] Worker started.")
    while True:
        lock_and_process_jobs()
        time.sleep(60)

if __name__ == "__main__":
    run()
