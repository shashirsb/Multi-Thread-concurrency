import time
import uuid
from datetime import datetime, timedelta
from pymongo import MongoClient
from pymongo.errors import PyMongoError

# Constants
MONGO_URI = "mongodb+srv://main_user:<password>@cluster0.kssen.mongodb.net/?retryWrites=true&w=majority&appName=Cluster0"
DB_NAME = "scheduler_demo"
COLLECTION_NAME = "jobs"
PROCESSING_TIME = 1  # seconds to simulate job processing
LOCK_EXPIRATION_MINUTES = 15  # Retry jobs locked but not processed in this time

# Unique worker ID
WORKER_ID = str(uuid.uuid4())

# MongoDB setup
client = MongoClient(MONGO_URI)
db = client[DB_NAME]
jobs = db[COLLECTION_NAME]


def take_over_stale_locks():
    # """Reassign stale or null-locked jobs to this worker for processing."""
    try:
        now = datetime.utcnow()
        stale_threshold = now - timedelta(minutes=LOCK_EXPIRATION_MINUTES)

        # Match either stale locked jobs or ones with null locked_at
        result = jobs.update_many(
            {
                "status": "locked",
                "$or": [
                    {"locked_at": {"$lt": stale_threshold}},
                    {"locked_at": None}
                ]
            },
            {
                "$set": {
                    "locked_by": WORKER_ID,
                    "locked_at": now
                }
            }
        )

        if result.modified_count > 0:
            print(f"[{WORKER_ID}] Took over {result.modified_count} stale or null-locked jobs.")
            locked_jobs = list(jobs.find({
                "status": "locked",
                "locked_by": WORKER_ID
            }))
            return locked_jobs
        else:
            print(f"[{WORKER_ID}] No stale or null-locked jobs found.")
            return []

    except PyMongoError as e:
        print(f"[{WORKER_ID}] Error taking over stale locks: {e}")
        return []



def lock_scheduled_jobs():
    # """Find and lock eligible pending jobs based on trigger time."""
    try:
        now = datetime.utcnow()
        window_start = now - timedelta(minutes=30)

        # Step 1: Find eligible pending jobs in the time window
        eligible_jobs = list(jobs.find({
            "status": "pending",
            "trigger_time": {"$gte": window_start, "$lte": now}
        }))

        if not eligible_jobs:
            print(f"[{WORKER_ID}] No eligible jobs found to lock.")
            return []

        job_ids = [job["_id"] for job in eligible_jobs]

        # Step 2: Lock the eligible jobs
        result = jobs.update_many(
            {
                "_id": {"$in": job_ids},
                "status": "pending"
            },
            {
                "$set": {
                    "status": "locked",
                    "locked_by": WORKER_ID,
                    "locked_at": now
                }
            }
        )

        if result.modified_count == 0:
            print(f"[{WORKER_ID}] No jobs locked — possibly already locked by another worker.")
            return []

        print(f"[{WORKER_ID}] Locked {result.modified_count} jobs.")

        # Step 3: Fetch only the jobs this worker has locked
        locked_jobs = list(jobs.find({
            "status": "locked",
            "locked_by": WORKER_ID
        }))

        return locked_jobs

    except PyMongoError as e:
        print(f"[{WORKER_ID}] MongoDB error during locking: {e}")
        return []


def process_jobs(jobs_to_process):
    # """Simulate job processing and mark them as completed."""
    for job in jobs_to_process:
        scheduler_id = job.get("scheduler_id")
        job_name = job.get("job_name")
        print(f"[{WORKER_ID}] Processing job {scheduler_id} ({job_name})...")
        time.sleep(PROCESSING_TIME)

        # Mark job as completed
        update_result = jobs.update_one(
            {"_id": job["_id"], "locked_by": WORKER_ID},
            {"$set": {"status": "completed"}}
        )

        if update_result.modified_count == 1:
            print(f"[{WORKER_ID}] ✅ Completed job {scheduler_id}")
        else:
            print(f"[{WORKER_ID}] ⚠️ Could not update job {scheduler_id} — maybe already processed or taken.")


def run():
    print(f"[{WORKER_ID}] Worker started.")
    while True:
        # Step 1: Try to take over stale jobs
        locked_jobs = take_over_stale_locks()

        # Step 2: If none, try to lock fresh jobs
        if not locked_jobs:
            locked_jobs = lock_scheduled_jobs()

        # Step 3: Process any jobs we got
        if locked_jobs:
            process_jobs(locked_jobs)
        else:
            print(f"[{WORKER_ID}] No jobs locked. Sleeping...")

        # Step 4: Sleep before next cycle
        time.sleep(30)


if __name__ == "__main__":
    run()
