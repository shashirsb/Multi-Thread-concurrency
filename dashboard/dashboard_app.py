from flask import Flask, render_template
from pymongo import MongoClient
from collections import defaultdict, Counter

app = Flask(__name__)

MONGO_URI = "mongodb+srv://main_user:<password>@cluster0.kssen.mongodb.net/?retryWrites=true&w=majority&appName=Cluster0"
DB_NAME = "scheduler_demo"
COLLECTION_NAME = "jobs"

client = MongoClient(MONGO_URI)
db = client[DB_NAME]
jobs = db[COLLECTION_NAME]

@app.route('/')
def index():
    job_docs = list(jobs.find({}))

    # Stats
    total_jobs = len(job_docs)
    completed_jobs = sum(1 for job in job_docs if job.get("status") == "completed")
    locked_jobs = sum(1 for job in job_docs if job.get("status") == "locked")
    pending_jobs = sum(1 for job in job_docs if job.get("status") == "pending")

    # Group jobs by worker (locked_by)
    grouped = defaultdict(list)
    worker_counters = defaultdict(lambda: {"locked": 0, "completed": 0})
    
    for doc in job_docs:
        locked_by = doc.get("locked_by")
        status = doc.get("status")

        if locked_by:
            grouped[locked_by].append(doc)
            if status == "locked":
                worker_counters[locked_by]["locked"] += 1
            elif status == "completed":
                worker_counters[locked_by]["completed"] += 1

    # Detect overlapping scheduler_ids globally
    seen = {}
    overlaps = set()
    for doc in job_docs:
        sid = doc.get('scheduler_id')
        if sid in seen:
            overlaps.add(sid)
        else:
            seen[sid] = doc.get('locked_by')

    # Worker status: 'ok' or 'overlap'
    worker_status = {}
    for worker_id, jobs_list in grouped.items():
        worker_overlap = any(job['scheduler_id'] in overlaps for job in jobs_list)
        worker_status[worker_id] = 'overlap' if worker_overlap else 'ok'

    return render_template(
        "dashboard.html",
        total_jobs=total_jobs,
        locked_jobs=locked_jobs,
        completed_jobs=completed_jobs,
        pending_jobs=pending_jobs,
        grouped=grouped,
        overlaps=overlaps,
        worker_status=worker_status,
        worker_counters=worker_counters
    )

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5001)
