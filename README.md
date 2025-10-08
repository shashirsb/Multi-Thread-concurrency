# 🛠️ MongoDB Job Scheduler Worker

This project is a **MongoDB-backed job scheduler worker**, built in Python, designed for distributed systems. It automatically picks up pending jobs, processes them, and handles stale or orphaned jobs safely.

---

## 📌 Features

- 🔐 **Distributed job locking** with `locked_by` and `locked_at`
- ♻️ **Automatic takeover** of stale or unassigned locked jobs
- ⏱️ **Scheduled job pickup** based on `trigger_time`
- ✅ **Job completion** tracking
- 🧪 Simulates job processing using `time.sleep()`
- 🧱 Built on **MongoDB** with `pymongo`

---

## 🧰 Tech Stack

- **Python 3**
- **MongoDB Atlas / MongoDB Cluster**
- **pymongo**

---

## 🗃️ Job Document Schema

Each job in the MongoDB collection should look like this:

```json
{
  "_id": ObjectId,
  "scheduler_id": "uuid-string",
  "job_name": "Job_1",
  "status": "pending",         // or "locked", "completed"
  "created_at": ISODate,
  "trigger_time": ISODate,
  "locked_by": "uuid-string or null",
  "locked_at": ISODate or null,
  "payload": { ... }           // optional data
}



🚀 Getting Started
1. Clone the Repo
git clone https://github.com/your-org/your-repo-name.git
cd your-repo-name
2. Install Dependencies
pip install pymongo
3. Configure MongoDB
Edit the connection URI and DB/collection names in job_scheduler_worker.py:
MONGO_URI = "mongodb+srv://<username>:<password>@<your-cluster-url>/?retryWrites=true&w=majority"
DB_NAME = "scheduler_demo"
COLLECTION_NAME = "jobs"
4. Run the Worker
python job_scheduler_worker.py
🔄 How It Works
The worker runs in a continuous loop:
Takes over stale jobs:
If a job is locked and either:
locked_at is more than 15 minutes old
locked_at is null
Locks new pending jobs:
If status: "pending" and trigger_time is within the past 30 minutes
Processes jobs:
Simulates work using time.sleep(PROCESSING_TIME)
Then marks the job as completed
Repeats every 30 seconds
🧠 Logic Overview
# Main loop
while True:
    take_over_stale_locks()
    lock_scheduled_jobs()
    process_jobs()
    time.sleep(30)
🔧 Configuration
All configs can be edited at the top of job_scheduler_worker.py:
PROCESSING_TIME = 5  # seconds
LOCK_EXPIRATION_MINUTES = 15
🧹 Tips for Maintenance
✅ Ensure indexes on: status, trigger_time, locked_at
✅ Use monitoring to track stuck or failed jobs
✅ Log locked_by for traceability
🐳 Docker (Optional)
If you want to dockerize it, here's a simple Dockerfile to get started:
FROM python:3.11-slim
WORKDIR /app
COPY job_scheduler_worker.py .
RUN pip install pymongo
CMD ["python", "job_scheduler_worker.py"]
📄 License
This project is licensed under the MIT License.
🤝 Contributing
PRs are welcome! For major changes, open an issue first to discuss what you’d like to change.
📬 Contact
Maintained by [Your Name / Your Team].
For questions, please open an issue or contact your dev team.

