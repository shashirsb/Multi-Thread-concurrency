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
