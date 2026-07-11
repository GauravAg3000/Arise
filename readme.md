# ARISE

This project is combination of 2 components:-

## 1. Resilient Ingestion Engine

- The server will accepts bulk events without crashing/ losing packets when load spikes.
- **Buffer Zone** → server will put incoming events into queue immediately.
- **Fast Acknowledgement** → respond to client "Got it!" instantly, rather than making it wait for db writes.
- **Throttling** → if server gets overwhelmed, it slows down incoming traffic gracefully instead of a crash.

## 2. Self-Healing Telemetry Pipeline

- **Database Outage** → If db goes down, pipeline automatically reroutes new bulk events to our backup db.
- **Active-Passive (Read only Backup)** → backup db only holds data temporarily. When primary db arises, pipeline sync data back to primary db.
- **Bad data isolation** → if a malformed event threatens to break the pipeline, system will dump it into a DLQ. Keeps processing good data.

---

# Notes

- An ingestion API in front of a durable queue is the standard architecture for high-throughput telemetry systems.