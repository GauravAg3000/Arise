ARISE - A Resilient Ingestion Engine & Self-Healing Telemetry Pipeline


Objective: Building a fault-tolerant, high-throughput telemetry ingestion platform capable of reliably accepting, buffering, processing, and recovering event data under failures.

1. Why are we building this?

Modern systems generate millions of events every day: The primary challenge is not generating events—it is ensuring they are never lost, even when downstream systems become slow or unavailable.

This project simulates the core architecture behind systems like PostHog, Datadog, Mixpanel, and internal telemetry platforms used by large tech companies.

The project is designed to deepen understanding of:
Distributed systems
Asynchronous processing
Fault tolerance
Backpressure handling
Observability
Data durability
Production-grade backend architecture

2. Goals

Accept telemetry events from multiple clients
Buffer events asynchronously
Persist events efficiently into PostgreSQL
Prevent data loss during failures
Automatically recover failed events

Non-Functional Goals
High throughput
Low ingestion latency
Reliable event delivery
Graceful degradation during failures

3. High-Level Architecture
CLI Agent
    │
    ▼
FastAPI Gateway
    │
    ▼
Redis Streams
    │
    ▼
Worker Pool
    │
    ▼
PostgreSQL

Failure Path

PostgreSQL Down
        │
        ▼
Circuit Breaker Opens
        │
        ▼
MongoDB Fallback Store
        │
        ▼
Healer Service
        │
        ▼
Replay into PostgreSQL

4. Technology Stack
Component	Technology
Client	Python CLI
API Gateway	FastAPI	High-performance async HTTP server
Queue	Redis Streams	Persistent append-only stream with consumer groups
Primary Database	PostgreSQL	Reliable relational storage with efficient bulk inserts
Fallback Store	MongoDB	Durable document storage during database outages
Background Workers	Python asyncio	Concurrent event processing
Containerization	Docker & Docker Compose	Local multi-service environment
Load Testing	k6 / Locust	Throughput and latency benchmarking

5. Architecture Decisions (ADR)
ADR-001: Decouple ingestion from persistence

Database writes are significantly slower than accepting HTTP requests.
Direct database writes increase latency and reduce system resilience.

Decision

Introduce an asynchronous ingestion pipeline using Redis Streams.

Lower API latency
Better throughput
Independent scaling of API and workers

ADR-002: Use Redis Streams instead of in-memory queues

In-memory queues lose data when processes restart.

Decision

Use Redis Streams with AOF persistence enabled.

Durable buffering
Consumer groups
Replay capability
Better failure recovery

ADR-003: Worker Pool Architecture

Database writes are expensive.

Decision

Dedicated background workers perform bulk database inserts.

Higher throughput
Better database utilization
Easier scaling

ADR-004: Circuit Breaker for Database Failures

Repeated connection attempts during outages waste resources.

Decision

Workers stop attempting PostgreSQL writes after repeated failures and redirect data to the fallback store.

Consequences

Pros

Faster failure detection
Stable system during outages
Reduced cascading failures

ADR-005: MongoDB as Temporary Fallback Store

Redis Streams should not become long-term storage during prolonged database outages.

Decision

Temporarily persist failed events into MongoDB until PostgreSQL recovers.

Prevents Redis memory exhaustion
Durable storage
Enables replay

ADR-006: Idempotent Event Processing

Workers may process the same event multiple times due to retries.

Decision

Each event receives a globally unique event_id.

PostgreSQL enforces uniqueness using ON CONFLICT DO NOTHING.

Duplicate processing becomes safe.

ADR-007: Client-side Batching

Sending every event individually creates unnecessary network overhead.

Decision

CLI batches events using configurable size and time thresholds.

Lower network overhead and higher throughput.

ADR-008: Backpressure

The system must remain stable when downstream processing slows.

Decision

Gateway monitors Redis Stream length.

If queue size exceeds a threshold:

Return HTTP 429
Include Retry-After
Client slows event generation

6. Planned Reliability Features

The following features will be implemented after the core pipeline is functional.

Graceful shutdown
Exponential backoff with jitter
Poison message handling
Partial batch failure recovery
Schema versioning
Distributed tracing
Connection pooling
Worker scaling
Queue lag metrics
Liveness & readiness probes
Configuration management
Security basics
Chaos testing
Service Level Objectives (SLOs)
Event lifecycle tracking

7. Success Criteria

The project will be considered successful if it can:

Sustain high event throughput under load
Continue accepting events during PostgreSQL outages
Recover all buffered events after database restoration
Avoid duplicate event persistence
Demonstrate backpressure handling
Provide meaningful operational metrics
Recover gracefully from simulated failures