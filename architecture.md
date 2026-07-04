## Phase 1 — Events Generation (Producer)

```mermaid
flowchart LR

    Developer["Developer / System"]

    CLI["CLI Agent"]

    Batch["Client-side Batch Buffer"]

    Developer --> CLI
    CLI --> Batch
```

Generate telemetry and batch events before sending.

---

## Phase 2 — Ingestion Gateway

```mermaid
flowchart LR

    CLI["CLI Agent"]

    Gateway["FastAPI Gateway"]

    Validate["Pydantic Validation"]

    Metadata["Attach Metadata\n(request_id, trace_id, received_at)"]

    CLI --> Gateway
    Gateway --> Validate
    Validate --> Metadata
```

Gateway only validates - Nothing else

---

## Phase 3 — Decoupling (Gateway and DB) Using Redis Streams

```mermaid
flowchart LR

    CLI["CLI Agent"]

    Gateway["FastAPI Gateway"]

    Redis["Redis Streams"]

    CLI --> Gateway
    Gateway --> Redis
```

Instead of waiting for PostgreSQL, the Gateway immediately returns **202 Accepted**.

---

## Phase 4 — Worker Pool

```mermaid
flowchart LR

    Redis["Redis Streams"]

    W1["Worker 1"]

    W2["Worker 2"]

    W3["Worker N"]

    Postgres["PostgreSQL"]

    Redis --> W1
    Redis --> W2
    Redis --> W3

    W1 --> Postgres
    W2 --> Postgres
    W3 --> Postgres
```

Each worker - reads, processes, batch inserts, acknowledges Redis

---

## Phase 5 — Baseline Architecture

```mermaid
flowchart LR

    CLI["CLI Agent"]

    Gateway["FastAPI Gateway"]

    Redis["Redis Streams"]

    Workers["Worker Pool"]

    Postgres["PostgreSQL"]

    CLI --> Gateway
    Gateway --> Redis
    Redis --> Workers
    Workers --> Postgres
```

---

## Phase 6 — Failure Handling

```mermaid
flowchart LR

    Redis["Redis Streams"]

    Workers["Worker Pool"]

    Postgres["PostgreSQL"]

    Mongo["MongoDB Fallback Store"]

    Redis --> Workers
    Workers --> Postgres

    Postgres -.Unavailable.-> Workers

    Workers --> Mongo
```

Workers decide where data goes - Not Gateway

---

## Phase 7 — Recovery

```mermaid
flowchart LR

    Mongo["MongoDB"]

    Replay["Replay / Healer Service"]

    Postgres["PostgreSQL"]

    Mongo --> Replay
    Replay --> Postgres
```

Replay only starts when PostgreSQL becomes healthy.

---

## Phase 8 — Complete Architecture

```mermaid
flowchart LR

    CLI["CLI Agent"]

    Gateway["FastAPI Gateway"]

    Redis["Redis Streams"]

    Workers["Worker Pool"]

    Postgres["PostgreSQL"]

    Mongo["MongoDB Fallback"]

    Replay["Replay Service"]

    CLI --> Gateway
    Gateway --> Redis
    Redis --> Workers
    Workers --> Postgres

    Postgres -.Down.-> Workers

    Workers --> Mongo
    Mongo --> Replay
    Replay --> Postgres
```