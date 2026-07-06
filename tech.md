1. **uv**
   - Modern tool for Python packaging (10–100x faster than pip)
   - Handles package installation, virtual environments, dependency locking (`uv.lock`), project management (`pyproject.toml`), fast installs (Rust-based), Python version management, and environment syncing
   - Replaces: `pip + venv + pip-tools + pipenv + pyenv`

2. **Typer**
   - Useful for building CLI tools and developer utilities
   - Converts standard Python functions into CLI commands with minimal boilerplate
   - Uses Python type hints as the source of truth
   - Built on top of Click; supports colors, prompts, dialogs, progress bars, and spinners

3. **W3C `traceparent` logic**
   - Instead of `X-Trace-ID`, use the W3C standard `traceparent` header
   - The request carries the header in the following format:

     ```text
     traceparent: 00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01
     ```

     - `version`: Trace context version (e.g., `00`)
     - `trace-id`: Unique identifier for the distributed trace
     - `parent-id`: Identifier of the immediate parent span
     - `trace-flags`: Sampling flags (e.g., `00` or `01`)

4. **Redis Streams**

   - One of the data structures inside Redis (like Lists, Sets, Hashes). It is an **append-only event log** that can be used as a **reliable message queue**.
   - Producers append events using `XADD`.
   - ACK from a worker **does not delete** the message from the stream. It only marks the message as processed for that **Consumer Group** by removing it from that group's Pending Entries List (PEL).
   - Consumer Groups - **metadata maintained by Redis** -- coordinate workers within the group so that **each message is delivered to only one worker in that group**.
   - Each group has its own: Offset, Pending Entries List (PEL), ACK state
   - **PEL (Pending Entries List)** tracks messages that have been delivered to a consumer but **not yet acknowledged**. If a worker crashes, another worker can reclaim and process those pending messages.

   - Consumer Groups are created once:
      ```python
      redis.xgroup_create(
            "orders-stream",
            "email-group",
            id="0"
      )
      ```

   - Workers continuously consume messages:
      ```python
      while True:
            messages = redis.xreadgroup(...)
            process(messages)
            redis.xack(...)
      ```

   - AOF (Append Only File) - one of Redis's persistence mechanism - logs every write command (like XADD) and replays them on restart (create consumer groups, consumer group state, PEL, every consumer group metadata)
   - `appendfsync` controls how often redis flushes AOF file to disk
   - `appendfsync everysec` - default, flush once per second, may lose atmost 1 second of writes on crash

   - If a worker crashes after writing to the DB but before ACKing, another worker may process the same message again.  
   - Other messaging systems like Kafka (designed for very large scale distributed event streaming, millions of events/sec, supports long-term retention)