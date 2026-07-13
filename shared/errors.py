class DatabaseConnectionError(Exception):
    """PostgreSQL unreachable or connection failed. Circuit breaker opens."""


class InvalidDataError(Exception):
    """Event data violates database constraints. Sent to DLQ."""
