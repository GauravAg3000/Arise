import logging
import time
from enum import Enum

logger = logging.getLogger(__name__)


class CircuitState(Enum):
    CLOSED = "closed"
    OPEN = "open"
    HALF_OPEN = "half_open"


class CircuitBreaker:
    """Tracks PostgreSQL health. Opens after N consecutive connection failures.

    State machine mirrors Resilience4j
    CLOSED - Every request goes to primary DB PostgreSQL.
    OPEN - No requests sent to PostgreSQL, the application should immediately use fallback DB
    HALF_OPEN - After waiting for a while...One request is allowed
        if "succeeds" =>  HALF_OPEN -> CLOSED
        if "fails"    =>  HALF_OPEN -> OPEN
    """

    # will try 3 times after failing to connect to Postgres
    FAILURE_THRESHOLD = 3

    # Wait 10 seconds and then OPEN -> HALF_OPEN
    OPEN_TIMEOUT_S = 10.0

    def __init__(self) -> None:
        self._state = CircuitState.CLOSED
        self._failure_count = 0
        self._opened_at = 0.0
        self._half_open_used = False

    @property
    def state(self) -> CircuitState:
        return self._state

    @property
    def failure_count(self) -> int:
        return self._failure_count

    def can_try_primary(self) -> bool:
        """Whether a request may attempt PostgreSQL.

        Returns True for:
        - CLOSED: always
        - HALF_OPEN: if the single probe slot hasn't been consumed yet
        - OPEN: if OPEN_TIMEOUT has elapsed (transitions to HALF_OPEN)

        Returns False otherwise — caller should use fallback.
        """
        if self._state is CircuitState.CLOSED:
            return True

        if self._state is CircuitState.OPEN and self._should_transition_to_half_open():
            self._state = CircuitState.HALF_OPEN
            self._half_open_used = False
            logger.info("circuit half-open | probing PostgreSQL")

        if self._state is CircuitState.HALF_OPEN and not self._half_open_used:
            self._half_open_used = True
            return True

        return False

    def record_success(self) -> None:
        """A PostgreSQL insert succeeded. Reset to CLOSED."""
        previous = self._state
        self._state = CircuitState.CLOSED
        self._failure_count = 0
        self._half_open_used = False
        if previous is not CircuitState.CLOSED:
            logger.info("circuit closed | PostgreSQL healthy again")

    def record_failure(self) -> None:
        """A DatabaseConnectionError occurred. Increment failure counter."""
        self._failure_count += 1
        if self._failure_count >= self.FAILURE_THRESHOLD and self._state is not CircuitState.OPEN:
            self._state = CircuitState.OPEN
            self._opened_at = time.monotonic()
            self._half_open_used = False
            logger.warning(
                "circuit opened | failures=%s threshold=%s",
                self._failure_count,
                self.FAILURE_THRESHOLD,
            )

    def _should_transition_to_half_open(self) -> bool:
        return time.monotonic() - self._opened_at >= self.OPEN_TIMEOUT_S
