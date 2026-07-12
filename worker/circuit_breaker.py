import logging
import time
from enum import Enum

logger = logging.getLogger(__name__)


class CircuitState(Enum):
    CLOSED = "closed"
    OPEN = "open"
    HALF_OPEN = "half_open"


# fmt: off
class CircuitBreaker:
    """Tracks PostgreSQL health. Opens after N consecutive connection failures.

    CLOSED    — every request goes to PostgreSQL
    OPEN      — skip PostgreSQL, use fallback
    HALF_OPEN — one probe request allowed; success → CLOSED, failure → OPEN
    """

    FAILURE_THRESHOLD = 3
    OPEN_TIMEOUT_S = 10.0

    def __init__(self) -> None:
        self._state = CircuitState.CLOSED
        self._failure_count = 0
        self._opened_at = 0.0
        self._half_open_used = False


    @property
    def state(self) -> CircuitState:
        return self._state


    def can_try_primary(self) -> bool:
        """Whether a request may attempt PostgreSQL
        CLOSED → always yes. OPEN → transition to HALF_OPEN if timeout elapsed.
        HALF_OPEN → yes once (probe), subsequent calls return False."""
        if self._state is CircuitState.CLOSED:
            return True

        if self._state is CircuitState.OPEN:
            self._try_transition_to_half_open()

        if self._state is CircuitState.HALF_OPEN and not self._half_open_used:
            self._half_open_used = True
            return True

        return False


    def record_success(self) -> None:
        """A PostgreSQL insert succeeded. Reset to CLOSED from any state."""
        if self._state is CircuitState.CLOSED:
            return
        self._transition_to_closed()


    def record_failure(self) -> None:
        """A DatabaseConnectionError occurred. Increment counter; open circuit at threshold."""
        self._failure_count += 1
        ready = self._failure_count >= self.FAILURE_THRESHOLD
        if ready and self._state is not CircuitState.OPEN:
            self._transition_to_open()


    def force_open(self) -> None:
        """Skip threshold — go straight to OPEN (used when PG is down at startup)."""
        self._transition_to_open()


    # ── transitions ──

    def _transition_to_closed(self) -> None:
        self._state = CircuitState.CLOSED
        self._failure_count = 0
        self._half_open_used = False
        logger.info("circuit closed | PostgreSQL healthy again")


    def _transition_to_open(self) -> None:
        self._state = CircuitState.OPEN
        self._opened_at = time.monotonic()
        self._half_open_used = False
        logger.warning("circuit opened | failures=%s", self._failure_count)


    def _try_transition_to_half_open(self) -> None:
        if time.monotonic() - self._opened_at < self.OPEN_TIMEOUT_S:
            return
        self._state = CircuitState.HALF_OPEN
        self._half_open_used = False
        logger.info("circuit half-open | probing PostgreSQL")
# fmt: on
