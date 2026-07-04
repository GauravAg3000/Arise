import re

_DURATION_RE = re.compile(r"^(\d+(?:\.\d+)?)(ms|s|m|h)$")
_DURATION_MULTIPLIERS = {"ms": 0.001, "s": 1, "m": 60, "h": 3600}


def parse_duration(s: str) -> float:
    match = _DURATION_RE.match(s)
    if not match:
        raise ValueError(f"invalid duration: {s}")
    value = float(match.group(1))
    unit = match.group(2)
    return value * _DURATION_MULTIPLIERS[unit]
