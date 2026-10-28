"""Config defaults and the Decision type returned by the limiter."""
from __future__ import annotations

from dataclasses import asdict, dataclass

DEFAULT_CONFIG = {"mode": "token_bucket", "rate": 5.0, "burst": 10, "limit": 10, "window": 1.0}


@dataclass
class Decision:
    allowed: bool
    limit: int
    remaining: float
    retry_after: float  # seconds until a request could succeed (only meaningful on DENY)
    reset: float        # seconds until the full limit is available again
    mode: str
    server_time: float
    seq: int = 0        # this client's decision number: the exact execution order

    def headers(self) -> dict[str, str]:
        h = {
            "X-RateLimit-Limit": str(self.limit),
            "X-RateLimit-Remaining": str(int(self.remaining)),
            "X-RateLimit-Reset": f"{self.reset:.3f}",
            "X-RateLimit-Mode": self.mode,
        }
        if not self.allowed:
            h["Retry-After"] = f"{self.retry_after:.3f}"
        return h

    def to_dict(self) -> dict:
        d = asdict(self)
        d["decision"] = "ALLOW" if self.allowed else "DENY"
        return d
