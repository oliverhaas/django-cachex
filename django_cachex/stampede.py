"""Cache stampede prevention via XFetch (Vattani et al., VLDB 2015).

Keys are stored with TTL = ``timeout + buffer``. On read, the remaining
TTL drives a probabilistic early-recompute decision so a single client
refreshes the value before all clients see a miss simultaneously.
"""

from __future__ import annotations

import random
from dataclasses import dataclass


@dataclass(frozen=True, slots=True)
class StampedeConfig:
    # buffer: extra TTL seconds added to the stored expiry so reads can
    #   distinguish "logically expired" from "physically expired".
    # beta:   XFetch beta — higher values trigger recomputation earlier.
    # delta:  estimated recomputation time in seconds.
    buffer: int = 60
    beta: float = 1.0
    delta: float = 1.0


def should_recompute(ttl: int, config: StampedeConfig) -> bool:
    """Return True when the caller should recompute the value early."""
    remaining = ttl - config.buffer

    if remaining <= 0:
        return True

    if config.delta > 0:
        threshold = config.delta * config.beta * -random.expovariate(1.0)
        if remaining + threshold <= 0:
            return True

    return False
