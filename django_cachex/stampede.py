"""Cache stampede prevention via XFetch probabilistic early expiration.

Implements the XFetch algorithm (Vattani et al., VLDB 2015) for optimal
cache stampede prevention. Instead of wrapping values, this uses Redis's
native TTL: keys are stored with an extended TTL (timeout + buffer), and
on read the remaining TTL is checked to decide whether to trigger early
recomputation.
"""

from __future__ import annotations

import random
from dataclasses import dataclass


@dataclass(frozen=True, slots=True)
class StampedeConfig:
    """Configuration for stampede prevention."""

    buffer: int = 60  # extra TTL seconds added to Redis expiry
    beta: float = 1.0  # XFetch beta parameter (higher = earlier recompute)
    delta: float = 1.0  # estimated recomputation time in seconds


def should_recompute(ttl: int, config: StampedeConfig) -> bool:
    """Decide whether to trigger early recomputation using the XFetch algorithm.

    Args:
        ttl: Remaining TTL in seconds from Redis (TTL command result).
        config: Stampede prevention configuration.

    Returns:
        True if the caller should recompute the value.
    """
    remaining = ttl - config.buffer

    if remaining <= 0:
        return True

    if config.delta > 0:
        threshold = config.delta * config.beta * -random.expovariate(1.0)
        if remaining + threshold <= 0:
            return True

    return False
