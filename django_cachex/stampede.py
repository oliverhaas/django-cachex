"""Cache stampede prevention via XFetch (Vattani et al., VLDB 2015).

Keys are stored with TTL = ``timeout + buffer``. On read, the remaining
TTL drives a probabilistic early-recompute decision so a single client
refreshes the value before all clients see a miss simultaneously.
"""

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


def resolve_stampede(
    instance_config: StampedeConfig | None,
    override: bool | dict | None = None,
) -> StampedeConfig | None:
    """Merge instance + per-call stampede config into the effective one.

    Merge order: ``StampedeConfig`` defaults → instance config → per-call override.

    Args:
        instance_config: The adapter's configured stampede policy, or ``None``
            if stampede prevention is disabled at the adapter level.
        override: Per-call override. ``None`` = use instance default,
            ``True`` = force on (using instance or default config),
            ``False`` = force off,
            ``dict`` = force on with specific overrides (e.g. ``{"delta": 2.0}``).
    """
    if override is None:
        return instance_config
    if override is False:
        return None
    base = instance_config or StampedeConfig()
    if isinstance(override, dict):
        return StampedeConfig(
            buffer=override.get("buffer", base.buffer),
            beta=override.get("beta", base.beta),
            delta=override.get("delta", base.delta),
        )
    return base


def get_timeout_with_buffer(
    timeout: int | None,
    instance_config: StampedeConfig | None,
    override: bool | dict | None = None,
) -> int | None:
    """Add the stampede buffer to ``timeout``.

    Returns ``timeout`` unchanged if stampede prevention is disabled, the
    timeout is ``None``, or non-positive.
    """
    config = resolve_stampede(instance_config, override)
    if not config or not timeout or timeout <= 0:
        return timeout
    return timeout + config.buffer


def make_stampede_config(option: bool | dict | None) -> StampedeConfig | None:
    """Build a ``StampedeConfig`` from an adapter ``stampede_prevention`` option."""
    if not option:
        return None
    if isinstance(option, dict):
        return StampedeConfig(**option)
    return StampedeConfig()
