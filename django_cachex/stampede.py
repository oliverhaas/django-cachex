"""Cache stampede prevention via XFetch (Vattani et al., VLDB 2015).

Keys are stored with TTL = ``timeout + buffer``. On read, the remaining
TTL drives a probabilistic early-recompute decision so a single client
refreshes the value before all clients see a miss simultaneously.
"""

import logging
import random
from dataclasses import dataclass

logger = logging.getLogger(__name__)


@dataclass(frozen=True, slots=True)
class StampedeConfig:
    # buffer: extra TTL seconds added to the stored expiry so reads can
    #   distinguish "logically expired" from "physically expired".
    # beta:   XFetch beta. Higher values trigger recomputation earlier.
    # delta:  estimated recomputation time in seconds.
    buffer: int = 60
    beta: float = 1.0
    delta: float = 1.0


def should_recompute(ttl: int, config: StampedeConfig) -> bool:
    """Return True when the caller should recompute the value early.

    Redis ``TTL`` sentinels are filtered up front: ``-1`` (key has no
    expire) and ``-2`` (key absent) would otherwise both flip to
    "recompute now" because ``ttl - buffer`` is unconditionally
    negative. ``ttl == 0`` (about-to-expire) still triggers; that's
    the whole point of the ``buffer``.
    """
    if ttl < 0:
        return False
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
    override: bool | StampedeConfig | None = None,
) -> StampedeConfig | None:
    """Merge instance + per-call stampede config into the effective one.

    Args:
        instance_config: The adapter's configured stampede policy, or ``None``
            if stampede prevention is disabled at the adapter level.
        override: Per-call override. ``None`` = use instance default,
            ``True`` = force on (using instance or default config),
            ``False`` = force off,
            ``StampedeConfig`` = force on with the supplied policy.
    """
    if override is None:
        return instance_config
    if override is False:
        return None
    if override is True:
        return instance_config or StampedeConfig()
    if isinstance(override, StampedeConfig):
        return override
    msg = f"stampede_prevention override must be bool | StampedeConfig | None, got {type(override).__name__}"
    raise TypeError(msg)


def get_timeout_with_buffer(
    timeout: int | None,
    instance_config: StampedeConfig | None,
    override: bool | StampedeConfig | None = None,
) -> int | None:
    """Add the stampede buffer to ``timeout``.

    Returns ``timeout`` unchanged if stampede prevention is disabled, the
    timeout is ``None``, or non-positive.
    """
    config = resolve_stampede(instance_config, override)
    if not config or not timeout or timeout <= 0:
        return timeout
    return timeout + config.buffer


_STAMPEDE_FIELDS = ("buffer", "beta", "delta")


def make_stampede_config(option: bool | dict | None) -> StampedeConfig | None:
    """Build a ``StampedeConfig`` from an adapter ``stampede_prevention`` option.

    Unknown dict keys are dropped (with a warning) instead of raising
    ``TypeError``. Matches ``resolve_stampede``'s tolerance for typos in
    per-call overrides; the two were inconsistent before.
    """
    if not option:
        return None
    if isinstance(option, dict):
        known = {k: v for k, v in option.items() if k in _STAMPEDE_FIELDS}
        unknown = sorted(set(option) - set(_STAMPEDE_FIELDS))
        if unknown:
            logger.warning(
                "stampede_prevention: ignoring unknown keys %s (valid: %s)",
                unknown,
                _STAMPEDE_FIELDS,
            )
        return StampedeConfig(**known)
    return StampedeConfig()
