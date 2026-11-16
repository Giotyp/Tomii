"""Helpers that build common `instance_map` shapes (see `_node.InstanceMap`).

Both helpers return a callable `j -> Iterable[int]`, resolved at graph-build
time once the consumer node's factor is known (see `_serialize._arg`).
"""

from __future__ import annotations

from typing import Callable, Iterable


def halo(radius: int, wrap: bool = True) -> Callable[[int], Iterable[int]]:
    """A neighbourhood (halo) stencil map: consumer instance ``j`` depends on
    producer instances ``j - radius .. j + radius`` (inclusive).

    With ``wrap=True`` (the default), the neighbourhood wraps around the
    producer's factor (``tile 0``'s left neighbour is the last tile) — the
    shape a ring/torus-style decomposition needs. With ``wrap=False``, a
    neighbourhood that would run off either end is clamped to the valid
    range (the boundary tiles depend on fewer producers, not out-of-range
    ones).

    The producer's factor is supplied by the runtime's own validation (every
    `instance_map` entry is checked against the real producer factor at load
    time) — this helper does not need it, so it works identically regardless
    of which producer it is attached to, as long as producer and consumer
    share the same factor (the usual halo/stencil shape).

    Example::

        tile = app.node("tile", func="...", factor=8)
        halo_step = app.node(
            "halo_step", func="...", factor=8,
            args=[tile.wait(map=tm.halo(1, wrap=True))],
        )
    """
    if radius < 0:
        raise ValueError(f"halo radius must be >= 0, got {radius}")

    def _map(j: int) -> Iterable[int]:
        # Resolved per consumer instance at build time; the producer's factor
        # is threaded in by the caller (`_serialize._arg`) via a closure over
        # `_bind_factor`, not visible here — see `_resolve_map`.
        raise RuntimeError("halo() map must be resolved via _resolve_map (internal)")

    # Expose the radius/wrap configuration so `_resolve_map` can build the
    # concrete per-instance lists once it knows the producer's factor.
    _map.__tomii_halo__ = (radius, wrap)  # type: ignore[attr-defined]
    return _map


def stride(k: int) -> Callable[[int], Iterable[int]]:
    """A strided map: consumer instance ``j`` depends on producer instance
    ``j * k`` only (a many-to-few reduction — e.g. consumer ``j`` summarizes
    producers ``[j*k, j*k + k)`` would instead use an explicit list
    comprehension; `stride` covers the single-producer-per-consumer case).

    Example::

        coarse = app.node(
            "coarse", func="...", factor=4,
            args=[fine.wait(map=tm.stride(4))],  # coarse[j] <- fine[j*4]
        )
    """
    if k < 1:
        raise ValueError(f"stride must be >= 1, got {k}")

    def _map(j: int) -> Iterable[int]:
        raise RuntimeError("stride() map must be resolved via _resolve_map (internal)")

    _map.__tomii_stride__ = k  # type: ignore[attr-defined]
    return _map


def resolve_map(
    map_fn: Callable[[int], Iterable[int]], consumer_factor: int, producer_factor: int
) -> "list[list[int]]":
    """Resolve a map callable (either a plain `j -> Iterable[int]` closure, or
    one produced by `halo`/`stride`) into an explicit `instance_map` list of
    lists, given the consumer's factor and the producer's factor.

    `halo`/`stride` are resolved specially here (where the producer's factor —
    needed for wrapping/clamping — is actually known); any other callable is
    just invoked per consumer instance `j` with no producer-factor awareness
    (the common case: a user-supplied closure that already captures whatever
    it needs).
    """
    halo_cfg = getattr(map_fn, "__tomii_halo__", None)
    if halo_cfg is not None:
        radius, wrap = halo_cfg
        n = producer_factor
        out: "list[list[int]]" = []
        for j in range(consumer_factor):
            if wrap:
                out.append([(j + d) % n for d in range(-radius, radius + 1)])
            else:
                out.append(
                    [j + d for d in range(-radius, radius + 1) if 0 <= j + d < n]
                )
        return out

    stride_k = getattr(map_fn, "__tomii_stride__", None)
    if stride_k is not None:
        return [[j * stride_k] for j in range(consumer_factor)]

    return [list(map_fn(j)) for j in range(consumer_factor)]
