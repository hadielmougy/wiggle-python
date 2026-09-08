"""Ambient metadata for the step currently running on this thread of execution.

Inside a ``forEach`` item step, the handler's parameter is the ITEM's current value; the frozen
pre-forEach context and the element's position/source key are available here instead of being
injected into user data:

    from wiggle import step

    def price(item):
        return {"sku": item, "cost": step.base()["rate"] * 2, "i": step.item_index()}

``base()`` is read-only — an item can never write the base; only the forEach combine's return
reaches the shared context. Calling these outside a forEach item step raises.
"""
from __future__ import annotations

import contextvars
from typing import Any, Optional

_SCOPE: contextvars.ContextVar[Optional[tuple]] = contextvars.ContextVar("wiggle_item_scope", default=None)


def base() -> dict:
    """The frozen pre-forEach context, as a dict (read-only by contract)."""
    return _require()[0]


def item_index() -> int:
    """This element's position in the input collection."""
    return _require()[1]


def item_map_key() -> Optional[str]:
    """This element's source key when the input collection was a map; ``None`` for a list."""
    return _require()[2]


def _require() -> tuple:
    scope = _SCOPE.get()
    if scope is None:
        raise RuntimeError("wiggle.step.base()/item_index()/item_map_key() are only available "
                           "inside a forEach item step (elsewhere the context IS the handler's parameter)")
    return scope


def _begin(base_ctx: Any, index: int, map_key: Optional[str]) -> contextvars.Token:
    return _SCOPE.set((base_ctx, index, map_key))


def _end(token: contextvars.Token) -> None:
    _SCOPE.reset(token)
