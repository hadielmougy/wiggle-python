"""Ambient metadata for the step currently running on this thread of execution.

A worker publishes the running task's identity around each handler call, so a handler can read
``step.attempt()``, ``step.name()`` or ``step.instance_id()`` without extra parameters, and can
``step.emit(...)`` domain events that ride the step's report.

Inside a ``forEach`` item step, the handler's parameter is the ITEM's current value; the frozen
pre-forEach context and the element's position/source key are available here instead of being
injected into user data:

    from wiggle import step

    def price(item):
        return {"sku": item, "cost": step.base()["rate"] * 2, "i": step.item_index()}

``base()`` is read-only — an item can never write the base; only the forEach combine's return
reaches the shared context. Calling these outside a running step raises.
"""
from __future__ import annotations

import builtins
import contextvars
from dataclasses import dataclass, field
from typing import Any, Optional


@dataclass
class _Scope:
    attempt: int = 0
    name: str = ""
    instance_id: str = ""
    base: Any = None
    has_base: bool = False
    item: bool = False
    index: int = 0
    map_key: Optional[str] = None
    emitted: list = field(default_factory=list)


_SCOPE: contextvars.ContextVar[Optional[_Scope]] = contextvars.ContextVar("wiggle_step_scope", default=None)


def emit(type: str, payload: dict) -> None:
    """Emit a domain event onto the instance's event log. It is buffered and rides this step's
    report, where the server appends it in the transaction that settles the step: committed if and
    only if this attempt completes. An attempt that raises leaves nothing behind, and its retry
    emits fresh. ``type`` names the fact (the ``wf.`` prefix is reserved for the engine); ``payload``
    is a dict, so a consumer can read a field from it."""
    scope = _current()
    if not type:
        raise ValueError("an event needs a type")
    if not isinstance(payload, dict):
        kind = "None" if payload is None else builtins.type(payload).__name__
        raise TypeError(f"event '{type}' carries a {kind} payload; an event's payload is a dict, "
                        f"so a consumer can read a field from it")
    scope.emitted.append((type, payload))


def attempt() -> int:
    """The engine-global attempt number: 1 on the first try, incremented on every retry."""
    return _current().attempt


def name() -> str:
    """The running step's name."""
    return _current().name


def instance_id() -> str:
    """The workflow instance the running step belongs to."""
    return _current().instance_id


def base() -> dict:
    """The frozen base context, as a dict (read-only by contract): inside a forEach item step the
    pre-forEach context; inside a fork/forEach combine the pre-fork context (staged results
    excluded). Available ambiently so a combine can also read it without popping scratch keys."""
    scope = _SCOPE.get()
    if scope is None or not scope.has_base:
        raise RuntimeError("wiggle.step.base() is only available inside a forEach item step or a "
                           "fork/forEach combine")
    return scope.base


def item_index() -> int:
    """This element's position in the input collection."""
    return _require_item().index


def item_map_key() -> Optional[str]:
    """This element's source key when the input collection was a map; ``None`` for a list."""
    return _require_item().map_key


def _current() -> _Scope:
    scope = _SCOPE.get()
    if scope is None:
        raise RuntimeError("wiggle.step metadata is only available inside a running step")
    return scope


def _require_item() -> _Scope:
    scope = _SCOPE.get()
    if scope is None or not scope.item:
        raise RuntimeError("wiggle.step.item_index()/item_map_key() are only available inside a "
                           "forEach item step")
    return scope


def _begin(scope: _Scope) -> contextvars.Token:
    return _SCOPE.set(scope)


def _end(token: contextvars.Token) -> None:
    _SCOPE.reset(token)
