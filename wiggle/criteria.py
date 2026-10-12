"""Queue criteria: a predicate over a ready task's properties, so a worker claims only the tasks of a
queue it should run -- the JPA criteria shape, evaluated by the server when it claims.

    from wiggle import criteria as c
    from wiggle.criteria import Task_

    worker = Worker(client, "eu-1", criteria=[
        c.QueueCriteria("payments", c.and_(c.equal(Task_.WORKFLOW, "orders"), c.less_than(Task_.ATTEMPT, 3))),
        c.QueueCriteria("shipping", c.equal(Task_.attr("region"), "eu")),   # a declared attribute
    ])

A comparison on an absent value is false (and ``not_`` of it true); only :func:`is_null` and
:func:`is_not_null` test for absence. Criteria are checked here against the server's limits, so a
malformed one fails where it is built.
"""
from __future__ import annotations

import datetime
import re
from dataclasses import dataclass
from decimal import Decimal
from typing import Any, Optional, Union

MAX_DEPTH = 8
MAX_NODES = 64
MAX_IN = 256
MAX_TEXT = 256
MAX_SCALE = 10
MAX_INTEGER_DIGITS = 28

_ATTRIBUTE_NAME = re.compile(r"[a-z][a-z0-9_]{0,62}")


@dataclass(frozen=True)
class Field:
    """A task property a comparison tests. ``attribute`` names a declared attribute (field
    ``ATTRIBUTE``); it is ``None`` for every other field."""
    name: str
    type: str                          # TEXT | INTEGER | ANY
    attribute: Optional[str] = None


class Task_:
    """The task properties a comparison tests, and :meth:`attr` for a value the workflow declares."""
    WORKFLOW = Field("WORKFLOW", "TEXT")
    VERSION = Field("VERSION", "INTEGER")
    STEP = Field("STEP", "TEXT")                       # equal, not_equal and in_ only
    NODE_ID = Field("NODE_ID", "TEXT")
    KIND = Field("KIND", "TEXT")                       # TASK or PREDICATE
    ATTEMPT = Field("ATTEMPT", "INTEGER")              # 1 on the first run
    CORRELATION_ID = Field("CORRELATION_ID", "TEXT")
    INSTANCE_CREATED_AT = Field("INSTANCE_CREATED_AT", "INTEGER")   # epoch millis, or a datetime

    @staticmethod
    def attr(name: str) -> Field:
        """A value the workflow declares (``Graph(attributes=[Attribute(name, type, path)])``)."""
        if not _ATTRIBUTE_NAME.fullmatch(name or ""):
            raise ValueError(f"attribute '{name}' is not a valid attribute name")
        return Field("ATTRIBUTE", "ANY", name)


@dataclass(frozen=True)
class Comparison:
    field: Field
    op: str
    args: tuple


@dataclass(frozen=True)
class And:
    terms: tuple


@dataclass(frozen=True)
class Or:
    terms: tuple


@dataclass(frozen=True)
class Not:
    term: "Predicate"


Predicate = Union[Comparison, And, Or, Not]


def equal(field: Field, value: Any) -> Comparison:
    return _cmp(field, "EQUAL", value)


def not_equal(field: Field, value: Any) -> Comparison:
    return _cmp(field, "NOT_EQUAL", value)


def in_(field: Field, *values: Any) -> Comparison:
    if len(values) == 1 and isinstance(values[0], (list, tuple, set, frozenset)):
        values = tuple(values[0])
    return _cmp(field, "IN", *values)


def less_than(field: Field, value: Any) -> Comparison:
    return _cmp(field, "LESS_THAN", value)


def less_than_or_equal_to(field: Field, value: Any) -> Comparison:
    return _cmp(field, "LESS_THAN_OR_EQUAL", value)


def greater_than(field: Field, value: Any) -> Comparison:
    return _cmp(field, "GREATER_THAN", value)


def greater_than_or_equal_to(field: Field, value: Any) -> Comparison:
    return _cmp(field, "GREATER_THAN_OR_EQUAL", value)


def between(field: Field, low: Any, high: Any) -> Comparison:
    return _cmp(field, "BETWEEN", low, high)


def like(field: Field, pattern: str) -> Comparison:
    """``%`` and ``_`` wildcards, ``\\`` escapes; case-sensitive."""
    return _cmp(field, "LIKE", pattern)


def is_null(field: Field) -> Comparison:
    return _cmp(field, "IS_NULL")


def is_not_null(field: Field) -> Comparison:
    return _cmp(field, "IS_NOT_NULL")


def and_(*terms: Predicate) -> And:
    if not terms:
        raise ValueError("and_() needs at least one term")
    return And(tuple(terms))


def or_(*terms: Predicate) -> Or:
    if not terms:
        raise ValueError("or_() needs at least one term")
    return Or(tuple(terms))


def not_(term: Predicate) -> Not:
    return Not(term)


@dataclass(frozen=True)
class QueueCriteria:
    """What a poll may claim from ``queue``: its ready tasks for which ``where`` holds (all of them
    when ``where`` is ``None``). A worker serving several queues holds one per queue."""
    queue: str
    where: Optional[Predicate] = None

    def __post_init__(self):
        if not self.queue or not self.queue.strip():
            raise ValueError("criteria need a queue")
        if self.where is not None:
            _validate(self.where, 1, [0])


_ARITY = {"EQUAL": (1, 1), "NOT_EQUAL": (1, 1), "IN": (1, MAX_IN), "LESS_THAN": (1, 1),
          "LESS_THAN_OR_EQUAL": (1, 1), "GREATER_THAN": (1, 1), "GREATER_THAN_OR_EQUAL": (1, 1),
          "BETWEEN": (2, 2), "LIKE": (1, 1), "IS_NULL": (0, 0), "IS_NOT_NULL": (0, 0)}
_ORDERING = {"LESS_THAN", "LESS_THAN_OR_EQUAL", "GREATER_THAN", "GREATER_THAN_OR_EQUAL", "BETWEEN"}


def _cmp(field: Field, op: str, *values: Any) -> Comparison:
    if not isinstance(field, Field):
        raise TypeError(f"not a criteria field: {field!r}")
    c = Comparison(field, op, tuple(_literal(field, v) for v in values))
    _validate_comparison(c)
    return c


def _literal(field: Field, v: Any) -> Any:
    if v is None:
        raise ValueError(f"{_label(field)}: a comparison value must not be None")
    if isinstance(v, datetime.datetime):
        return int(v.timestamp() * 1000)
    if isinstance(v, float) and not isinstance(v, bool):
        return Decimal(repr(v))
    if isinstance(v, Decimal):
        return v.normalize()
    return v


def _label(field: Field) -> str:
    return f"attribute '{field.attribute}'" if field.attribute else field.name


def _validate(p: Predicate, depth: int, nodes: list) -> None:
    if depth > MAX_DEPTH:
        raise ValueError(f"criteria nest deeper than {MAX_DEPTH}")
    nodes[0] += 1
    if nodes[0] > MAX_NODES:
        raise ValueError(f"criteria have more than {MAX_NODES} nodes")
    if isinstance(p, Comparison):
        _validate_comparison(p)
    elif isinstance(p, (And, Or)):
        if not p.terms:
            raise ValueError("and_()/or_() need at least one term")
        for t in p.terms:
            _validate(t, depth + 1, nodes)
    elif isinstance(p, Not):
        _validate(p.term, depth + 1, nodes)
    else:
        raise TypeError(f"not a criteria predicate: {p!r}")


def _validate_comparison(c: Comparison) -> None:
    f, op, n = c.field, c.op, len(c.args)
    lo, hi = _ARITY[op]
    if not lo <= n <= hi:
        want = str(lo) if lo == hi else f"{lo}..{hi}"
        raise ValueError(f"{_label(f)} {op} takes {want} argument(s), got {n}")
    if f.name == "STEP" and op not in ("EQUAL", "NOT_EQUAL", "IN"):
        raise ValueError(f"STEP supports equal, not_equal and in_ only, not {op}")
    if op == "LIKE" and f.type == "INTEGER":
        raise ValueError(f"like needs a text field, not {f.name}")
    kinds = set()
    for a in c.args:
        if isinstance(a, bool):
            kind = "flag"
        elif isinstance(a, int):
            kind = "integer"
        elif isinstance(a, Decimal):
            kind = "decimal"
        elif isinstance(a, str):
            kind = "text"
            if len(a) > MAX_TEXT:
                raise ValueError(f"{_label(f)}: a text argument is longer than {MAX_TEXT} characters")
        else:
            raise TypeError(f"{_label(f)} takes text, an integer, a decimal or a bool, "
                            f"got {type(a).__name__}")
        kinds.add(kind)
        if f.type == "TEXT" and kind != "text":
            raise ValueError(f"{f.name} takes text, got {type(a).__name__}")
        if f.type == "INTEGER" and kind != "integer":
            raise ValueError(f"{f.name} takes an integer, got {type(a).__name__}")
        if kind == "decimal" and not _fits(a):
            raise ValueError(f"{_label(f)}: {a} has more than {MAX_SCALE} fraction or "
                             f"{MAX_INTEGER_DIGITS} integer digits")
    if f.type == "ANY":
        if {"integer", "decimal"} & kinds:
            kinds = (kinds - {"integer", "decimal"}) | {"number"}
        if len(kinds) > 1:
            raise ValueError(f"{_label(f)}: arguments of mixed types")
        if op == "LIKE" and kinds != {"text"}:
            raise ValueError(f"{_label(f)}: like needs text")
        if kinds == {"flag"} and (op in _ORDERING or op == "LIKE"):
            raise ValueError(f"{_label(f)}: a boolean supports equal, not_equal and in_ only")


def _fits(d: Decimal) -> bool:
    t = d.normalize().as_tuple()
    scale = max(0, -t.exponent)
    integer_digits = max(0, len(t.digits) + t.exponent)
    return scale <= MAX_SCALE and integer_digits <= MAX_INTEGER_DIGITS
