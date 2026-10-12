"""The worker's reflective seam: turns a handlers object into executable step wrappers.

Two pure operations, deliberately free of I/O and of the worker's runtime state, so every
signature rule here is unit-testable against a compiled graph (mirrors the Java client's
``HandlerBinder``):

- :func:`scan` — inventory an object's step methods (by canonical name), rejecting ambiguous
  names and non-single-context signatures.
- :func:`bind` — resolve an inventory against a graph, node by node, validating each matched
  method's return annotation against the node's kind and building the invocation wrapper.
  Returns the bindings plus the steps this object doesn't serve; the ``Worker`` decides what to
  do with both.

A method's return annotation picks its kind: ``-> bool`` binds a gate, ``-> None`` a side
effect, anything else (or no annotation) a task whose return REPLACES the context. A method
matched to a combine node returns the complete post-join context.

:func:`unstage` is the third pure operation: it turns a combine activation's context into the
shape a combine handler takes, and computes the base ``wiggle.step.base()`` exposes inside it.
"""
from __future__ import annotations

import inspect
import re
from dataclasses import dataclass
from typing import Any, Callable, Optional

from .workflow import (ARM_SCRATCH_PREFIX, FOR_EACH_SCRATCH_PREFIX, SPAWN_SCRATCH_PREFIX,
                       STEPS_SCRATCH_PREFIX)

# tokens split on any non-alphanumeric run, and on camelCase / acronym boundaries
_NON_ALNUM = re.compile(r"[^0-9A-Za-z]+")
_CAMEL_BOUNDARY = re.compile(r"(?<=[a-z0-9])(?=[A-Z])")
_ACRONYM_BOUNDARY = re.compile(r"(?<=[A-Z])(?=[A-Z][a-z])")


def canonical(name: str) -> str:
    """Fold a name to a case/style-independent key: lowercase alphanumeric tokens, concatenated. So
    ``in-stock``, ``in_stock``, ``inStock``, ``InStock``, and ``instock`` all yield ``instock``."""
    spaced = _NON_ALNUM.sub(" ", name)
    spaced = _ACRONYM_BOUNDARY.sub(" ", _CAMEL_BOUNDARY.sub(" ", spaced))
    return "".join(tok.lower() for tok in spaced.split())


@dataclass
class Candidate:
    name: str                 # the original method name, for error messages
    method: Callable
    return_annotation: Any    # inspect return annotation (drives the kind)


@dataclass(frozen=True)
class Compensation:
    """Both context snapshots of the step being undone, captured by the engine at the step's
    completion — NOT read from the instance's latest context, which a later step may have replaced.
    ``result`` is the primary snapshot for most undos (the step's own products live there);
    ``input`` serves restore-previous-value undos and undo-only data (an idempotency key derived
    from the input), so nothing has to be smuggled through the business context to reach the
    compensator."""
    input: Any    # the context as the step received it
    result: Any   # the context as the step left it — the post-step snapshot


@dataclass
class Binding:
    """One resolved binding: the executable wrapper plus where it plugs into the worker."""
    activity: str
    step: str
    queue: str
    handler: Callable


@dataclass
class Result:
    """Everything :func:`bind` decided: the bindings, plus the steps this object doesn't serve
    (informational — another worker may serve them)."""
    bindings: list[Binding]
    unserved: list[str]


def scan(handlers: object) -> dict[str, Candidate]:
    """Introspect ``handlers`` for its step methods, keyed by canonical name. Raises if two public
    methods fold to the same canonical name (ambiguous across case styles), or if a public method
    does not take a single context argument."""
    found: dict[str, Candidate] = {}
    for attr in sorted(dir(handlers)):
        if attr.startswith("_") or attr == "workflow":
            continue
        try:
            member = getattr(handlers, attr)
        except Exception:  # noqa: BLE001 - a property that raises is not a handler
            continue
        if not callable(member) or inspect.isclass(member):
            continue
        try:
            sig = inspect.signature(member)
        except (TypeError, ValueError):
            continue
        if not _accepts_single_context(sig):
            raise ValueError(f"handler method '{attr}' must accept a single context argument "
                             f"(prefix helper methods with '_' to skip them)")
        canon = canonical(attr)
        if not canon:
            continue
        if canon in found:
            raise ValueError(f"handler methods '{found[canon].name}' and '{attr}' both map to the "
                             f"same step name '{canon}'; names differing only in case/style are "
                             f"ambiguous -- rename one")
        found[canon] = Candidate(attr, member, sig.return_annotation)
    if not found:
        raise ValueError("no handler methods found on the object (public methods taking one context "
                         "argument)")
    return found


def bind(workflow: str, candidates: dict[str, Candidate], graph: dict,
         branch_steps: frozenset = frozenset()) -> Result:
    """Resolve an inventory against a compiled graph: each candidate is matched to a
    worker-dispatched step by canonical name and wrapped per the node's kind. A method matching no
    step, or a kind clash, fails fast -- unless it implements one of ``branch_steps`` (canonical
    names of steps only created branches run), which :func:`bind_created` binds on dispatch. Pure:
    no I/O, no worker state."""
    nodes = {n["name"]: n for n in graph.get("nodes", [])
             if n.get("kind") in ("TASK", "PREDICATE") and "name" in n}
    by_canon: dict[str, tuple[str, dict]] = {}
    for name, node in nodes.items():
        by_canon.setdefault(canonical(name), (name, node))   # graph step names are already unique
    candidates = {canon: cand for canon, cand in candidates.items()
                  if canon in by_canon or _branch_target(canon, branch_steps) is None}
    # Partition out compensators first: a method named compensate_<step> (any case style) whose
    # canonical name matches no step directly is the undo of <step>. A step literally named
    # "compensate-x" keeps priority — its handler matches by_canon directly and stays a forward
    # handler.
    forward: dict[str, Candidate] = {}
    compensators: dict[str, Candidate] = {}   # canonical TARGET step -> candidate
    for canon, cand in candidates.items():
        target = canon[len("compensate"):] if canon.startswith("compensate") else ""
        if canon not in by_canon and target and target in by_canon:
            compensators[target] = cand
        else:
            forward[canon] = cand
    bindings: list[Binding] = []
    served: set[str] = set()
    for canon, cand in forward.items():
        match = by_canon.get(canon)
        if match is None:
            avail = sorted(nodes)
            raise ValueError(f"handler '{cand.name}' matches no step in workflow '{workflow}' "
                             f"(available steps: {avail})")
        step_name, node = match
        activity = f"{workflow}#{step_name}"
        bindings.append(Binding(activity, step_name, node.get("queue", workflow),
                                _wrapper_for(cand, node, activity)))
        served.add(step_name)
    # Compensators bind under "<activity>#compensate"; the pairing is checked BOTH ways — a
    # compensate_<x> targeting a non-compensable step is a lie, and a compensable step served
    # forward without its undo would strand the engine's reverse pass.
    for target, cand in compensators.items():
        step_name, node = by_canon[target]
        if node["kind"] != "TASK" or is_combine(node) or not node.get("compensable"):
            raise ValueError(f"compensator '{cand.name}' targets step '{step_name}' of workflow "
                             f"'{workflow}', which is not declared compensable in the topology")
        bindings.append(Binding(f"{workflow}#{step_name}#compensate", step_name,
                                node.get("queue", workflow), _compensation_wrapper(cand.method)))
    for canon in forward:
        match = by_canon.get(canon)
        if match is None:
            continue
        step_name, node = match
        if node["kind"] == "TASK" and node.get("compensable") and canon not in compensators:
            raise ValueError(f"step '{step_name}' of workflow '{workflow}' is declared "
                             f"compensable but the handlers object has no "
                             f"'compensate_{canon}' method taking a wiggle.Compensation")
    unserved = sorted(set(nodes) - served)
    return Result(bindings, unserved)


def _branch_target(canon: str, branch_steps: frozenset) -> Optional[str]:
    """The branch step ``canon`` implements -- itself, or the step it undoes -- or None."""
    if canon in branch_steps:
        return canon
    if canon.startswith("compensate") and canon[len("compensate"):] in branch_steps:
        return canon[len("compensate"):]
    return None


def bind_created(workflow: str, candidates: dict[str, Candidate], step_name: str, kind: str,
                 combine: bool, undo: bool) -> Optional[Callable]:
    """The wrapper for a step a created branch runs -- in no graph, so matched by name on dispatch
    -- or None when no method implements it. ``kind`` and ``combine`` describe the dispatched task;
    ``undo`` asks for the step's compensator. Raises ``ValueError`` when the method's signature does
    not fit the step, as :func:`bind` would."""
    canon = canonical(step_name)
    if undo:
        cand = candidates.get("compensate" + canon)
        return _compensation_wrapper(cand.method) if cand else None
    cand = candidates.get(canon)
    if cand is None:
        return None
    node = {"kind": kind}
    if combine:
        node["collectKey"] = "~"
    return _wrapper_for(cand, node, f"{workflow}#{step_name}")


def _compensation_wrapper(method: Callable) -> Callable:
    """Adapt a compensator method to the runtime handler shape: the engine stages the two
    snapshots as the activation context ``{"input": ..., "result": ...}``; split them out. The
    return is None — an undo never changes the (already doomed) instance context."""
    def wrapped(ctx):
        method(Compensation(input=ctx.get("input"), result=ctx.get("result")))
        return None
    return wrapped


def is_combine(node: dict) -> bool:
    """Whether a registered graph node is a fork or forEach combine: a TASK with ``armNames`` or a
    ``collectKey``, or -- in a graph written before those fields -- an ``itemsKey``."""
    return node.get("kind") == "TASK" and bool(
        node.get("armNames") or node.get("collectKey") or node.get("itemsKey"))


def unstage(ctx: Any, arm_names: list, collect_key: str, step_name: str) -> tuple[Any, Any, Any]:
    """Rewrite a combine activation's context into the shape a combine handler takes, and return it
    with the base (the context without the staged results) and the results themselves, which
    ``wiggle.step.results()`` exposes. A fork's arms appear under their names and a forEach's
    results under its name; created branches' results appear only through ``results()``, since
    they have no name of their own. Raises ``ValueError`` when an arm or forEach name is also a key
    of the workflow's own context, which the handler could not tell apart."""
    if not isinstance(ctx, dict) or (not arm_names and not collect_key):
        return ctx, None, None
    base = dict(ctx)
    placed = {}

    def place(scratch: str, name: str) -> None:
        if scratch not in base:
            return
        value = base.pop(scratch)
        if name in base:
            raise ValueError(f"combine '{step_name}': '{name}' is both a result to combine and a key "
                             f"of the context; rename the branch or the key")
        placed[name] = value

    if arm_names:
        for arm in arm_names:
            place(ARM_SCRATCH_PREFIX + arm, arm)
        return {**base, **placed}, base, dict(placed)
    base.pop(STEPS_SCRATCH_PREFIX + collect_key, None)
    if collect_key.startswith(SPAWN_SCRATCH_PREFIX):
        results = base.pop(collect_key, [])
        return base, base, results
    name = collect_key[len(FOR_EACH_SCRATCH_PREFIX):] \
        if collect_key.startswith(FOR_EACH_SCRATCH_PREFIX) else collect_key
    place(collect_key, name)
    return {**base, **placed}, base, placed.get(name)


def _wrapper_for(cand: Candidate, node: dict, activity: str) -> Callable:
    """Build the runtime wrapper for a matched method, validating the method's return annotation
    against the graph node's kind (the graph decides gate vs task; the annotation decides task vs
    effect)."""
    kind = node["kind"]
    ret = cand.return_annotation
    method = cand.method
    empty = inspect.Signature.empty
    if kind == "PREDICATE":
        if ret is not empty and ret is not bool:
            raise ValueError(f"activity '{activity}' is a gate (PREDICATE) but handler "
                             f"'{cand.name}' is annotated to return {ret!r}; a gate must return bool")
        return lambda ctx: bool(method(ctx))
    if is_combine(node):
        # A fork/forEach combine: the return is the COMPLETE post-join context, sent verbatim (no
        # diff) -- the engine replaces the context with it; there is no implicit fold.
        if ret is bool or ret is None:
            raise ValueError(f"activity '{activity}' is a fork combine; handler '{cand.name}' "
                             f"must return the complete post-join context (a dict), not {ret!r}")
        def combine_wrapper(ctx):
            return method(ctx)
        return combine_wrapper
    # TASK node: task unless the method is a declared side effect (-> None), a bool is a mistake here
    if ret is bool:
        raise ValueError(f"activity '{activity}' is a TASK but handler '{cand.name}' is annotated "
                         f"to return bool; that looks like a gate -- did you mean a PREDICATE step?")
    if ret is None:   # explicit `-> None`: a side effect, context unchanged
        def effect_wrapper(ctx):
            method(ctx)
            return None
        return effect_wrapper

    def task_wrapper(ctx):
        # The return is the step's COMPLETE next context: sent whole, it replaces the
        # previous value server-side (None leaves it untouched).
        return method(ctx)
    return task_wrapper


def _accepts_single_context(sig: inspect.Signature) -> bool:
    """True if the (already-bound) method takes exactly one required positional arg (the context) --
    or none plus ``*args``. Extra keyword/defaulted params are fine."""
    required = 0
    has_var_positional = False
    for p in sig.parameters.values():
        if p.kind == inspect.Parameter.VAR_POSITIONAL:
            has_var_positional = True
        elif p.kind in (inspect.Parameter.POSITIONAL_ONLY, inspect.Parameter.POSITIONAL_OR_KEYWORD):
            if p.default is inspect.Parameter.empty:
                required += 1
    return required == 1 or (required == 0 and has_var_positional)
