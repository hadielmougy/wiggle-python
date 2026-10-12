"""The workflow topology: :meth:`FlowSpec.define` records a chain of steps (see :mod:`wiggle.flow`)
and compiles it, through the private node stream below, into the graph definition the server stores.

Handlers are **not** part of the topology -- a :class:`~wiggle.worker.Worker` binds them separately, by
name (``handle`` / ``handle_gate`` / ``handle_effect``). The graph lives on the server; any worker that
implements the matching activity names can run its steps, so Python, Java, and Go workers interoperate.

The context is a plain ``dict`` that flows through the steps. A handler returns the *whole* context
(usually ``{**ctx, ...}``); the engine replaces the context with it.

    from wiggle import FlowSpec, RetryPolicy, Wiggle

    def order(f):
        validated = f.then_apply("validate").then_filter("in-stock")
        payment = validated.then_apply("charge", queue="payments", retry=RetryPolicy.exponential(5, 0.1))
        shipping = validated.then_apply("reserve").then_apply("label")
        return Wiggle.all_of(payment, shipping).combine("merge").then_accept("notify")

    wf = FlowSpec.define("order", order)
"""
from __future__ import annotations

import hashlib
import json
import re
from collections import deque
from dataclasses import dataclass, field
from typing import Any, Callable, Optional, Union

# The engine stages a join's results under reserved keys so they cannot collide with the workflow's
# own data: a fork arm under ARM_SCRATCH_PREFIX + arm, a forEach's collected results under the
# combine's collectKey -- FOR_EACH_SCRATCH_PREFIX + name for a forEach, SPAWN_SCRATCH_PREFIX + step for
# branches a step created -- and the step that produced each result under STEPS_SCRATCH_PREFIX + collectKey.
ARM_SCRATCH_PREFIX = "__arm__"
FOR_EACH_SCRATCH_PREFIX = "__forEach__"
SPAWN_SCRATCH_PREFIX = "__spawn__"
STEPS_SCRATCH_PREFIX = "__steps__"

_MAX_INT = 2_147_483_647

Context = dict
Activity = Callable[[Context], Context]      # task handler: ctx -> new ctx
SideEffect = Callable[[Context], Any]        # effect handler: ctx -> (ignored)
Predicate = Callable[[Context], bool]        # gate / choose guard / do-while condition: ctx -> bool


@dataclass(frozen=True)
class RetryPolicy:
    """A per-step retry policy. Backoffs are given in seconds."""

    max_attempts: int = _MAX_INT
    initial_backoff_s: float = 1.0
    multiplier: float = 1.0
    max_backoff_s: float = 60.0
    jitter: float = 0.0

    def to_json(self) -> dict:
        return {
            "maxAttempts": int(self.max_attempts),
            "initialBackoffMillis": int(self.initial_backoff_s * 1000),
            "multiplier": float(self.multiplier),
            "maxBackoffMillis": int(self.max_backoff_s * 1000),
            "jitter": float(self.jitter),
        }

    @staticmethod
    def forever() -> "RetryPolicy":
        return RetryPolicy(_MAX_INT, 1.0, 1.0, 60.0, 0.0)

    @staticmethod
    def none() -> "RetryPolicy":
        return RetryPolicy(1, 0.0, 1.0, 0.0, 0.0)

    @staticmethod
    def exponential(max_attempts: int, initial_backoff_s: float) -> "RetryPolicy":
        return RetryPolicy(max_attempts, initial_backoff_s, 2.0, 300.0, 0.2)

    @staticmethod
    def fixed(max_attempts: int, backoff_s: float) -> "RetryPolicy":
        return RetryPolicy(max_attempts, backoff_s, 1.0, backoff_s, 0.0)


# ---- the recorded topology: what a WiggleFlow chain compiles to before it becomes graph rows ----


class _Node:
    """One element of a compiled node stream; the builder below turns a list of these into graph rows."""


@dataclass
class _Task(_Node):
    """A step run on a worker (``then_apply`` / ``then_accept``). ``combine`` makes it a spawning
    step: the branches its handler creates rejoin at that combine."""

    name: str
    queue: Optional[str] = None
    retry: Optional[RetryPolicy] = None
    compensate: bool = False
    combine: str = ""
    combine_queue: Optional[str] = None
    combine_retry: Optional[RetryPolicy] = None


@dataclass
class _Gate(_Node):
    """A predicate (``then_filter``): false ends the instance as ``gated:<name>``, or inside a branch
    short-circuits to that branch's join."""

    name: str
    queue: Optional[str] = None
    retry: Optional[RetryPolicy] = None


@dataclass
class _Sleep(_Node):
    name: str
    millis: int = 0


@dataclass
class _AwaitSignal(_Node):
    name: str
    timeout_s: float = 0.0
    escalation: Optional[list[_Node]] = None


@dataclass
class _SubWorkflow(_Node):
    name: str
    workflow: str


@dataclass
class _Fork(_Node):
    """Parallel arms on isolated context copies, rejoined at the mandatory ``combine``."""

    arms: list[tuple[str, list[_Node]]]
    combine: str = ""
    combine_queue: Optional[str] = None
    combine_retry: Optional[RetryPolicy] = None


@dataclass
class _ForEach(_Node):
    """One isolated branch per element of the collection at ``items_key``, rejoined at ``combine``."""

    name: str
    items_key: str
    body: list[_Node]
    combine: str = ""
    combine_queue: Optional[str] = None
    combine_retry: Optional[RetryPolicy] = None


@dataclass
class _Case:
    """One arm of a choice; ``when is None`` is the otherwise arm, which must be last."""

    then: list[_Node]
    when: Optional[str] = None
    queue: Optional[str] = None
    retry: Optional[RetryPolicy] = None


@dataclass
class _Choose(_Node):
    cases: list[_Case]


@dataclass
class _DoWhile(_Node):
    """Run ``body``, then again while ``condition`` holds; ``max_iterations`` 0 = engine default."""

    condition: str
    body: list[_Node]
    max_iterations: int = 0
    queue: Optional[str] = None


@dataclass
class _DefaultQueue(_Node):
    """Sets the queue of every step after it that names none."""

    queue: str


@dataclass
class _DeclareAttribute(_Node):
    attribute: "Attribute"


@dataclass
class FlowSpec:
    """A compiled workflow: the definition sent to the server (topology only). Handlers are bound
    separately on a worker, by name. Make one with :meth:`define`."""

    name: str
    version: int
    definition: dict
    queues: list[str]

    @staticmethod
    def define(name: str, body: "Callable[[WiggleFlow], Any]", *, version: Optional[int] = None,
               default_retry: Optional[RetryPolicy] = None) -> "FlowSpec":
        """Defines a workflow as a chain of steps, as ``FlowSpec.define`` does in the Java client.

        ``body`` receives a :class:`~wiggle.flow.WiggleFlow` at the start of the flow and records
        steps on it; it runs once, here, and nothing in it executes a step::

            FlowSpec.define("order", lambda f: f
                .then_apply("validate")
                .then_filter("in-stock")
                .then_accept("notify"))

        ``version`` pins an explicit version instead of the default content hash of the graph's
        structure. With one, **you** own bumping it when the graph changes: the server overwrites
        the stored graph for a reused version, which affects instances already running on it.
        ``default_retry`` applies to every step that names no retry policy (default: retry
        forever)."""
        from .flow import _define
        return _define(name, body, version, default_retry)


_ATTRIBUTE_NAME = re.compile(r"[a-z][a-z0-9_]{0,62}")
MAX_ATTRIBUTES = 16


class AttrType:
    """The type of a declared :class:`Attribute`, and how a context value is read as one."""
    STRING = "STRING"
    LONG = "LONG"
    DECIMAL = "DECIMAL"
    BOOLEAN = "BOOLEAN"


_ATTRIBUTE_TYPES = (AttrType.STRING, AttrType.LONG, AttrType.DECIMAL, AttrType.BOOLEAN)


@dataclass(frozen=True)
class Attribute:
    """A value the workflow declares for queue criteria to test (``Task_.attr(name)``): ``path`` is
    a dotted path into the context a step receives, read as ``type`` -- an :class:`AttrType`. The
    server extracts it each time a step becomes ready; a value that is missing or does not fit the
    type is absent."""

    name: str
    type: str
    path: str

    def __post_init__(self):
        if not _ATTRIBUTE_NAME.fullmatch(self.name or ""):
            raise ValueError(f"attribute name '{self.name}' must match {_ATTRIBUTE_NAME.pattern}")
        if self.type not in _ATTRIBUTE_TYPES:
            raise ValueError(f"attribute '{self.name}' type must be one of {_ATTRIBUTE_TYPES}, "
                             f"got {self.type!r}")
        if not self.path or "" in self.path.split("."):
            raise ValueError(f"attribute '{self.name}' needs a dotted path, got '{self.path}'")

    def to_json(self) -> dict:
        return {"name": self.name, "type": self.type, "path": self.path}


def _compile(name: str, nodes: list[_Node], version: Optional[int],
             default_retry: Optional[RetryPolicy]) -> FlowSpec:
    """Turns a recorded node stream into the definition the server stores."""
    if not name or not name.strip():
        raise ValueError("workflow name is required")
    if not nodes or all(isinstance(n, (_DefaultQueue, _DeclareAttribute)) for n in nodes):
        raise ValueError(f"workflow '{name}' has no steps")

    graph = _Graph(name, name, default_retry)
    root = _Builder(graph, enclosing_join=None, is_root=True)
    root.append_nodes(nodes)
    end_id = graph.add_end()
    root.wire_open_to(end_id)
    if graph.start_node is None:
        graph.start_node = end_id

    attributes = graph.attributes
    if len(attributes) > MAX_ATTRIBUTES:
        raise ValueError(f"a workflow declares at most {MAX_ATTRIBUTES} attributes, "
                         f"got {len(attributes)}")
    if len({a.name for a in attributes}) != len(attributes):
        raise ValueError("an attribute is declared twice")

    queues = sorted(graph.queues)
    definition = {
        "name": name,
        "startNode": graph.start_node,
        "nodes": list(graph.nodes.values()),
        "queues": queues,
        "executionMode": "SERVER",
    }
    if attributes:
        definition["attributes"] = [a.to_json() for a in attributes]
    version = _check_version(version) if version is not None else _content_version(definition)
    definition = {"version": version, **definition}
    return FlowSpec(name, version, definition, queues)


class _Graph:
    """The accumulating node store shared by a workflow and all its nested branches."""

    def __init__(self, name: str, default_queue: str, default_retry: Optional[RetryPolicy]):
        self.name = name
        self.default_queue = default_queue
        self.default_retry = default_retry or RetryPolicy.forever()
        self.attributes: list[Attribute] = []
        self.nodes: dict[str, dict] = {}
        self.queues: set[str] = set()
        self.reserved: set[str] = set()
        self.start_node: Optional[str] = None
        self._counter = 0

    def _nid(self, prefix: str = "n") -> str:
        self._counter += 1
        return f"{prefix}{self._counter}"

    def _reserve(self, name: str) -> None:
        if not name:
            raise ValueError("a step is missing a name")
        if name in self.reserved:
            raise ValueError(f"duplicate step name '{name}'")
        self.reserved.add(name)

    def add_worker(self, kind: str, name: str, queue: Optional[str], retry: Optional[RetryPolicy]) -> str:
        self._reserve(name)
        nid = self._nid()
        q = queue or self.default_queue
        self.queues.add(q)
        self.nodes[nid] = {"id": nid, "kind": kind, "name": name, "activity": f"{self.name}#{name}",
                           "queue": q, "retry": (retry or self.default_retry).to_json()}
        return nid

    def add_timer(self, kind: str, name: str, millis: int, reserve: bool) -> str:
        if reserve:
            self._reserve(name)
        nid = self._nid()
        node = {"id": nid, "kind": kind, "name": name}
        if millis > 0:
            node["sleepMillis"] = millis
        self.nodes[nid] = node
        return nid

    def add_fork(self) -> str:
        nid = self._nid("fork")
        self.nodes[nid] = {"id": nid, "kind": "FORK", "name": nid}
        return nid

    def add_subworkflow(self, name: str, child_workflow: str) -> str:
        self._reserve(name)
        nid = self._nid("sub")
        # the child workflow's name travels in `activity`; the engine starts it (no worker handler)
        self.nodes[nid] = {"id": nid, "kind": "SUB_WORKFLOW", "name": name, "activity": child_workflow}
        return nid

    def add_dynfork(self, name: str, items_key: str, item_key: str) -> str:
        self._reserve(name)
        nid = self._nid("dynfork")
        self.nodes[nid] = {"id": nid, "kind": "DYN_FORK", "name": name,
                           "itemsKey": items_key, "itemKey": item_key}
        return nid

    def add_combine(self, name: str, arms: list[str], queue: Optional[str],
                    retry: Optional[RetryPolicy]) -> str:
        """The mandatory merge node after a fork's join: a TASK bound by name like any step, carrying
        the fork's ``armNames`` so the engine can stage each isolated branch's result for the
        handler, and strip it afterward."""
        nid = self.add_worker("TASK", name, queue, retry)
        self.nodes[nid]["armNames"] = list(arms)
        return nid

    def add_for_each_combine(self, name: str, for_each_name: str, queue: Optional[str],
                             retry: Optional[RetryPolicy]) -> str:
        """The mandatory merge node after a forEach's join: a TASK bound by name whose ``collectKey``
        is the scratch key the engine stages the collected item results under."""
        nid = self.add_worker("TASK", name, queue, retry)
        self.nodes[nid]["collectKey"] = FOR_EACH_SCRATCH_PREFIX + for_each_name
        return nid

    def add_spawn_combine(self, name: str, step_name: str, queue: Optional[str],
                          retry: Optional[RetryPolicy]) -> str:
        """The combine after a spawning step: a TASK bound by name whose ``collectKey`` stages the
        results of the branches ``step_name`` created."""
        nid = self.add_worker("TASK", name, queue, retry)
        self.nodes[nid]["collectKey"] = SPAWN_SCRATCH_PREFIX + step_name
        return nid

    def add_join(self, expected: int) -> str:
        nid = self._nid("join")
        node = {"id": nid, "kind": "JOIN", "name": nid}
        if expected > 0:                       # dynamic joins carry their width in the runtime group
            node["expected"] = expected
        self.nodes[nid] = node
        return nid

    def add_end(self, reason: Optional[str] = None) -> str:
        nid = self._nid("end")
        node = {"id": nid, "kind": "END", "success": True}
        if reason is not None:
            node["reason"] = reason
        self.nodes[nid] = node
        return nid

    def wire(self, node_id: str, edge: str, target: str) -> None:
        self.nodes[node_id]["next" if edge == "next" else "altNext"] = target

    def set_branches(self, fork_id: str, starts: list[str]) -> None:
        self.nodes[fork_id]["branches"] = list(starts)


class _Builder:
    """Open-ends wiring over a node stream. One per node stream (the root, plus one per
    nested branch / case / body); they all share a single :class:`_Graph`."""

    def __init__(self, graph: _Graph, enclosing_join: Optional[str], is_root: bool = False):
        self.g = graph
        self.enclosing_join = enclosing_join
        self.is_root = is_root
        self.open: list[tuple[str, str]] = []   # (node_id, "next"|"alt") ends waiting to be wired
        self.start: Optional[str] = None

    def sub(self, enclosing_join: Optional[str]) -> "_Builder":
        return _Builder(self.g, enclosing_join)

    def attach(self, node_id: str) -> None:
        """Wire the current open ends to ``node_id``; the very first node becomes this stream's start."""
        if self.open:
            for nid, edge in self.open:
                self.g.wire(nid, edge, node_id)
            self.open = []
        elif self.start is None:
            self.start = node_id
            if self.is_root:
                self.g.start_node = node_id

    def chain(self, node_id: str) -> None:
        self.attach(node_id)
        self.open = [(node_id, "next")]

    def wire_open_to(self, target: str) -> None:
        for nid, edge in self.open:
            self.g.wire(nid, edge, target)
        self.open = []

    def append_nodes(self, nodes: list[_Node]) -> None:
        for n in nodes:
            self.append_node(n)

    def append_node(self, n: _Node) -> None:
        if isinstance(n, _Task):
            nid = self.g.add_worker("TASK", n.name, n.queue, n.retry)
            if n.compensate:
                self.g.nodes[nid]["compensable"] = True
            self.chain(nid)
            if n.combine:
                join_id = self.g.add_join(0)                  # 0 = as many as the step creates
                self.chain(join_id)
                self.chain(self.g.add_spawn_combine(n.combine, n.name, n.combine_queue, n.combine_retry))
        elif isinstance(n, _Gate):
            nid = self.g.add_worker("PREDICATE", n.name, n.queue, n.retry)
            self.attach(nid)
            target = self.enclosing_join if self.enclosing_join is not None else self.g.add_end(f"gated:{n.name}")
            self.g.wire(nid, "alt", target)
            self.open = [(nid, "next")]
        elif isinstance(n, _Sleep):
            self.chain(self.g.add_timer("SLEEP", n.name, n.millis, reserve=False))
        elif isinstance(n, _AwaitSignal):
            self._append_await_signal(n)
        elif isinstance(n, _SubWorkflow):
            self.chain(self.g.add_subworkflow(n.name, n.workflow))
        elif isinstance(n, _Fork):
            self._append_fork(n)
        elif isinstance(n, _ForEach):
            self._append_for_each(n)
        elif isinstance(n, _Choose):
            self._append_choose(n)
        elif isinstance(n, _DoWhile):
            self._append_do_while(n)
        elif isinstance(n, _DefaultQueue):
            self.g.default_queue = n.queue
        elif isinstance(n, _DeclareAttribute):
            self.g.attributes.append(n.attribute)
        else:
            raise TypeError(f"not a workflow node: {n!r}")

    def _append_await_signal(self, n: _AwaitSignal) -> None:
        nid = self.g.add_timer("SIGNAL", n.name, int(n.timeout_s * 1000), reserve=True)
        self.attach(nid)
        if n.escalation is None:
            self.open = [(nid, "next")]                       # delivery path only
            return
        if n.timeout_s <= 0:
            raise ValueError("then_await escalation needs a positive timeout_s")
        # delivery path is `next`; the escalation branch hangs off `alt` and its tail rejoins, so both
        # continue to whatever follows -- the SIGNAL next/altNext shape the Java client emits.
        sub = self.sub(self.enclosing_join)
        sub.append_nodes(n.escalation)
        if sub.start is None:
            raise ValueError(f"escalation branch of '{n.name}' defines no steps")
        self.g.wire(nid, "alt", sub.start)
        self.open = [(nid, "next"), *sub.open]

    def _append_fork(self, n: _Fork) -> None:
        if len(n.arms) < 2:
            raise ValueError("all_of needs at least two arms")
        if not n.combine:
            raise ValueError("all_of needs a combine: arms rejoin at an explicit merge handler; "
                             "there is no implicit fold")
        fork_id = self.g.add_fork()
        self.attach(fork_id)
        join_id = self.g.add_join(len(n.arms))
        starts = [self._build_branch(arm, steps, join_id) for arm, steps in n.arms]
        self.g.set_branches(fork_id, starts)
        combine_id = self.g.add_combine(n.combine, [arm for arm, _ in n.arms],
                                        n.combine_queue, n.combine_retry)
        self.g.wire(join_id, "next", combine_id)
        self.open = [(combine_id, "next")]

    def _append_for_each(self, n: _ForEach) -> None:
        if not n.combine:
            raise ValueError(f"then_for_each '{n.name}' needs a combine: item results rejoin at an "
                             f"explicit merge handler; there is no implicit fold")
        fork_id = self.g.add_dynfork(n.name, n.items_key, n.items_key)
        self.attach(fork_id)
        join_id = self.g.add_join(0)                          # 0 = dynamic width
        template_start = self._build_branch(n.name, n.body, join_id)
        self.g.set_branches(fork_id, [template_start])
        self.g.wire(fork_id, "next", join_id)   # empty-collection skip (past the combine, engine-side)
        combine_id = self.g.add_for_each_combine(n.combine, n.name, n.combine_queue, n.combine_retry)
        self.g.wire(join_id, "next", combine_id)
        self.open = [(combine_id, "next")]

    def _append_do_while(self, n: _DoWhile) -> None:
        sub = self.sub(self.enclosing_join)
        sub.append_nodes(n.body)
        if sub.start is None:
            raise ValueError(f"the body of repeat_while '{n.condition}' defines no steps")
        if n.max_iterations < 0:
            raise ValueError(f"repeat_while '{n.condition}': max_iterations must be positive (0 = engine default)")
        cond_id = self.g.add_worker("PREDICATE", n.condition, n.queue, None)
        # -1 = the engine-default budget sentinel, mirroring the Java client's two-arg doWhile.
        self.g.nodes[cond_id]["loopBudget"] = n.max_iterations if n.max_iterations > 0 else -1
        self.attach(sub.start)                                # enter at the body
        sub.wire_open_to(cond_id)                             # body tail -> condition
        self.g.wire(cond_id, "next", sub.start)               # true: loop back to the body
        self.open = [(cond_id, "alt")]                        # false: continue onward

    def _append_choose(self, n: _Choose) -> None:
        cases = n.cases
        has_default = _validate_choose(cases)
        n_guards = len(cases) - (1 if has_default else 0)

        guard_ids = [self.g.add_worker("PREDICATE", cases[i].when, cases[i].queue, cases[i].retry)
                     for i in range(n_guards)]
        self.attach(guard_ids[0])
        self.open = []
        for i in range(n_guards - 1):                         # each guard's false path -> next guard
            self.g.wire(guard_ids[i], "alt", guard_ids[i + 1])
        for i in range(n_guards):                             # each guard's true path -> its branch
            self._collect_case(cases[i], guard_ids[i], "next")

        last = guard_ids[-1]
        if has_default:
            self._collect_case(cases[-1], last, "alt")
        else:
            self.open.append((last, "alt"))                   # no match skips the choose entirely

    def _build_branch(self, name: str, steps: list[_Node], join_id: str) -> str:
        sub = self.sub(join_id)
        sub.append_nodes(steps)
        if sub.start is None:
            raise ValueError(f"branch '{name}' defines no steps")
        sub.wire_open_to(join_id)
        return sub.start

    def _collect_case(self, case: _Case, guard_id: str, edge: str) -> None:
        sub = self.sub(self.enclosing_join)
        sub.append_nodes(case.then)
        if sub.start is None:
            raise ValueError("an arm of one_of defines no steps")
        self.g.wire(guard_id, edge, sub.start)
        self.open.extend(sub.open)


def _validate_choose(cases: list[_Case]) -> bool:
    if not cases:
        raise ValueError("one_of needs at least one arm")
    for c in cases[:-1]:
        if c.when is None:
            raise ValueError("otherwise() must be the last arm of one_of")
    has_default = cases[-1].when is None
    if has_default and len(cases) == 1:
        raise ValueError("one_of needs an arm opened with when(...)")
    return has_default


_MAX_VERSION = 0x7FFFFFFF


def _check_version(version: int) -> int:
    if not isinstance(version, int) or isinstance(version, bool):
        raise TypeError("version must be an int")
    if not 1 <= version <= _MAX_VERSION:
        raise ValueError(f"version must be in 1..{_MAX_VERSION}")
    return version


def _edges(node: dict) -> list:
    return [node.get("next"), node.get("altNext"), *node.get("branches", [])]


def _content_version(definition: dict) -> int:
    """A deterministic, positive 31-bit content hash of the graph's *structure* (name, node contents,
    edge topology, execution mode) -- independent of the incidental node-id numbering. The same
    structure always hashes the same, so re-registering is idempotent and the server de-duplicates it.

    Node ids are relabelled by a deterministic breadth-first walk from the start node (edges visited
    as next, altNext, then branches in order), so changing how ids are minted never changes the hash.
    """
    nodes = {n["id"]: n for n in definition["nodes"]}
    canon: dict[str, str] = {}
    queue = deque([definition["startNode"]])
    while queue:
        nid = queue.popleft()
        if nid in canon or nid not in nodes:
            continue
        canon[nid] = f"c{len(canon)}"
        for target in _edges(nodes[nid]):
            if target is not None and target in nodes and target not in canon:
                queue.append(target)
    for nid in sorted(nodes):                        # any unreachable nodes, deterministically
        canon.setdefault(nid, f"c{len(canon)}")

    canon_nodes = []
    for nid, label in sorted(canon.items(), key=lambda kv: int(kv[1][1:])):
        n = dict(nodes[nid])
        n["id"] = label
        if "next" in n:
            n["next"] = canon[n["next"]]
        if "altNext" in n:
            n["altNext"] = canon[n["altNext"]]
        if "branches" in n:
            n["branches"] = [canon[b] for b in n["branches"]]
        canon_nodes.append(n)

    material = {
        "name": definition["name"],
        "startNode": canon[definition["startNode"]],
        "nodes": canon_nodes,
        "executionMode": definition["executionMode"],
    }
    if "attributes" in definition:
        material["attributes"] = definition["attributes"]
    canonical = json.dumps(material, sort_keys=True, separators=(",", ":"))
    digest = hashlib.sha256(canonical.encode("utf-8")).digest()
    v = ((digest[0] & 0x7F) << 24) | (digest[1] << 16) | (digest[2] << 8) | digest[3]
    return v or 1
