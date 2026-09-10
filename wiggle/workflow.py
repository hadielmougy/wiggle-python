"""The workflow topology as declarative data: describe a graph of steps, gates, timers, signal waits,
parallel ``Fork`` and exclusive ``Choose`` as a :class:`Graph` of :class:`Node` values that mirror the
YAML/graph schema, then :meth:`Graph.compile` it into a :class:`Blueprint` you register and start.

Handlers are **not** part of the topology -- a :class:`~wiggle.worker.Worker` binds them separately, by
name (``handle`` / ``handle_gate`` / ``handle_effect``). The graph lives on the server; any worker that
implements the matching activity names can run its steps, so Python, Java, and Go workers interoperate.

The context is a plain ``dict`` that flows through the steps. A handler returns the *whole* context
(usually ``{**ctx, ...}``); the engine merges only what changed, so parallel branches that touch
different fields merge cleanly.

    from wiggle import Graph, Step, Gate, Effect, Fork, Branch, Retry

    wf = Graph("order", [
        Step("validate"),
        Gate("in-stock"),
        Fork([
            Branch("payment",  [Step("charge", queue="payments", retry=Retry.exponential(5, 0.1))]),
            Branch("shipping", [Step("reserve"), Step("label")]),
        ]),
        Effect("notify"),
    ]).compile()
"""
from __future__ import annotations

import hashlib
import json
from collections import deque
from dataclasses import dataclass, field
from typing import Any, Callable, Optional, Union

_MAX_INT = 2_147_483_647

Context = dict
Activity = Callable[[Context], Context]      # task handler: ctx -> new ctx
SideEffect = Callable[[Context], Any]        # effect handler: ctx -> (ignored)
Predicate = Callable[[Context], bool]        # gate / choose guard / do-while condition: ctx -> bool


@dataclass(frozen=True)
class Retry:
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
    def forever() -> "Retry":
        return Retry(_MAX_INT, 1.0, 1.0, 60.0, 0.0)

    @staticmethod
    def none() -> "Retry":
        return Retry(1, 0.0, 1.0, 0.0, 0.0)

    @staticmethod
    def exponential(max_attempts: int, initial_backoff_s: float) -> "Retry":
        return Retry(max_attempts, initial_backoff_s, 2.0, 300.0, 0.2)

    @staticmethod
    def fixed(max_attempts: int, backoff_s: float) -> "Retry":
        return Retry(max_attempts, backoff_s, 1.0, backoff_s, 0.0)


# ---- the declarative node types (mirror the graph schema) ----


class Node:
    """Base class for a topology node. A workflow's ``steps`` is a list of these."""


@dataclass
class Step(Node):
    """A unit of work run on a worker; its handler returns the new context (only changed keys merge
    back). Bind it with ``Worker.handle``."""

    name: str
    queue: Optional[str] = None      # None -> the default queue
    retry: Optional[Retry] = None    # None -> default (retry forever)


@dataclass
class Effect(Node):
    """A step run for its side effect only; the context is unchanged. Bind it with
    ``Worker.handle_effect``. (Topologically identical to a :class:`Step`.)"""

    name: str
    queue: Optional[str] = None
    retry: Optional[Retry] = None


@dataclass
class Gate(Node):
    """A predicate: continue only while it holds; a false result ends the instance as ``gated:<name>``
    (inside a fork/choose branch it short-circuits to the enclosing join). Bind it with
    ``Worker.handle_gate``."""

    name: str
    queue: Optional[str] = None
    retry: Optional[Retry] = None


@dataclass
class Sleep(Node):
    """A server-side timer; no worker is held while the instance waits."""

    name: str
    seconds: float = 0.0
    millis: int = 0


@dataclass
class AwaitSignal(Node):
    """Wait for a named external signal (delivered via :meth:`WiggleClient.signal`); the payload merges
    into the context like a step's result. With a positive ``timeout_s`` the instance **fails** if the
    signal does not arrive in time -- unless an ``escalation`` list is given, in which case those nodes
    run instead on timeout and then rejoin the flow (exactly one of delivery / escalation happens)."""

    name: str
    timeout_s: float = 0.0
    escalation: Optional[list["Node"]] = None


@dataclass
class SubWorkflow(Node):
    """Run another registered workflow as a child: it starts with this instance's context, and on
    completion its final context merges back here. The child must be registered separately; ``workflow``
    is its name (or a :class:`Blueprint` / :class:`Graph` to take the name from)."""

    name: str
    workflow: "Union[str, Blueprint, Graph]"


@dataclass
class Branch:
    """One parallel arm of a :class:`Fork` (a name plus its own list of nodes)."""

    name: str
    steps: list["Node"]


@dataclass
class Fork(Node):
    """Fan out into parallel branches and wait for all of them (join). Needs >= 2 branches.

    ``combine`` names the MANDATORY merge step run after the join: its handler receives the context
    with each branch's result staged under the branch's name, and must return the COMPLETE
    post-join context -- the engine replaces the context with it, so keys the handler omits do not
    survive the join. There is no implicit fold of the arms. Bind it with
    :meth:`wiggle.worker.Worker.handle_combine` (or a :class:`Handlers` method matched by name)."""

    branches: list[Branch]
    combine: str = ""


@dataclass
class ForEach(Node):
    """Runtime fan-out: at run time the engine reads the collection in the context at ``over`` (a
    list or a map) and spawns one parallel branch per element, each on its own ISOLATED copy of the
    context with the element injected under ``as_`` (its index under ``as_ + "Index"`` and, for a
    map, its key under ``as_ + "Key"``). Item writes never touch the shared context. ``combine``
    names the MANDATORY merge step: its handler receives the context with every item's final
    context collected under ``name`` — a list ordered by item index for a list input, a map keyed
    like the input for a map input — and must return the COMPLETE post-join context (the engine
    replaces the context with it). An empty collection skips the body and the combine."""

    name: str
    over: str                # itemsKey: the context key holding the collection
    body: list["Node"]
    combine: str = ""


@dataclass
class Case:
    """One arm of a :class:`Choose`. ``when is None`` marks the ``otherwise`` default (which must be
    last); otherwise ``when`` is the name of the guard predicate."""

    then: list["Node"]
    when: Optional[str] = None


@dataclass
class Choose(Node):
    """Exclusive choice: the first :class:`Case` whose guard holds runs, the rest are skipped. A cascade
    of guards -- nothing runs in parallel and there is no join."""

    cases: list[Case]


@dataclass
class DoWhile(Node):
    """A do-while loop: run ``body`` once, then evaluate the ``while_`` predicate on a worker; while it
    holds, the body runs again (the body always runs at least once).

    Every loop is budgeted: the guard may evaluate true at most ``max_iterations`` times, after
    which the instance FAILS with a clear error -- an unbounded loop with a buggy condition would
    hot-spin workers and the database. ``0`` means the engine default
    (``WIGGLE_LOOP_MAX_ITERATIONS``, 10,000); set it explicitly when a loop legitimately needs
    more."""

    while_: str
    body: list["Node"]
    max_iterations: int = 0


@dataclass
class Blueprint:
    """A compiled workflow: the definition sent to the server (topology only). Handlers are bound
    separately on a worker, by name."""

    name: str
    version: int
    definition: dict
    queues: list[str]


@dataclass
class Graph:
    """A workflow's topology as declarative data. ``compile()`` validates the shape and turns it into a
    :class:`Blueprint` you register and start.

    ``version`` pins an explicit version instead of the auto content hash. Use it to choose a stable,
    human-meaningful number (or to match another client) -- but then **you** own bumping it when the
    graph changes: the server overwrites the stored graph for a reused version, which affects instances
    already running on it. Leave it unset for the safe content-addressed default.
    """

    name: str
    steps: list[Node] = field(default_factory=list)
    version: Optional[int] = None
    default_queue: Optional[str] = None

    def compile(self) -> Blueprint:
        if not self.name or not self.name.strip():
            raise ValueError("workflow name is required")
        if not self.steps:
            raise ValueError("workflow has no steps")

        graph = _Graph(self.name, self.default_queue or self.name)
        root = _Builder(graph, enclosing_join=None, is_root=True)
        root.append_nodes(self.steps)
        end_id = graph.add_end()
        root.wire_open_to(end_id)
        if graph.start_node is None:
            graph.start_node = end_id

        queues = sorted(graph.queues)
        definition = {
            "name": self.name,
            "startNode": graph.start_node,
            "nodes": list(graph.nodes.values()),
            "queues": queues,
            "executionMode": "SERVER",
        }
        version = _check_version(self.version) if self.version is not None else _content_version(definition)
        definition = {"version": version, **definition}
        return Blueprint(self.name, version, definition, queues)


class _Graph:
    """The accumulating node store shared by a workflow and all its nested branches."""

    def __init__(self, name: str, default_queue: str):
        self.name = name
        self.default_queue = default_queue
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

    def add_worker(self, kind: str, name: str, queue: Optional[str], retry: Optional[Retry]) -> str:
        self._reserve(name)
        nid = self._nid()
        q = queue or self.default_queue
        self.queues.add(q)
        self.nodes[nid] = {"id": nid, "kind": kind, "name": name, "activity": f"{self.name}#{name}",
                           "queue": q, "retry": (retry or Retry.forever()).to_json()}
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

    def add_combine(self, name: str, arms: list[str]) -> str:
        """The mandatory merge node after a fork's join: a TASK bound by name like any step, carrying
        the fork's arm names (a JSON array) on its ``itemsKey`` so the engine can stage each isolated
        branch's result under its name for the handler, and strip those keys afterward."""
        nid = self.add_worker("TASK", name, None, None)
        self.nodes[nid]["itemsKey"] = json.dumps(arms, separators=(",", ":"))
        return nid

    def add_for_each_combine(self, name: str, scratch_key: str) -> str:
        """The mandatory merge node after a forEach's join: a TASK bound by name whose ``itemsKey``
        is a JSON STRING (the scratch key the engine stages the collected item results under) —
        versus a fork combine's arm-name array."""
        nid = self.add_worker("TASK", name, None, None)
        self.nodes[nid]["itemsKey"] = json.dumps(scratch_key)
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
    """Open-ends wiring over the declarative node list. One per node stream (the root, plus one per
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

    def append_nodes(self, nodes: list[Node]) -> None:
        for n in nodes:
            self.append_node(n)

    def append_node(self, n: Node) -> None:
        if isinstance(n, Step):
            self.chain(self.g.add_worker("TASK", n.name, n.queue, n.retry))
        elif isinstance(n, Effect):
            self.chain(self.g.add_worker("TASK", n.name, n.queue, n.retry))
        elif isinstance(n, Gate):
            nid = self.g.add_worker("PREDICATE", n.name, n.queue, n.retry)
            self.attach(nid)
            target = self.enclosing_join if self.enclosing_join is not None else self.g.add_end(f"gated:{n.name}")
            self.g.wire(nid, "alt", target)
            self.open = [(nid, "next")]
        elif isinstance(n, Sleep):
            self.chain(self.g.add_timer("SLEEP", n.name, int(n.seconds * 1000) + int(n.millis), reserve=False))
        elif isinstance(n, AwaitSignal):
            self._append_await_signal(n)
        elif isinstance(n, SubWorkflow):
            self.chain(self.g.add_subworkflow(n.name, _child_name(n.workflow)))
        elif isinstance(n, Fork):
            self._append_fork(n)
        elif isinstance(n, ForEach):
            self._append_for_each(n)
        elif isinstance(n, Choose):
            self._append_choose(n)
        elif isinstance(n, DoWhile):
            self._append_do_while(n)
        else:
            raise TypeError(f"not a workflow node: {n!r}")

    def _append_await_signal(self, n: AwaitSignal) -> None:
        nid = self.g.add_timer("SIGNAL", n.name, int(n.timeout_s * 1000), reserve=True)
        self.attach(nid)
        if n.escalation is None:
            self.open = [(nid, "next")]                       # delivery path only
            return
        if n.timeout_s <= 0:
            raise ValueError("await_signal escalation needs a positive timeout_s")
        # delivery path is `next`; the escalation branch hangs off `alt` and its tail rejoins, so both
        # continue to whatever follows -- the SIGNAL next/altNext shape the Java client emits.
        sub = self.sub(self.enclosing_join)
        sub.append_nodes(n.escalation)
        if sub.start is None:
            raise ValueError(f"escalation branch of '{n.name}' defines no steps")
        self.g.wire(nid, "alt", sub.start)
        self.open = [(nid, "next"), *sub.open]

    def _append_fork(self, n: Fork) -> None:
        if len(n.branches) < 2:
            raise ValueError("fork needs at least two branches")
        if not n.combine:
            raise ValueError("fork needs a combine step name (Fork(..., combine=...)): branches "
                             "rejoin at an explicit merge handler; there is no implicit fold")
        fork_id = self.g.add_fork()
        self.attach(fork_id)
        join_id = self.g.add_join(len(n.branches))
        starts = [self._build_branch(b, join_id) for b in n.branches]
        self.g.set_branches(fork_id, starts)
        combine_id = self.g.add_combine(n.combine, [b.name for b in n.branches])
        self.g.wire(join_id, "next", combine_id)
        self.open = [(combine_id, "next")]

    def _append_for_each(self, n: ForEach) -> None:
        if not n.combine:
            raise ValueError("for_each needs a combine step name (ForEach(..., combine=...)): item "
                             "results rejoin at an explicit merge handler; there is no implicit fold")
        fork_id = self.g.add_dynfork(n.name, n.over, n.over)
        self.attach(fork_id)
        join_id = self.g.add_join(0)                          # 0 = dynamic width
        template_start = self._build_branch(Branch(n.name, n.body), join_id)
        self.g.set_branches(fork_id, [template_start])
        self.g.wire(fork_id, "next", join_id)   # empty-collection skip (past the combine, engine-side)
        combine_id = self.g.add_for_each_combine(n.combine, n.name)
        self.g.wire(join_id, "next", combine_id)
        self.open = [(combine_id, "next")]

    def _append_do_while(self, n: DoWhile) -> None:
        sub = self.sub(self.enclosing_join)
        sub.append_nodes(n.body)
        if sub.start is None:
            raise ValueError("do_while body defines no steps")
        if n.max_iterations < 0:
            raise ValueError(f"do_while '{n.while_}': max_iterations must be positive (0 = engine default)")
        cond_id = self.g.add_worker("PREDICATE", n.while_, None, None)
        # -1 = the engine-default budget sentinel, mirroring the Java client's two-arg doWhile.
        self.g.nodes[cond_id]["loopBudget"] = n.max_iterations if n.max_iterations > 0 else -1
        self.attach(sub.start)                                # enter at the body
        sub.wire_open_to(cond_id)                             # body tail -> condition
        self.g.wire(cond_id, "next", sub.start)               # true: loop back to the body
        self.open = [(cond_id, "alt")]                        # false: continue onward

    def _append_choose(self, n: Choose) -> None:
        cases = n.cases
        has_default = _validate_choose(cases)
        n_guards = len(cases) - (1 if has_default else 0)

        guard_ids = [self.g.add_worker("PREDICATE", cases[i].when, None, None) for i in range(n_guards)]
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

    def _build_branch(self, branch: Branch, join_id: str) -> str:
        sub = self.sub(join_id)
        sub.append_nodes(branch.steps)
        if sub.start is None:
            raise ValueError(f"branch '{branch.name}' defines no steps")
        sub.wire_open_to(join_id)
        return sub.start

    def _collect_case(self, case: Case, guard_id: str, edge: str) -> None:
        sub = self.sub(self.enclosing_join)
        sub.append_nodes(case.then)
        if sub.start is None:
            raise ValueError("a choose case defines no steps")
        self.g.wire(guard_id, edge, sub.start)
        self.open.extend(sub.open)


def _validate_choose(cases: list[Case]) -> bool:
    if not cases:
        raise ValueError("choose needs at least one case")
    for c in cases[:-1]:
        if c.when is None:
            raise ValueError("the otherwise (default) case must be last")
    has_default = cases[-1].when is None
    if has_default and len(cases) == 1:
        raise ValueError("choose needs at least one guarded case")
    return has_default


def _child_name(workflow: "Union[str, Blueprint, Graph]") -> str:
    if isinstance(workflow, str):
        child = workflow
    elif isinstance(workflow, (Blueprint, Graph)):
        child = workflow.name
    else:
        raise TypeError("workflow must be a Graph, Blueprint, or workflow name")
    if not child:
        raise ValueError("child workflow name is required")
    return child


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
    canonical = json.dumps(material, sort_keys=True, separators=(",", ":"))
    digest = hashlib.sha256(canonical.encode("utf-8")).digest()
    v = ((digest[0] & 0x7F) << 24) | (digest[1] << 16) | (digest[2] << 8) | digest[3]
    return v or 1
