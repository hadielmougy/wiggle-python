"""The data plane: a worker pulls tasks it has capacity for, runs the matching handler, and reports
the result. Workers hold no durable state -- a crash loses at most the in-flight step, which the
server re-leases and re-runs."""
from __future__ import annotations

import inspect
import logging
import os
import re
import threading
import time
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass
from typing import Any, Callable, Iterable, Optional

import grpc

from ._convert import from_value
from . import step as _step
from .client import WiggleClient
from .workflow import Activity, Predicate, SideEffect

log = logging.getLogger("wiggle.worker")


class PermanentError(Exception):
    """Raise from a handler to fail the step without retrying (a bad request, not a blip)."""


class Handlers:
    """Optional base for a class whose methods implement a workflow's steps -- one public method per
    step, each taking the context. Register an instance with :meth:`Worker.register_handlers`.

    Method names match step names **regardless of case style**: ``in_stock``, ``inStock``, and a graph
    step named ``in-stock`` all normalise the same. The method's return annotation picks the kind:
    ``-> bool`` binds a gate (predicate), ``-> None`` a side effect, anything else (or no annotation) a
    task. Prefix helper methods with ``_`` so they are not treated as step handlers.

    Set the class attribute ``workflow`` to the workflow name these handlers implement, or pass
    ``workflow=`` to :meth:`Worker.register_handlers`.
    """

    workflow: Optional[str] = None


# tokens split on any non-alphanumeric run, and on camelCase / acronym boundaries
_NON_ALNUM = re.compile(r"[^0-9A-Za-z]+")
_CAMEL_BOUNDARY = re.compile(r"(?<=[a-z0-9])(?=[A-Z])")
_ACRONYM_BOUNDARY = re.compile(r"(?<=[A-Z])(?=[A-Z][a-z])")


def _canonical(name: str) -> str:
    """Fold a name to a case/style-independent key: lowercase alphanumeric tokens, concatenated. So
    ``in-stock``, ``in_stock``, ``inStock``, ``InStock``, and ``instock`` all yield ``instock``."""
    spaced = _NON_ALNUM.sub(" ", name)
    spaced = _ACRONYM_BOUNDARY.sub(" ", _CAMEL_BOUNDARY.sub(" ", spaced))
    return "".join(tok.lower() for tok in spaced.split())


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


@dataclass
class _Candidate:
    name: str                 # the original method name, for error messages
    method: Callable
    return_annotation: Any    # inspect return annotation (drives the kind)



def _with_combine_base(wrapper, items_key_json):
    """Wraps a combine handler so wiggle.step.base() works inside it: the base is the staged
    context minus the scratch key(s) (a forEach's collected-results key, or a fork's arm names)."""
    import json as _json
    parsed = _json.loads(items_key_json)
    scratch_keys = [parsed] if isinstance(parsed, str) else list(parsed)

    def wrapped(ctx):
        base = {k: v for k, v in ctx.items() if k not in scratch_keys} if isinstance(ctx, dict) else ctx
        token = _step._begin(base, 0, None, item=False)
        try:
            return wrapper(ctx)
        finally:
            _step._end(token)
    return wrapped


def _collect_handler_methods(handlers: object) -> dict[str, _Candidate]:
    """Introspect ``handlers`` for its step methods, keyed by canonical name. Raises if two public
    methods fold to the same canonical name (ambiguous across case styles), or if a public method does
    not take a single context argument."""
    found: dict[str, _Candidate] = {}
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
        canon = _canonical(attr)
        if not canon:
            continue
        if canon in found:
            raise ValueError(f"handler methods '{found[canon].name}' and '{attr}' both map to the "
                             f"same step name '{canon}'; names differing only in case/style are "
                             f"ambiguous -- rename one")
        found[canon] = _Candidate(attr, member, sig.return_annotation)
    if not found:
        raise ValueError("no handler methods found on the object (public methods taking one context "
                         "argument)")
    return found


class Worker:
    """Bind handlers to workflow steps by name, then :meth:`start` to pull and run work. Topology is
    registered separately (``client.register`` of a compiled :class:`Graph`, or the ``wiggle`` CLI).

    >>> worker = Worker(client, "worker-1").handle("order", "validate", validate)
    >>> worker.start()          # background threads; call stop() to drain
    """

    def __init__(self, client: WiggleClient, worker_id: str, *,
                 concurrency: Optional[int] = None, lease_s: float = 30.0,
                 long_poll_wait_s: float = 10.0, idle_backoff_s: float = 0.2,
                 error_backoff_s: float = 2.0, queues: Optional[Iterable[str]] = None,
                 await_registration_s: float = 0.0):
        self._client = client
        self.worker_id = worker_id
        self._concurrency = concurrency or os.cpu_count() or 4
        self._lease_ms = int(lease_s * 1000)
        self._wait_ms = int(long_poll_wait_s * 1000)
        self._idle_backoff = idle_backoff_s
        self._error_backoff = error_backoff_s
        self._explicit_queues = set(queues) if queues else None
        self._await_registration_s = await_registration_s

        self._handlers: dict[str, Callable] = {}
        self._queues: set[str] = set()
        self._claims: list[tuple[str, str, str]] = []   # (workflow, step, expected NodeKind)
        self._handler_sets: list[tuple[str, dict[str, "_Candidate"]]] = []  # (workflow, candidates)
        self._executor: Optional[ThreadPoolExecutor] = None
        self._poll_thread: Optional[threading.Thread] = None
        self._running = threading.Event()
        self._inflight = 0
        self._lock = threading.Lock()

    def handle(self, workflow: str, step: str, fn: Activity) -> "Worker":
        """Bind a handler to one step of an already-registered workflow, by name -- no topology
        re-declaration. The graph lives on the server; this worker just implements ``step``. ``fn``
        takes the context and returns the new context — sent whole, it REPLACES the previous
        context server-side (no diff, no merge; ``None`` leaves it untouched). Which queue the step polls is discovered from the graph on
        :meth:`start`, which also fails fast if ``step`` does not exist (or is the wrong kind)."""
        def wrapper(ctx):
            return fn(ctx)
        return self._bind(workflow, step, "TASK", wrapper)

    def handle_combine(self, workflow: str, step: str, fn: Activity) -> "Worker":
        """Bind the mandatory merge step that follows a fork's join. ``fn`` receives the context
        with each branch's result staged under the branch's name, and must return the COMPLETE
        post-join context: it is sent verbatim (no diff, no implicit fold) and the engine REPLACES
        the context with it -- keys ``fn`` omits do not survive the join."""
        def wrapper(ctx):
            return fn(ctx)
        return self._bind(workflow, step, "COMBINE", wrapper)

    def handle_gate(self, workflow: str, step: str, test: Predicate) -> "Worker":
        """Bind a predicate (gate / choose guard / do-while condition) step by name; ``test`` returns
        a bool. See :meth:`handle`."""
        return self._bind(workflow, step, "PREDICATE", lambda ctx: bool(test(ctx)))

    def handle_effect(self, workflow: str, step: str, fn: SideEffect) -> "Worker":
        """Bind a side-effect step by name; ``fn``'s return is ignored and the context is unchanged.
        See :meth:`handle`."""
        def wrapper(ctx):
            fn(ctx)
            return None
        return self._bind(workflow, step, "TASK", wrapper)

    def register_handlers(self, handlers: object, *, workflow: Optional[str] = None) -> "Worker":
        """Bind a whole object's methods as step handlers in one call. Each public method that takes a
        context becomes a handler; on :meth:`start` the worker matches it to a step of ``workflow`` by
        **case-insensitive name** (``in_stock``/``inStock`` both match a step named ``in-stock``) and
        picks the kind from the method's return annotation (``-> bool`` gate, ``-> None`` effect, else
        task). The graph is the source of truth for the exact step name and whether it is a gate.

        ``workflow`` may be passed here or set as a ``workflow`` attribute on the object (e.g. a
        :class:`Handlers` subclass). Two methods whose names collide under case-folding are rejected
        here (ambiguous); a method matching no step, or a kind clash with the graph, is caught on
        :meth:`start` during reconciliation.
        """
        wf = workflow or getattr(handlers, "workflow", None)
        if not wf:
            raise ValueError("register_handlers needs a workflow name: pass workflow=... or set a "
                             "'workflow' attribute on the handlers object")
        candidates = _collect_handler_methods(handlers)   # validates case-fold duplicates
        self._handler_sets.append((wf, candidates))
        return self

    def _bind(self, workflow: str, step: str, kind: str, wrapper: Callable) -> "Worker":
        if not workflow or not step:
            raise ValueError("workflow and step are required")
        activity = f"{workflow}#{step}"
        if activity in self._handlers:
            raise ValueError(f"duplicate handler for activity '{activity}'")
        self._handlers[activity] = wrapper
        self._claims.append((workflow, step, kind))
        return self

    @property
    def _served_queues(self) -> set[str]:
        return self._explicit_queues if self._explicit_queues is not None else self._queues

    def start(self) -> "Worker":
        if self._running.is_set():
            return self
        if self._claims or self._handler_sets:
            self._reconcile()
        self._running.set()
        self._executor = ThreadPoolExecutor(max_workers=self._concurrency,
                                            thread_name_prefix=f"wiggle-{self.worker_id}")
        self._poll_thread = threading.Thread(target=self._poll_loop,
                                             name=f"wiggle-poll-{self.worker_id}", daemon=True)
        self._poll_thread.start()
        log.info("worker %s polling queues %s with concurrency %d",
                 self.worker_id, sorted(self._served_queues), self._concurrency)
        return self

    def stop(self, timeout_s: float = 10.0) -> None:
        self._running.clear()
        if self._executor:
            self._executor.shutdown(wait=True, cancel_futures=False)
        if self._poll_thread:
            self._poll_thread.join(timeout=timeout_s)

    def __enter__(self) -> "Worker":
        return self.start()

    def __exit__(self, *exc) -> None:
        self.stop()

    def run_forever(self) -> None:
        """Start and block until interrupted (Ctrl-C)."""
        self.start()
        try:
            while self._running.is_set():
                self._running.wait(1.0)
        except KeyboardInterrupt:
            pass
        finally:
            self.stop()

    # ---- internals ----

    def _reconcile(self) -> None:
        """Check every :meth:`handle`-bound claim against the server's registered graph, and learn
        the queue each claimed step polls (a name-only binding has no other way to know it). Fails
        fast on a typo'd step name or a kind mismatch, so a bad binding is caught at start rather
        than as a silent runtime "no handler" much later."""
        for wf in sorted({w for w, _, _ in self._claims}):
            graph = self._fetch_graph(wf)
            nodes = {n["activity"]: n for n in graph.get("nodes", [])
                     if n.get("kind") in ("TASK", "PREDICATE") and "activity" in n}
            served: set[str] = set()
            for w, step, kind in self._claims:
                if w != wf:
                    continue
                activity = f"{w}#{step}"
                node = nodes.get(activity)
                if node is None:
                    avail = sorted(a.split("#", 1)[1] for a in nodes)
                    raise ValueError(f"no step '{step}' in registered workflow '{wf}' "
                                     f"(available steps: {avail})")
                is_combine = node["kind"] == "TASK" and node.get("itemsKey")
                if kind == "COMBINE" and not is_combine:
                    raise ValueError(f"activity '{activity}' is not a fork combine; bind it with handle()")
                elif kind == "COMBINE":
                    # Expose the frozen base ambiently inside the combine: wiggle.step.base() and
                    # popping the scratch key from the raw dict are both valid access styles.
                    self._handlers[activity] = _with_combine_base(
                        self._handlers[activity], node["itemsKey"])
                elif is_combine:
                    raise ValueError(f"activity '{activity}' is a fork combine; bind it with "
                                     f"handle_combine() -- its return is the complete post-join "
                                     f"context (there is no implicit fold)")
                elif node["kind"] != kind:
                    verb = "handle_gate" if node["kind"] == "PREDICATE" else "handle"
                    raise ValueError(f"activity '{activity}' is a {node['kind']} in the graph but was "
                                     f"bound as {kind}; use {verb}() instead")
                self._queues.add(node.get("queue", wf))
                served.add(activity)
            unclaimed = sorted(a.split("#", 1)[1] for a in set(nodes) - served)
            if unclaimed:   # info, not an error: a polyglot worker may intentionally serve a subset
                log.info("workflow '%s' has steps served by no handler on this worker: %s", wf, unclaimed)
        for wf, candidates in self._handler_sets:
            self._match_handler_set(wf, candidates)

    def _match_handler_set(self, wf: str, candidates: dict[str, "_Candidate"]) -> None:
        """Match a :meth:`register_handlers` object's methods to ``wf``'s steps by canonical name, then
        bind each with a wrapper whose kind follows the graph (gate vs task) and the method's return
        annotation (task vs effect). A method matching no step, or a kind clash, fails fast."""
        graph = self._fetch_graph(wf)
        nodes = {n["name"]: n for n in graph.get("nodes", [])
                 if n.get("kind") in ("TASK", "PREDICATE") and "name" in n}
        by_canon: dict[str, tuple[str, dict]] = {}
        for name, node in nodes.items():
            by_canon.setdefault(_canonical(name), (name, node))   # graph step names are already unique
        served: set[str] = set()
        for canon, cand in candidates.items():
            match = by_canon.get(canon)
            if match is None:
                avail = sorted(nodes)
                raise ValueError(f"handler '{cand.name}' matches no step in workflow '{wf}' "
                                 f"(available steps: {avail})")
            step_name, node = match
            activity = f"{wf}#{step_name}"
            if activity in self._handlers:
                raise ValueError(f"duplicate handler for activity '{activity}'")
            self._handlers[activity] = self._wrapper_for(cand, node, activity)
            self._queues.add(node.get("queue", wf))
            served.add(step_name)
        unclaimed = sorted(set(nodes) - served)
        if unclaimed:   # info, not an error: a polyglot worker may intentionally serve a subset
            log.info("workflow '%s' has steps served by no handler on this worker: %s", wf, unclaimed)

    @staticmethod
    def _wrapper_for(cand: "_Candidate", node: dict, activity: str) -> Callable:
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
        if node.get("itemsKey"):
            # A fork combine: the return is the COMPLETE post-join context, sent verbatim (no
            # diff) -- the engine replaces the context with it; there is no implicit fold.
            if ret is bool or ret is None:
                raise ValueError(f"activity '{activity}' is a fork combine; handler '{cand.name}' "
                                 f"must return the complete post-join context (a dict), not {ret!r}")
            def combine_wrapper(ctx):
                return method(ctx)
            return _with_combine_base(combine_wrapper, node["itemsKey"])
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

    def _fetch_graph(self, workflow: str) -> dict:
        """Fetch the registered graph, optionally waiting out a registration race (the authoring
        client may still be starting up). Beyond the grace window, a missing graph is fatal."""
        deadline = time.monotonic() + self._await_registration_s
        while True:
            try:
                return self._client.get_workflow(workflow)
            except grpc.RpcError as e:
                not_found = e.code() == grpc.StatusCode.NOT_FOUND
                if not_found and time.monotonic() < deadline:
                    self._running.wait(0.25)
                    continue
                if not_found:
                    raise ValueError(
                        f"workflow '{workflow}' is not registered; register its graph before "
                        f"starting a worker that binds handlers to it (or pass await_registration_s)") from e
                raise

    def _poll_loop(self) -> None:
        while self._running.is_set():
            try:
                self._poll_once()
            except Exception as e:  # noqa: BLE001 - a poll failure must not kill the loop
                if not self._running.is_set():
                    return
                log.warning("poll failed: %s", e)
                self._running.wait(self._error_backoff)

    def _poll_once(self) -> None:
        with self._lock:
            free = self._concurrency - self._inflight
        if free <= 0:
            self._running.wait(self._idle_backoff)
            return
        result = self._client.poll(self.worker_id, self._served_queues, free,
                                   self._lease_ms, self._wait_ms)
        if not result.tasks:
            # honour the server's backpressure hold-off if it is shedding load
            backoff = (result.retry_after_millis / 1000) if result.retry_after_millis > 0 else self._idle_backoff
            self._running.wait(backoff)
            return
        for task in result.tasks:
            with self._lock:
                self._inflight += 1
            self._executor.submit(self._run, task)

    def _run(self, task) -> None:
        try:
            self._execute(task)
        finally:
            with self._lock:
                self._inflight -= 1

    def _execute(self, task) -> None:
        handler = self._handlers.get(task.activity)
        if handler is None:
            self._client.fail(task.task_id, task.lease_owner,
                              f"no handler registered for activity '{task.activity}'", retryable=False)
            return

        stop_heartbeat = self._start_heartbeat(task)
        scope = None
        if task.HasField("base_context"):
            # A forEach item step: the handler's parameter is the ITEM's value (scalars included);
            # the frozen base and the element's index/source key ride on wiggle.step.
            scope = _step._begin(from_value(task.base_context), task.item_index,
                                 task.item_map_key or None)
        try:
            result = handler(from_value(task.context))
            if task.kind == "PREDICATE":
                self._client.complete(task.task_id, task.lease_owner, {"value": bool(result)})
            else:
                self._client.complete(task.task_id, task.lease_owner, result)
        except PermanentError as e:
            self._client.fail(task.task_id, task.lease_owner, str(e), retryable=False)
        except Exception as e:  # noqa: BLE001 - a handler failure is a step failure, retried
            log.debug("step %s of %s failed: %s", task.step_name, task.instance_id, e)
            self._client.fail(task.task_id, task.lease_owner, f"{type(e).__name__}: {e}", retryable=True)
        finally:
            if scope is not None:
                _step._end(scope)
            stop_heartbeat.set()

    def _start_heartbeat(self, task) -> threading.Event:
        """Extend the lease periodically while a handler runs, so a slow step keeps its claim."""
        stop = threading.Event()
        interval = max(self._lease_ms / 2000, 1.0)   # half the lease, in seconds

        def beat() -> None:
            while not stop.wait(interval):
                try:
                    self._client.heartbeat(task.task_id, task.lease_owner, self._lease_ms)
                except Exception:  # noqa: BLE001 - the step will just fail/retry if the lease is lost
                    return

        threading.Thread(target=beat, name=f"wiggle-hb-{task.task_id}", daemon=True).start()
        return stop
