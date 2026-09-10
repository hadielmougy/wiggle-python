"""The data plane: a worker pulls tasks it has capacity for, runs the matching handler, and reports
the result. Workers hold no durable state -- a crash loses at most the in-flight step, which the
server re-leases and re-runs."""
from __future__ import annotations

import logging
import os
import re
import threading
import time
from concurrent.futures import ThreadPoolExecutor
from typing import Any, Callable, Iterable, Optional

import grpc

from ._convert import from_value
from . import step as _step
from . import _binder
from ._binder import Compensation  # noqa: F401  (re-exported: wiggle.Compensation)
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


_canonical = _binder.canonical   # re-exported: step-name folding lives in wiggle._binder


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
        self._handler_sets: list[tuple[str, dict[str, "_binder.Candidate"]]] = []  # (workflow, candidates)
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

    def handle_compensation(self, workflow: str, step: str, fn: Callable) -> "Worker":
        """Bind the undo of a compensable step (one declared ``compensate=True`` in the topology).
        ``fn`` receives a :class:`wiggle.Compensation` (the step's input/result snapshots) and runs
        in the reverse pass after the instance fails. ``start`` refuses a worker that serves a
        compensable step's forward handler without also binding its compensator — the undo task is
        minted on the same queue, and a worker that can do but not undo would strand the reverse
        pass."""
        if not workflow or not step:
            raise ValueError("workflow and step are required")
        activity = f"{workflow}#{step}#compensate"
        if activity in self._handlers:
            raise ValueError(f"duplicate compensator for activity '{activity}'")
        self._handlers[activity] = _binder._compensation_wrapper(fn)
        self._claims.append((workflow, step, "COMPENSATE"))
        return self

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
        candidates = _binder.scan(handlers)   # validates case-fold duplicates
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
                if kind == "COMPENSATE":
                    if node["kind"] != "TASK" or is_combine or not node.get("compensable"):
                        raise ValueError(f"step '{step}' of workflow '{wf}' is not declared "
                                         f"compensate=True in the topology; handle_compensation "
                                         f"binds only compensable steps")
                elif kind == "COMBINE" and not is_combine:
                    raise ValueError(f"activity '{activity}' is not a fork combine; bind it with handle()")
                elif kind == "COMBINE":
                    # Expose the frozen base ambiently inside the combine: wiggle.step.base() and
                    # popping the scratch key from the raw dict are both valid access styles.
                    self._handlers[activity] = _binder.with_combine_base(
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
            # Pairing: a forward handler on a compensable step requires its compensator here too —
            # the undo task lands on the same queue this worker polls, so "can do but not undo"
            # would strand the reverse pass at claim time.
            compensated = {step for w, step, kind in self._claims
                           if w == wf and kind == "COMPENSATE"}
            for w, step, kind in self._claims:
                if w != wf or kind != "TASK" or step in compensated:
                    continue
                node = nodes.get(f"{w}#{step}")
                if node is not None and node.get("compensable"):
                    raise ValueError(f"step '{step}' of workflow '{wf}' is declared "
                                     f"compensate=True but this worker binds no compensator; add "
                                     f"handle_compensation('{wf}', '{step}', ...)")
            unclaimed = sorted(a.split("#", 1)[1] for a in set(nodes) - served)
            if unclaimed:   # info, not an error: a polyglot worker may intentionally serve a subset
                log.info("workflow '%s' has steps served by no handler on this worker: %s", wf, unclaimed)
        for wf, candidates in self._handler_sets:
            self._match_handler_set(wf, candidates)

    def _match_handler_set(self, wf: str, candidates: dict[str, "_binder.Candidate"]) -> None:
        """Match a :meth:`register_handlers` object's methods to ``wf``'s steps (fetched here — the
        binder itself is pure) and install the resulting bindings. See :mod:`wiggle._binder`."""
        graph = self._fetch_graph(wf)
        result = _binder.bind(wf, candidates, graph)
        for b in result.bindings:
            if b.activity in self._handlers:
                raise ValueError(f"duplicate handler for activity '{b.activity}'")
            self._handlers[b.activity] = b.handler
            self._queues.add(b.queue)
        if result.unserved:   # info, not an error: a polyglot worker may intentionally serve a subset
            log.info("workflow '%s' has steps served by no handler on this worker: %s", wf, result.unserved)

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
