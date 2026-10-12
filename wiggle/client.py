"""The control-plane client: register workflows, start and track instances, deliver signals,
manage schedules, read the event feed. Also carries the low-level worker RPCs
(poll/report_steps/fail/heartbeat) used by :class:`~wiggle.worker.Worker`."""
from __future__ import annotations

import logging
import time
from dataclasses import dataclass, field
from typing import Any, Iterable, Optional, Union

import grpc
from google.protobuf import json_format, struct_pb2

from ._convert import from_value, to_value
from ._proto import wiggle_pb2 as pb
from ._proto import wiggle_pb2_grpc as rpc
from .workflow import FlowSpec

# COMPENSATING is NOT terminal: the reverse pass is still running its compensators; it settles
# as COMPENSATED or COMPENSATION_FAILED.
TERMINAL = frozenset({"COMPLETED", "FAILED", "CANCELLED", "COMPENSATED", "COMPENSATION_FAILED"})

_log = logging.getLogger("wiggle.client")


# The "expected" failures -- reconcile waiting for a workflow to be registered, or a server not yet
# up -- log at DEBUG; anything else is a genuine error and logs at WARNING.
_EXPECTED_CODES = (grpc.StatusCode.NOT_FOUND, grpc.StatusCode.UNAVAILABLE)


class _ErrorLogInterceptor(grpc.UnaryUnaryClientInterceptor):
    """Logs every failed unary RPC (method, status code, details) in one place. CANCELLED (worker
    shutdown) is skipped; NOT_FOUND/UNAVAILABLE log at DEBUG, everything else at WARNING."""

    def intercept_unary_unary(self, continuation, client_call_details, request):
        call = continuation(client_call_details, request)
        code = call.code()
        if code is not None and code not in (grpc.StatusCode.OK, grpc.StatusCode.CANCELLED):
            method = client_call_details.method.rsplit("/", 1)[-1]
            level = logging.DEBUG if code in _EXPECTED_CODES else logging.WARNING
            _log.log(level, "rpc %s failed: %s: %s", method, code.name, call.details())
        return call


@dataclass
class InstanceView:
    id: str
    workflow: str
    version: int
    status: str
    termination_reason: Optional[str]
    error: Optional[str]
    context: Any
    created_at: int
    updated_at: int

    @property
    def done(self) -> bool:
        return self.status in TERMINAL


@dataclass
class ScheduleView:
    id: str
    workflow: str
    every_millis: int
    cron: str
    next_fire_at: int
    created_at: int


@dataclass
class EventView:
    """One entry of the event log: a lifecycle transition (``wf.*``) or a handler-emitted event."""
    seq: int
    shard: int
    instance_id: str
    workflow: str
    version: int
    correlation_id: str
    type: str
    created_at: int
    payload: Any
    node_id: str
    cursor: str


@dataclass
class EventBatch:
    events: list[EventView]
    retry_after_millis: int = 0


@dataclass
class SearchHit:
    instance_id: str
    workflow: str
    version: int
    status: str
    correlation_id: str
    created_at: int
    updated_at: int
    score: float
    purged: bool


@dataclass
class SearchResult:
    hits: list[SearchHit]
    partial: bool
    model: str


@dataclass
class NodeStats:
    node_id: str
    name: str
    count: int
    mean_millis: float
    p50_millis: int
    p95_millis: int
    max_millis: int
    wait_p50_millis: int
    wait_p95_millis: int


@dataclass
class StepStats:
    workflow: str
    version: int
    nodes: list[NodeStats]


@dataclass
class BacklogSlice:
    workflow: str
    version: int
    queue: str
    ready_count: int
    oldest_available_at: int
    covered: bool
    coverage: str
    claimable: Optional[int]
    criteria: list[str]


@dataclass
class BacklogCoverage:
    slices: list[BacklogSlice]
    live_pollers: int


@dataclass
class StepReport:
    """One finished step, as a worker reports it: exactly one of ``merge`` (a task's complete next
    context; ``None`` leaves it unchanged) or ``predicate`` (a predicate's branch), with when it ran
    and the events it emitted as ``(type, payload)`` pairs."""
    node_id: str
    merge: Any = None
    predicate: Optional[bool] = None
    started_at: int = 0
    finished_at: int = 0
    events: list = field(default_factory=list)


@dataclass
class RunApplied:
    """What a reported run applied to. Anything but RUNNING means stop here."""
    instance_status: str
    lease_expires_at: int
    next_task_id: Optional[str]


class ReportRefused(Exception):
    """The server refused a reported run; it wrote nothing. ``status`` is HTTP-style (404, 409, ...)."""

    def __init__(self, task_id: str, status: int, message: str):
        super().__init__(f"report of task {task_id} refused ({status}): {message}")
        self.task_id = task_id
        self.status = status


class WiggleClient:
    """A thin, Pythonic wrapper over the gRPC control plane. Use as a context manager."""

    def __init__(self, target: str = "localhost:8080", *,
                 credentials: Optional[grpc.ChannelCredentials] = None,
                 wait_for_ready: bool = True):
        """``wait_for_ready`` (default True) makes the worker-critical RPCs -- register, get_workflow,
        poll, report_steps, fail, heartbeat -- wait for the server to become reachable instead of failing
        fast with UNAVAILABLE. This lets a worker ride out a cold start, a rolling restart, or a
        reschedule (many workers coming up before the server is ready) without crashing. Query calls
        (instance, list, ...) stay fail-fast."""
        base = (grpc.secure_channel(target, credentials) if credentials
                else grpc.insecure_channel(target))
        self._channel = grpc.intercept_channel(base, _ErrorLogInterceptor())
        self._stub = rpc.WiggleControlPlaneStub(self._channel)
        self._wfr = wait_for_ready

    def __enter__(self) -> "WiggleClient":
        return self

    def __exit__(self, *exc) -> None:
        self.close()

    def close(self) -> None:
        self._channel.close()

    # ---- workflows & instances ----

    def register(self, flow_spec: FlowSpec, *, force: bool = False) -> int:
        """Register a workflow definition; returns its version. Idempotent for the same graph.
        ``force`` replaces an already-registered (name, version) whose graph differs instead of
        failing; the server refuses it unless configured to allow graph replacement."""
        struct = json_format.ParseDict(flow_spec.definition, struct_pb2.Struct())
        result = self._stub.RegisterWorkflow(pb.WorkflowDefinition(definition=struct, force=force),
                                             wait_for_ready=self._wfr)
        return int(result.version)

    def get_workflow(self, name: str, version: Optional[int] = None) -> dict:
        """The registered graph for ``name`` (the latest version, or ``version``) as a plain dict --
        the server's source of truth for a workflow's step names, kinds, and queues. Raises
        ``grpc.RpcError`` (NOT_FOUND) if it was never registered. Used by worker reconciliation."""
        req = pb.GetWorkflowRequest(name=name)
        if version is not None:
            req.version = version
        resp = self._stub.GetWorkflow(req, wait_for_ready=self._wfr)
        return json_format.MessageToDict(resp.definition)

    def start(self, workflow: Union[FlowSpec, str], context: Any, *,
              version: Optional[int] = None, correlation_id: Optional[str] = None) -> str:
        """Start an instance; returns its id."""
        name = workflow.name if isinstance(workflow, FlowSpec) else workflow
        req = pb.StartInstanceRequest(workflow=name, context=to_value(context))
        if version is not None:
            req.version = version
        if correlation_id is not None:
            req.correlation_id = correlation_id
        return self._stub.StartInstance(req).instance_id

    def instance(self, instance_id: str) -> InstanceView:
        return _view(self._stub.GetInstance(pb.InstanceIdRequest(instance_id=instance_id)).instance)

    def await_completion(self, instance_id: str, timeout_s: float = 30.0,
                         poll_interval_s: float = 0.2) -> InstanceView:
        """Poll until the instance reaches a terminal state, or raise ``TimeoutError``."""
        deadline = time.monotonic() + timeout_s
        while True:
            view = self.instance(instance_id)
            if view.done:
                return view
            if time.monotonic() >= deadline:
                raise TimeoutError(f"instance {instance_id} still {view.status} after {timeout_s}s")
            time.sleep(poll_interval_s)

    def list_instances(self, *, workflow: Optional[str] = None, status: Optional[str] = None,
                       limit: int = 100) -> list[InstanceView]:
        req = pb.ListInstancesRequest(limit=limit)
        if workflow is not None:
            req.workflow = workflow
        if status is not None:
            req.status = status
        return [_view(v) for v in self._stub.ListInstances(req).instances]

    def cancel(self, instance_id: str, reason: str = "cancelled") -> None:
        self._stub.CancelInstance(pb.CancelInstanceRequest(instance_id=instance_id, reason=reason))

    def signal(self, instance_id: str, signal: str, payload: Any = None) -> None:
        """Deliver a named signal; ``payload`` merges into the instance context."""
        self._stub.SignalInstance(pb.SignalRequest(
            instance_id=instance_id, signal=signal, payload=to_value(payload)))

    # ---- schedules ----

    def create_schedule(self, workflow: str, *, every_s: Optional[float] = None,
                        cron: Optional[str] = None, context: Any = None) -> ScheduleView:
        if (every_s is None) == (cron is None):
            raise ValueError("pass exactly one of every_s or cron")
        req = pb.CreateScheduleRequest(workflow=workflow, context=to_value(context))
        if every_s is not None:
            req.every_millis = int(every_s * 1000)
        else:
            req.cron = cron
        return _schedule(self._stub.CreateSchedule(req))

    def list_schedules(self) -> list[ScheduleView]:
        return [_schedule(s) for s in self._stub.ListSchedules(pb.Empty()).schedules]

    def delete_schedule(self, schedule_id: str) -> None:
        self._stub.DeleteSchedule(pb.ScheduleIdRequest(id=schedule_id))

    # ---- cluster / health ----

    def health(self) -> dict:
        h = self._stub.HealthCheck(pb.Empty())
        return {"status": h.status, "node": h.node, "leader": h.leader}

    def cluster(self) -> dict:
        c = self._stub.GetCluster(pb.Empty())
        return {
            "self": c.self,
            "leader": c.leader,
            "members": [{"id": m.id, "name": m.name, "workers": m.workers,
                         "leader": m.leader, "alive": m.alive} for m in c.members],
        }

    # ---- event feed ----

    def poll_events(self, consumer: str, *, max_events: int = 100, wait_s: float = 10.0,
                    start_from: int = 0) -> EventBatch:
        """Long-poll the event log through the named cursor ``consumer``; the first poll registers
        it at ``start_from`` (0 = tail, -1 = earliest retained, else a seq). Events stay unacknowledged
        -- and are delivered again -- until :meth:`ack_events` moves the cursor past them."""
        res = self._stub.PollEvents(pb.PollEventsRequest(
            consumer=consumer, max=max_events, wait_millis=int(wait_s * 1000), start_from=start_from))
        return EventBatch([_event(e) for e in res.events], res.retry_after_millis)

    def ack_events(self, consumer: str, *, cursor: Optional[str] = None,
                   seq: Optional[int] = None) -> None:
        """Acknowledge every event up to and including the one whose ``cursor`` is given (or, on a
        single-shard log only, every event with ``seq`` at most this)."""
        if (cursor is None) == (seq is None):
            raise ValueError("pass exactly one of cursor or seq")
        req = pb.AckEventsRequest(consumer=consumer)
        if cursor is not None:
            req.acked_cursor = cursor
        else:
            req.acked_seq = seq
        self._stub.AckEvents(req)

    # ---- search, statistics, backlog ----

    def search(self, text: str = "", *, workflow: Optional[str] = None, status: Optional[str] = None,
               updated_from: int = 0, updated_to: int = 0, limit: int = 50, partial_ok: bool = False,
               semantic: bool = False) -> SearchResult:
        """Search instances by the words of their correlation id, context, termination reason or
        error -- or by meaning with ``semantic``. FAILED_PRECONDITION when search is not enabled."""
        req = pb.SearchRequest(text=text, updated_from=updated_from, updated_to=updated_to,
                               limit=limit, partial_ok=partial_ok, semantic=semantic)
        if workflow is not None:
            req.workflow = workflow
        if status is not None:
            req.status = status
        res = self._stub.SearchInstances(req)
        return SearchResult([SearchHit(h.instance_id, h.workflow, h.version, h.status, h.correlation_id,
                                       h.created_at, h.updated_at, h.score, h.purged) for h in res.hits],
                            res.partial, res.model)

    def step_stats(self, workflow: str, *, version: int = 0, since: int = 0,
                   sample: int = 0) -> StepStats:
        """Per-step duration and queue-wait percentiles over recently settled steps of one workflow
        version (0 = the latest)."""
        res = self._stub.GetStepStats(pb.StepStatsRequest(
            workflow=workflow, version=version, since=since, sample=sample))
        return StepStats(res.workflow, res.version, [
            NodeStats(n.node_id, n.name, n.count, n.mean_millis, n.p50_millis, n.p95_millis,
                      n.max_millis, n.wait_p50_millis, n.wait_p95_millis) for n in res.nodes])

    def backlog_coverage(self, max_slices: int = 0) -> BacklogCoverage:
        """Dispatchable work by (workflow, version, queue), each slice saying whether any worker
        polling this node would claim it."""
        res = self._stub.GetBacklogCoverage(pb.BacklogCoverageRequest(max=max_slices))
        return BacklogCoverage([
            BacklogSlice(b.workflow, b.version, b.queue, b.ready_count, b.oldest_available_at,
                         b.covered, b.coverage, b.claimable if b.HasField("claimable") else None,
                         list(b.criteria)) for b in res.slices], res.live_pollers)

    # ---- low-level worker RPCs (used by Worker) ----

    def poll(self, worker_id: str, queues: Iterable[str], max_tasks: int,
             lease_millis: int, wait_millis: int,
             versions: Iterable[tuple[str, int]] = ()) -> pb.TaskList:
        """Claim up to ``max_tasks`` ready tasks on ``queues``. A non-empty ``versions`` -- (workflow,
        version) pairs -- claims only those versions' tasks."""
        return self._stub.PollTasks(pb.PollRequest(
            worker_id=worker_id, queues=list(queues), max=max_tasks,
            lease_millis=lease_millis, wait_millis=wait_millis,
            versions=[pb.WorkflowVersion(workflow=w, version=v) for w, v in versions]),
            wait_for_ready=self._wfr)

    def report_steps(self, task_id: str, lease_owner: str, steps: list[StepReport],
                     final: bool = True) -> RunApplied:
        """Report the finished steps of one leased task. ``final`` says this worker takes no
        continuation. Raises :class:`ReportRefused` when the server refuses the run."""
        run = pb.ReportedRun(task_id=task_id, lease_owner=lease_owner, final=final,
                             steps=[_step_result(s) for s in steps])
        res = self._stub.ReportSteps(pb.ReportStepsRequest(runs=[run]), wait_for_ready=self._wfr)
        for r in res.results:
            if r.task_id != task_id:
                continue
            if r.error_status != 0 or not r.HasField("applied"):
                raise ReportRefused(task_id, r.error_status, r.error)
            a = r.applied
            return RunApplied(a.instance_status, a.lease_expires_at, a.next_task_id or None)
        raise ReportRefused(task_id, 500, "no answer for this run")

    def fail(self, task_id: str, lease_owner: str, message: str, retryable: bool) -> None:
        self._stub.FailTask(pb.TaskFailureRequest(
            task_id=task_id, lease_owner=lease_owner, message=message, retryable=retryable),
            wait_for_ready=self._wfr)

    def heartbeat(self, task_id: str, lease_owner: str, extend_millis: int) -> int:
        return self._stub.HeartbeatTask(pb.HeartbeatRequest(
            task_id=task_id, lease_owner=lease_owner, extend_millis=extend_millis),
            wait_for_ready=self._wfr).lease_expires_at


def _step_result(s: StepReport) -> pb.StepResult:
    r = pb.StepResult(node_id=s.node_id, started_at=s.started_at, finished_at=s.finished_at,
                      events=[pb.EmittedEvent(type=t, payload=to_value(p)) for t, p in s.events])
    if s.predicate is not None:
        r.predicate_value = s.predicate
    elif s.merge is not None:
        r.merge.CopyFrom(to_value(s.merge))
    return r


def _event(e: pb.EventView) -> EventView:
    return EventView(e.seq, e.shard, e.instance_id, e.workflow, e.version, e.correlation_id, e.type,
                     e.created_at, from_value(e.payload), e.node_id, e.cursor)


def _view(v: pb.InstanceView) -> InstanceView:
    return InstanceView(
        id=v.id, workflow=v.workflow, version=v.version, status=v.status,
        termination_reason=v.termination_reason if v.HasField("termination_reason") else None,
        error=v.error if v.HasField("error") else None,
        context=from_value(v.context), created_at=v.created_at, updated_at=v.updated_at)


def _schedule(s: pb.ScheduleView) -> ScheduleView:
    return ScheduleView(s.id, s.workflow, s.every_millis, s.cron, s.next_fire_at, s.created_at)
