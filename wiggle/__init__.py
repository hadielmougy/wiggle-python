"""Wiggle — Python client and worker for the Wiggle workflow engine.

    from wiggle import Graph, Step, Gate, Effect, Retry, WiggleClient, Worker

Describe a workflow's topology as declarative data (a Graph of nodes), compile and register it, start
instances from the client, and implement its steps with a worker that binds handlers by name. Talks the
same gRPC control plane as the Java and Go clients, so their workers interoperate.
"""
from .client import (
    BacklogCoverage,
    EventBatch,
    EventView,
    InstanceView,
    ReportRefused,
    ScheduleView,
    SearchResult,
    StepStats,
    WiggleClient,
)
from .worker import Compensation, Handlers, PermanentError, Worker
from . import step  # noqa: F401  (wiggle.step.base() et al.)
from .workflow import (
    AwaitSignal,
    FlowSpec,
    Branch,
    Case,
    Choose,
    DoWhile,
    Effect,
    Fork,
    ForEach,
    Gate,
    Graph,
    Node,
    Retry,
    Sleep,
    Step,
    SubWorkflow,
)

__all__ = [
    "Graph",
    "Node",
    "Step",
    "Effect",
    "Gate",
    "Sleep",
    "AwaitSignal",
    "SubWorkflow",
    "Fork",
    "Branch",
    "ForEach",
    "Choose",
    "Case",
    "DoWhile",
    "Retry",
    "FlowSpec",
    "WiggleClient",
    "InstanceView",
    "ScheduleView",
    "EventView",
    "EventBatch",
    "SearchResult",
    "StepStats",
    "BacklogCoverage",
    "ReportRefused",
    "Worker",
    "Handlers",
    "PermanentError",
]

__version__ = "0.1.0"
