"""Wiggle — Python client and worker for the Wiggle workflow engine.

    from wiggle import Graph, Step, Gate, Effect, RetryPolicy, WiggleClient, Worker

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
from .worker import Compensation, Handlers, PermanentActivityError, Worker, for_flow
from . import criteria, step  # noqa: F401  (wiggle.step.create() et al., wiggle.criteria)
from .branch import Branch as CreatedBranch, BranchError
from .workflow import (
    AttrType,
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
    Attribute,
    Graph,
    Node,
    RetryPolicy,
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
    "RetryPolicy",
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
    "Attribute",
    "AttrType",
    "CreatedBranch",
    "BranchError",
    "criteria",
    "Worker",
    "Handlers",
    "for_flow",
    "PermanentActivityError",
]

try:
    from importlib.metadata import PackageNotFoundError, version as _version
    __version__ = _version("wiggle-client")      # single source of truth: pyproject.toml
except PackageNotFoundError:                     # running from a checkout that isn't installed
    __version__ = "0.0.0+unknown"
