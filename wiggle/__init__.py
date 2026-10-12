"""Wiggle — Python client and worker for the Wiggle workflow engine.

    from wiggle import FlowSpec, RetryPolicy, Wiggle, WiggleClient, Worker

Define a workflow as a chain of steps (``FlowSpec.define``), register it, start instances from the
client, and implement its steps with a worker that binds handlers by name. Talks the
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
from .flow import Arms, Items, Wiggle, WiggleFlow
from .workflow import AttrType, FlowSpec, RetryPolicy

__all__ = [
    "FlowSpec",
    "WiggleFlow",
    "Wiggle",
    "Arms",
    "Items",
    "RetryPolicy",
    "WiggleClient",
    "InstanceView",
    "ScheduleView",
    "EventView",
    "EventBatch",
    "SearchResult",
    "StepStats",
    "BacklogCoverage",
    "ReportRefused",
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
