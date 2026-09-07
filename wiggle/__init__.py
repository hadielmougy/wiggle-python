"""Wiggle — Python client and worker for the Wiggle workflow engine.

    from wiggle import Graph, Step, Gate, Effect, Retry, WiggleClient, Worker

Describe a workflow's topology as declarative data (a Graph of nodes), compile and register it, start
instances from the client, and implement its steps with a worker that binds handlers by name. Talks the
same gRPC control plane as the Java and Go clients, so their workers interoperate.
"""
from .client import InstanceView, ScheduleView, WiggleClient
from .resolver import CellResolver, Placement, is_legacy_id, parse_id
from .worker import Handlers, PermanentError, Worker
from .workflow import (
    AwaitSignal,
    Blueprint,
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
    "Blueprint",
    "WiggleClient",
    "InstanceView",
    "ScheduleView",
    "Worker",
    "Handlers",
    "PermanentError",
    "CellResolver",
    "Placement",
    "parse_id",
    "is_legacy_id",
]

__version__ = "0.1.0"
