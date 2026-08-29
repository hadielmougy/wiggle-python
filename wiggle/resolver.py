"""Client-side routing to cells, via the coordinator's Resolve / ActiveCells. Mirror of the Java
``CellResolver`` and the Go ``resolver.go``.

With a coordinator configured, calls are routed to the cell that owns a namespace or instance;
resolutions are cached by TTL and per-cell :class:`WiggleClient` s are reused. With no coordinator it
is a pass-through to a single static target, so existing (non-sharded) usage is unchanged.
"""
from __future__ import annotations

import re
import threading
import time
from dataclasses import dataclass
from typing import Dict, List, Optional, Tuple

import grpc

from .client import WiggleClient
from ._proto import coordinator_pb2 as cpb
from ._proto import coordinator_pb2_grpc as crpc

# The epoch-aware instance id: {namespace}.e{epoch}.s{shard}.{ulid}. Legacy wfi_... ids do not match.
_ID = re.compile(r"^([^.]+)\.e(\d+)\.s(\d+)\.(.+)$")


@dataclass(frozen=True)
class Placement:
    namespace: str
    epoch: int
    shard: int
    ulid: str


def parse_id(instance_id: Optional[str]) -> Optional[Placement]:
    """Parse an epoch-aware id, or return None for a legacy id."""
    if not instance_id:
        return None
    m = _ID.match(instance_id)
    if not m:
        return None
    return Placement(m.group(1), int(m.group(2)), int(m.group(3)), m.group(4))


def is_legacy_id(instance_id: Optional[str]) -> bool:
    return parse_id(instance_id) is None


def _strip(target: str) -> str:
    i = target.find("://")
    return target if i < 0 else target[i + 3:]


class CellResolver:
    """Routes client calls to the cell that owns a namespace or instance."""

    def __init__(self, *, coordinator_url: Optional[str] = None, static_target: Optional[str] = None,
                 caller_region: str = "", credentials=None):
        self._coord_url = coordinator_url
        self._static = static_target
        self._region = caller_region or ""
        self._credentials = credentials          # TLS for cell clients (MVP: plaintext)
        self._lock = threading.Lock()
        self._ns_cache: Dict[str, Tuple[str, float]] = {}   # namespace -> (target, expiry_monotonic)
        self._clients: Dict[str, WiggleClient] = {}
        if coordinator_url:
            self._coord_channel = grpc.insecure_channel(_strip(coordinator_url))
            self._coord = crpc.CellCoordinatorStub(self._coord_channel)
        else:
            self._coord_channel = None
            self._coord = None

    @classmethod
    def coordinator(cls, url: str, caller_region: str = "", credentials=None) -> "CellResolver":
        return cls(coordinator_url=url, caller_region=caller_region, credentials=credentials)

    @classmethod
    def direct(cls, static_target: str, credentials=None) -> "CellResolver":
        return cls(static_target=static_target, credentials=credentials)

    def client_for_namespace(self, namespace: str) -> WiggleClient:
        return self._client_for(self._resolve_namespace(namespace))

    def client_for_instance(self, instance_id: str) -> WiggleClient:
        if not self._coord_url:
            return self._client_for(self._static)
        p = parse_id(instance_id)
        if p is None:
            raise ValueError(f"cannot route a legacy instance id {instance_id!r} under a coordinator")
        return self._client_for(self._resolve_namespace(p.namespace))

    def active_cell_targets(self, namespace: str) -> List[str]:
        if not self._coord_url:
            return [self._static]
        resp = self._coord.ActiveCells(cpb.ActiveCellsRequest(namespace=namespace, caller_region=self._region))
        return [e.target for e in resp.cells]

    def invalidate(self, namespace: str) -> None:
        """Drop a cached resolution -- call after a cell RPC fails with UNAVAILABLE/NOT_FOUND."""
        with self._lock:
            self._ns_cache.pop(namespace, None)

    def _resolve_namespace(self, namespace: str) -> str:
        if not self._coord_url:
            return self._static
        with self._lock:
            cached = self._ns_cache.get(namespace)
            if cached and time.monotonic() < cached[1]:
                return cached[0]
        resp = self._coord.Resolve(cpb.ResolveRequest(namespace=namespace, caller_region=self._region))
        target = resp.endpoint.target
        ttl = max(1, resp.endpoint.ttl_seconds)
        with self._lock:
            self._ns_cache[namespace] = (target, time.monotonic() + ttl)
        return target

    def _client_for(self, target: str) -> WiggleClient:
        t = _strip(target)
        with self._lock:
            client = self._clients.get(t)
            if client is None:
                client = WiggleClient(t, credentials=self._credentials)
                self._clients[t] = client
            return client

    def close(self) -> None:
        with self._lock:
            for c in self._clients.values():
                c.close()
            self._clients.clear()
        if self._coord_channel is not None:
            self._coord_channel.close()

    def __enter__(self) -> "CellResolver":
        return self

    def __exit__(self, *exc) -> None:
        self.close()
