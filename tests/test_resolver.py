"""Offline tests for the client-side CellResolver: id parsing, coordinator-mode routing with a TTL
cache (against a tiny in-process fake coordinator), and no-coordinator pass-through."""
from concurrent import futures

import grpc
import pytest

from wiggle import CellResolver, is_legacy_id, parse_id
from wiggle._proto import coordinator_pb2 as cpb
from wiggle._proto import coordinator_pb2_grpc as crpc


def test_parse_id():
    p = parse_id("acme.e7.s2.01H8ABC")
    assert p is not None
    assert (p.namespace, p.epoch, p.shard, p.ulid) == ("acme", 7, 2, "01H8ABC")
    assert parse_id("wfi_01h8abc") is None
    assert is_legacy_id("wfi_01h8abc")
    assert not is_legacy_id("ns.e0.s0.x")


class _FakeCoord(crpc.CellCoordinatorServicer):
    def __init__(self):
        self.resolve_calls = 0
        self.target = "cell-a:9"

    def Resolve(self, req, ctx):
        self.resolve_calls += 1
        return cpb.ResolveResponse(namespace=req.namespace, epoch=0, ttl_seconds=30,
                                   endpoint=cpb.Endpoint(target=self.target, ttl_seconds=30))

    def ActiveCells(self, req, ctx):
        return cpb.ActiveCellsResponse(generation=1, ttl_seconds=30,
                                       cells=[cpb.Endpoint(target=self.target)])


def _start_fake_coord():
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=2))
    fake = _FakeCoord()
    crpc.add_CellCoordinatorServicer_to_server(fake, server)
    port = server.add_insecure_port("127.0.0.1:0")
    server.start()
    return fake, f"127.0.0.1:{port}", server


def test_coordinator_mode_routing_and_cache():
    fake, url, server = _start_fake_coord()
    try:
        with CellResolver.coordinator(url, caller_region="eu-west") as r:
            assert r.active_cell_targets("acme") == ["cell-a:9"]

            assert r.client_for_namespace("acme") is not None
            # TTL cache: a second resolve of the same namespace must not re-hit the coordinator
            r.client_for_namespace("acme")
            assert fake.resolve_calls == 1

            # by epoch-aware id → routed via the id's namespace (still cached)
            assert r.client_for_instance("acme.e0.s0.01H8") is not None
            # a legacy id under a coordinator is rejected
            with pytest.raises(ValueError):
                r.client_for_instance("wfi_legacy")
    finally:
        server.stop(0)


def test_direct_mode_is_pass_through():
    with CellResolver.direct("static:8080") as r:
        assert r.active_cell_targets("ignored") == ["static:8080"]
        # direct mode routes everything to the static target, legacy ids included
        assert r.client_for_instance("wfi_legacy") is not None
        assert r.client_for_namespace("ignored") is not None
