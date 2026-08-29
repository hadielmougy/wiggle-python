"""Cold-start regression: a worker-critical call issued before the server exists must WAIT for it
(wait_for_ready) and then succeed once it comes up -- rather than failing fast with UNAVAILABLE.
Uses a tiny in-process gRPC server (no real Wiggle server needed), started after the call is already
blocked."""
import socket
import threading
import time
from concurrent import futures

import grpc

from wiggle import Graph, Step, WiggleClient
from wiggle._proto import wiggle_pb2 as pb
from wiggle._proto import wiggle_pb2_grpc as rpc


class _FakeControlPlane(rpc.WiggleControlPlaneServicer):
    def RegisterWorkflow(self, request, context):
        return pb.RegisterWorkflowResult(name="cold", version="1", nodes=1)


def _free_port() -> int:
    s = socket.socket()
    s.bind(("127.0.0.1", 0))
    port = s.getsockname()[1]
    s.close()
    return port


def test_cold_start_register_waits_for_server():
    port = _free_port()
    client = WiggleClient(f"127.0.0.1:{port}")  # wait_for_ready defaults to True
    wf = Graph("cold", [Step("a")]).compile()

    result = {}

    def do_register():
        try:
            result["version"] = client.register(wf)
        except Exception as e:  # noqa: BLE001
            result["error"] = repr(e)

    t = threading.Thread(target=do_register)
    t.start()

    time.sleep(0.6)
    assert not result, f"register returned before the server was up: {result}"  # still blocked

    server = grpc.server(futures.ThreadPoolExecutor(max_workers=2))
    rpc.add_WiggleControlPlaneServicer_to_server(_FakeControlPlane(), server)
    server.add_insecure_port(f"127.0.0.1:{port}")
    server.start()
    try:
        t.join(timeout=5)
        assert result.get("version") == 1, f"register did not complete after server up: {result}"
    finally:
        server.stop(0)


def test_wait_for_ready_flag_default_and_override():
    assert WiggleClient("127.0.0.1:1").__dict__["_wfr"] is True
    assert WiggleClient("127.0.0.1:1", wait_for_ready=False).__dict__["_wfr"] is False


def test_interceptor_logs_rpc_error(caplog):
    """The client interceptor logs any failed RPC. GetWorkflow hits the fake server, which doesn't
    implement it (UNIMPLEMENTED), so a warning must be logged."""
    import logging

    port = _free_port()
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=2))
    rpc.add_WiggleControlPlaneServicer_to_server(_FakeControlPlane(), server)
    server.add_insecure_port(f"127.0.0.1:{port}")
    server.start()
    try:
        client = WiggleClient(f"127.0.0.1:{port}")
        with caplog.at_level(logging.WARNING, logger="wiggle.client"):
            try:
                client.get_workflow("nope")
            except grpc.RpcError:
                pass
        assert any("GetWorkflow failed" in r.message for r in caplog.records), caplog.text
    finally:
        server.stop(0)