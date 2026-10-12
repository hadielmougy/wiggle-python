"""Opt-in end-to-end tests against a running server. Set WIGGLE_TEST_URL, e.g. after starting the
engine image:  docker run --rm -p 8080:8080 hadielmougy/wiggle

    WIGGLE_TEST_URL=localhost:8080 pytest tests/test_integration.py
"""
import os
import time

import pytest

from wiggle import (AwaitSignal, Branch, Case, Choose, DoWhile, ForEach, Fork, Graph, PermanentActivityError,
                    Sleep, Step, SubWorkflow, WiggleClient, Worker, step)

URL = os.environ.get("WIGGLE_TEST_URL")
pytestmark = pytest.mark.skipif(not URL, reason="set WIGGLE_TEST_URL to run integration tests")


@pytest.fixture
def client():
    with WiggleClient(URL) as c:
        yield c


def _name(prefix: str) -> str:
    return f"{prefix}-{time.time_ns()}"


def _run(client, flow, worker, context, timeout_s=30.0):
    client.register(flow)
    with worker.start():
        iid = client.start(flow, context)
        return client.await_completion(iid, timeout_s=timeout_s)


def test_linear_flow_with_gate(client):
    name = _name("py-it-order")
    flow = Graph(name, [Step("validate"), Step("charge", queue=name + "-payments")]).compile()
    w = (Worker(client, "it-order", concurrency=2, long_poll_wait_s=1)
         .handle(name, "validate", lambda c: {**c, "status": "VALIDATED"})
         .handle(name, "charge", lambda c: {**c, "paymentRef": f"auth-{c['orderId']}"}))
    v = _run(client, flow, w, {"orderId": "x1"})
    assert v.status == "COMPLETED", v.error
    assert v.context == {"orderId": "x1", "status": "VALIDATED", "paymentRef": "auth-x1"}


def test_fork_combine(client):
    name = _name("py-it-fork")
    flow = Graph(name, [Fork([Branch("left", [Step("l")]), Branch("right", [Step("r")])],
                             combine="join")]).compile()

    def join(c):
        assert step.base() == {"n": 1}
        return {"n": c["n"], "sides": c["left"]["side"] + c["right"]["side"]}

    w = (Worker(client, "it-fork", concurrency=2, long_poll_wait_s=1)
         .handle(name, "l", lambda c: {**c, "side": "L"})
         .handle(name, "r", lambda c: {**c, "side": "R"})
         .handle_combine(name, "join", join))
    v = _run(client, flow, w, {"n": 1})
    assert v.status == "COMPLETED", v.error
    assert v.context == {"n": 1, "sides": "LR"}


def test_for_each(client):
    name = _name("py-it-each")
    flow = Graph(name, [ForEach("doubled", items_key="xs", body=[Step("double")], combine="total")]).compile()
    w = (Worker(client, "it-each", concurrency=2, long_poll_wait_s=1)
         .handle(name, "double", lambda x: x * step.base()["factor"])
         .handle_combine(name, "total", lambda c: {"total": sum(c["doubled"])}))
    v = _run(client, flow, w, {"xs": [1, 2, 3], "factor": 2})
    assert v.status == "COMPLETED", v.error
    assert v.context == {"total": 12}


def test_choose_and_do_while(client):
    name = _name("py-it-loop")
    flow = Graph(name, [
        DoWhile("more", body=[Step("inc")], max_iterations=10),
        Choose([Case(when="big", then=[Step("label-big")]), Case(then=[Step("label-small")])]),
    ]).compile()
    w = (Worker(client, "it-loop", concurrency=2, long_poll_wait_s=1)
         .handle(name, "inc", lambda c: {**c, "i": c["i"] + 1})
         .handle_gate(name, "more", lambda c: c["i"] < 4)
         .handle_gate(name, "big", lambda c: c["i"] > 3)
         .handle(name, "label-big", lambda c: {**c, "label": "big"})
         .handle(name, "label-small", lambda c: {**c, "label": "small"}))
    v = _run(client, flow, w, {"i": 0})
    assert v.status == "COMPLETED", v.error
    assert v.context == {"i": 4, "label": "big"}


def test_signal_and_sleep(client):
    name = _name("py-it-signal")
    flow = Graph(name, [Sleep("nap", millis=100), AwaitSignal("approve", timeout_s=30),
                        Step("finish")]).compile()
    client.register(flow)
    w = Worker(client, "it-signal", concurrency=2, long_poll_wait_s=1) \
        .handle(name, "finish", lambda c: {**c, "done": True})
    with w.start():
        iid = client.start(flow, {})
        deadline = time.monotonic() + 10
        while True:
            try:
                client.signal(iid, "approve", {"by": "ops"})
                break
            except Exception:  # noqa: BLE001 - not waiting for the signal yet
                if time.monotonic() > deadline:
                    raise
                time.sleep(0.1)
        v = client.await_completion(iid, timeout_s=20)
    assert v.status == "COMPLETED", v.error
    assert v.context == {"by": "ops", "done": True}


def test_sub_workflow(client):
    child, parent = _name("py-it-child"), _name("py-it-parent")
    child_flow = Graph(child, [Step("work")]).compile()
    client.register(child_flow)
    flow = Graph(parent, [SubWorkflow("child", child), Step("after")]).compile()
    w = (Worker(client, "it-sub", concurrency=2, long_poll_wait_s=1)
         .handle(child, "work", lambda c: {**c, "child": True})
         .handle(parent, "after", lambda c: {**c, "after": True}))
    v = _run(client, flow, w, {})
    assert v.status == "COMPLETED", v.error
    assert v.context == {"child": True, "after": True}


def test_compensation(client):
    name = _name("py-it-saga")
    flow = Graph(name, [Step("reserve", compensate=True), Step("overwrite"), Step("boom")]).compile()
    undone = []

    def boom(c):
        raise PermanentActivityError("downstream exploded")

    w = (Worker(client, "it-saga", concurrency=2, long_poll_wait_s=1)
         .handle(name, "reserve", lambda c: {"orderId": c["orderId"], "reservationRef": "r-1"})
         .handle_compensation(name, "reserve", undone.append)
         .handle(name, "overwrite", lambda c: {"unrelated": True})
         .handle(name, "boom", boom))
    v = _run(client, flow, w, {"orderId": "o-9"})
    assert v.status == "COMPENSATED", v.error
    assert undone[0].result["reservationRef"] == "r-1"
    assert undone[0].input == {"orderId": "o-9"}


def test_retry_then_success_reports_the_attempt(client):
    name = _name("py-it-retry")
    flow = Graph(name, [Step("flaky")]).compile()

    def flaky(c):
        if step.attempt() < 2:
            raise RuntimeError("blip")
        return {"attempt": step.attempt(), "step": step.name()}

    w = Worker(client, "it-retry", concurrency=2, long_poll_wait_s=1).handle(name, "flaky", flaky)
    v = _run(client, flow, w, {}, timeout_s=40)
    assert v.status == "COMPLETED", v.error
    assert v.context == {"attempt": 2, "step": "flaky"}


def test_emitted_events_reach_the_feed_and_timings_feed_step_stats(client):
    name = _name("py-it-events")
    consumer = name + "-feed"
    client.poll_events(consumer, max_events=1, wait_s=0, start_from=-1)   # register the cursor
    flow = Graph(name, [Step("ship")]).compile()

    def ship(c):
        step.emit("order.shipped", {"attempt": step.attempt(), "step": step.name()})
        return c

    w = Worker(client, "it-events", concurrency=2, long_poll_wait_s=1).handle(name, "ship", ship)
    v = _run(client, flow, w, {"id": "o-1"})
    assert v.status == "COMPLETED", v.error

    seen = {}
    deadline = time.monotonic() + 15
    while len(seen) < 3 and time.monotonic() < deadline:
        batch = client.poll_events(consumer, max_events=100, wait_s=2)
        seen.update({e.type: e for e in batch.events if e.instance_id == v.id})
        if batch.events:
            client.ack_events(consumer, cursor=batch.events[-1].cursor)
    assert {"wf.started", "order.shipped", "wf.completed"} <= set(seen), seen
    shipped = seen["order.shipped"]
    assert shipped.payload == {"attempt": 1, "step": "ship"} and shipped.node_id

    stats = client.step_stats(name)
    assert [(n.name, n.count) for n in stats.nodes] == [("ship", 1)]
