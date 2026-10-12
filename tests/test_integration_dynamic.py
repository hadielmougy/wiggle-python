"""Opt-in end-to-end tests of branches created at run time (``wiggle.step.create``) and of queue
criteria, against a running server: ``WIGGLE_TEST_URL=localhost:8080 pytest tests/``."""
import os
import time

import pytest

from wiggle import (AttrType, Attribute, Graph, Step, WiggleClient, Worker, criteria as c, step)

URL = os.environ.get("WIGGLE_TEST_URL")
pytestmark = pytest.mark.skipif(not URL, reason="set WIGGLE_TEST_URL to run integration tests")


@pytest.fixture
def client():
    with WiggleClient(URL) as cl:
        yield cl


def _name(prefix: str) -> str:
    return f"{prefix}-{time.time_ns()}"


def _worker(client, wid, **kw):
    return Worker(client, wid, concurrency=4, long_poll_wait_s=1, **kw)


def _run(client, flow, worker, context, timeout_s=30.0):
    client.register(flow)
    with worker.start():
        iid = client.start(flow, context)
        return client.await_completion(iid, timeout_s=timeout_s)


def test_a_step_creates_branches_that_run_different_chains(client):
    name = _name("py-dyn-fulfil")
    flow = Graph(name, [Step("fulfil", combine="summarise")]).compile()

    def fulfil(order):
        for item in order["items"]:
            if item["digital"]:
                step.create(item).then_accept("deliver-link")
            else:
                step.create(item).then_apply("reserve").then_apply("ship")
        return {**order, "fulfilled": True}

    def summarise(ctx):
        return {"id": ctx["id"], "fulfilled": ctx["fulfilled"],
                "results": [r.get("tracking", "link:" + r["sku"]) for r in step.results()]}

    w = (_worker(client, "dyn-fulfil")
         .branch_steps(name, "deliver-link", "reserve", "ship")
         .handle(name, "fulfil", fulfil)
         .handle_effect(name, "deliver-link", lambda item: None)
         .handle(name, "reserve", lambda item: {**item, "reserved": step.base()["id"]})
         .handle(name, "ship", lambda item: {**item, "tracking": "T-" + item["sku"]})
         .handle_combine(name, "summarise", summarise))
    v = _run(client, flow, w, {"id": "o-1", "items": [
        {"sku": "a", "digital": False}, {"sku": "b", "digital": True}, {"sku": "c", "digital": False}]})
    assert v.status == "COMPLETED", v.error
    assert v.context == {"id": "o-1", "fulfilled": True, "results": ["T-a", "link:b", "T-c"]}


class FanHandlers:
    """A handlers object: its branch steps bind by name when first dispatched."""
    branch_steps = ("double",)

    def fan(self, ctx):
        for k, x in ctx["xs"].items():
            step.create({"x": x}, key=k).then_apply("double")
        return ctx

    def double(self, ctx):
        return {"x": ctx["x"] * 2}

    def gather(self, ctx) -> dict:
        return {"doubled": {k: r["x"] for k, r in step.results().items()}}


def test_keyed_branches_reach_the_combine_as_a_dict(client):
    name = _name("py-dyn-keyed")
    flow = Graph(name, [Step("fan", combine="gather")]).compile()
    w = _worker(client, "dyn-keyed").register_handler(FanHandlers(), workflow=name)
    v = _run(client, flow, w, {"xs": {"a": 1, "b": 2}})
    assert v.status == "COMPLETED", v.error
    assert v.context == {"doubled": {"a": 2, "b": 4}}


def test_a_step_that_creates_nothing_still_runs_its_combine(client):
    name = _name("py-dyn-empty")
    flow = Graph(name, [Step("fan", combine="gather")]).compile()
    w = (_worker(client, "dyn-empty")
         .handle(name, "fan", lambda ctx: ctx)
         .handle_combine(name, "gather", lambda ctx: {"n": len(step.results())}))
    v = _run(client, flow, w, {})
    assert v.status == "COMPLETED", v.error
    assert v.context == {"n": 0}


def test_a_combine_that_creates_branches_runs_another_round(client):
    name = _name("py-dyn-rounds")
    flow = Graph(name, [Step("seed", combine="again")]).compile()

    def seed(ctx):
        step.create({"v": 1}).then_apply("inc")
        return {"round": 1, "total": 0}

    def again(ctx):
        total = ctx["total"] + sum(r["v"] for r in step.results())
        if ctx["round"] < 3:
            step.create({"v": ctx["round"] + 1}).then_apply("inc")
            return {"round": ctx["round"] + 1, "total": total}
        return {"rounds": ctx["round"], "total": total}

    w = (_worker(client, "dyn-rounds")
         .branch_steps(name, "inc")
         .handle(name, "seed", seed)
         .handle(name, "inc", lambda b: {"v": b["v"] * 10})
         .handle_combine(name, "again", again))
    v = _run(client, flow, w, {})
    assert v.status == "COMPLETED", v.error
    assert v.context == {"rounds": 3, "total": 60}


def test_server_side_operators_in_a_branch(client):
    name, child = _name("py-dyn-ops"), _name("py-dyn-child")
    client.register(Graph(child, [Step("child-work")]).compile())
    flow = Graph(name, [Step("plan", combine="done")]).compile()

    def plan(ctx):
        (step.create({"n": 0, "size": 5})
         .then_sleep(millis=50)
         .repeat_while("more", lambda b: b.then_apply("inc"), max_iterations=10)
         .then_one_of(lambda b: b.when("big").then_apply("label-big"),
                      lambda b: b.otherwise().then_apply("label-small"))
         .then_all_of(lambda a: a.then_apply("left"), lambda b: b.then_apply("right")).combine("both")
         .then_sub_flow("child", child)
         .then_await("go")
         .then_filter("keep"))
        return ctx

    w = (_worker(client, "dyn-ops")
         .branch_steps(name, "inc", "more", "big", "label-big", "label-small", "left", "right",
                       "both", "keep")
         .handle(name, "plan", plan)
         .handle(name, "inc", lambda b: {**b, "n": b["n"] + 1})
         .handle_gate(name, "more", lambda b: b["n"] < 3)
         .handle_gate(name, "big", lambda b: b["size"] > 3)
         .handle(name, "label-big", lambda b: {**b, "label": "big"})
         .handle(name, "label-small", lambda b: {**b, "label": "small"})
         .handle(name, "left", lambda b: {"side": "L"})
         .handle(name, "right", lambda b: {"side": "R"})
         .handle_combine(name, "both", lambda b: {**{k: v for k, v in b.items() if k not in ("left", "right")},
                                                  "sides": b["left"]["side"] + b["right"]["side"]})
         .handle(child, "child-work", lambda b: {**b, "child": True})
         .handle_gate(name, "keep", lambda b: True)
         .handle_combine(name, "done", lambda ctx: {"branch": step.results()[0]}))
    client.register(flow)
    with w.start():
        iid = client.start(flow, {})
        deadline = time.monotonic() + 20
        while True:
            try:
                client.signal(iid, "go", {"approved": True})
                break
            except Exception:  # noqa: BLE001 - the branch is not waiting yet
                if time.monotonic() > deadline:
                    raise
                time.sleep(0.2)
        v = client.await_completion(iid, timeout_s=30)
    assert v.status == "COMPLETED", v.error
    branch = v.context["branch"]
    assert branch["n"] == 3 and branch["label"] == "big" and branch["sides"] == "LR"
    assert branch["child"] is True and branch["approved"] is True


def test_nested_branches(client):
    name = _name("py-dyn-nested")
    flow = Graph(name, [Step("lines", combine="packed-all")]).compile()

    def lines(order):
        for line in order["lines"]:
            step.create(line).then_apply("pick").combine("packed").then_apply("label")
        return order

    def pick(line):
        for unit in range(line["qty"]):
            step.create({"unit": unit}).then_apply("scan")
        return line

    w = (_worker(client, "dyn-nested")
         .branch_steps(name, "pick", "packed", "label", "scan")
         .handle(name, "lines", lines)
         .handle(name, "pick", pick)
         .handle(name, "scan", lambda u: {"scanned": u["unit"]})
         .handle_combine(name, "packed", lambda line: {"sku": line["sku"], "units": len(step.results())})
         .handle(name, "label", lambda p: {**p, "label": f"{p['sku']}x{p['units']}"})
         .handle_combine(name, "packed-all", lambda ctx: {"labels": [r["label"] for r in step.results()]}))
    v = _run(client, flow, w, {"lines": [{"sku": "a", "qty": 2}, {"sku": "b", "qty": 3}]})
    assert v.status == "COMPLETED", v.error
    assert v.context == {"labels": ["ax2", "bx3"]}


def test_a_compensable_branch_step_is_undone_when_the_instance_fails(client):
    name = _name("py-dyn-saga")
    flow = Graph(name, [Step("book", combine="settle"), Step("boom")]).compile()
    undone = []

    def boom(ctx):
        from wiggle import PermanentActivityError
        raise PermanentActivityError("downstream exploded")

    w = (_worker(client, "dyn-saga")
         .branch_steps(name, "hold")
         .handle(name, "book", lambda ctx: (step.create({"seat": 7}).then_apply_compensable("hold"), ctx)[1])
         .handle(name, "hold", lambda b: {**b, "held": True})
         .handle_compensation(name, "hold", undone.append)
         .handle_combine(name, "settle", lambda ctx: ctx)
         .handle(name, "boom", boom))
    v = _run(client, flow, w, {})
    assert v.status == "COMPENSATED", v.error
    assert undone and undone[0].result == {"seat": 7, "held": True}


def test_create_outside_a_spawning_step_fails_the_step(client):
    name = _name("py-dyn-misuse")
    flow = Graph(name, [Step("plain")]).compile()
    w = _worker(client, "dyn-misuse").handle(
        name, "plain", lambda ctx: (step.create({}).then_apply("x"), ctx)[1])
    v = _run(client, flow, w, {})
    assert v.status == "FAILED"
    assert "cannot create branches" in (v.error or "")


def test_queue_criteria_claim_only_the_matching_tasks(client):
    name = _name("py-crit")
    queue = name + "-q"
    flow = Graph(name, [Step("work", queue=queue)],
                 attributes=[Attribute("tier", AttrType.STRING, "customer.tier")]).compile()
    client.register(flow)
    done = []
    gold_only = c.QueueCriteria(queue, c.and_(c.equal(c.Task_.WORKFLOW, name),
                                              c.equal(c.Task_.attr("tier"), "gold"),
                                              c.less_than(c.Task_.ATTEMPT, 5)))
    w = _worker(client, "crit-gold", criteria=[gold_only]).handle(
        name, "work", lambda ctx: (done.append(ctx["customer"]["tier"]), {**ctx, "done": True})[1])
    with w.start():
        gold = client.start(flow, {"customer": {"tier": "gold"}}, correlation_id="g")
        silver = client.start(flow, {"customer": {"tier": "silver"}}, correlation_id="s")
        assert client.await_completion(gold, timeout_s=20).status == "COMPLETED"
        time.sleep(1.5)
        assert client.instance(silver).status == "RUNNING"
        slices = [s for s in client.backlog_coverage().slices if s.workflow == name]
        assert slices and slices[0].ready_count == 1 and slices[0].coverage != "COVERED"
    other = _worker(client, "crit-silver", criteria=[
        c.QueueCriteria(queue, c.in_(c.Task_.CORRELATION_ID, "s", "t"))]).handle(name, "work", lambda ctx: ctx)
    with other.start():
        assert client.await_completion(silver, timeout_s=20).status == "COMPLETED"
    assert done == ["gold"]
