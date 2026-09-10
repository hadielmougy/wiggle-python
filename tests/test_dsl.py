"""Offline tests for the Wiggle Python topology and wire conversions -- no server required.

They build workflows as declarative Graphs and assert on the compiled definition (node kinds, edge
wiring, queues, version determinism) plus the value/diff helpers.
"""
import pytest

from wiggle import (
    AwaitSignal,
    Branch,
    Case,
    Choose,
    DoWhile,
    Effect,
    Fork,
    ForEach,
    Gate,
    Graph,
    Retry,
    Sleep,
    Step,
    SubWorkflow,
)
from wiggle._convert import from_value, to_value


def _by_id(bp):
    return {n["id"]: n for n in bp.definition["nodes"]}


def _kind(bp, kind):
    return [n for n in bp.definition["nodes"] if n["kind"] == kind]


# ---------------------------------------------------------------- linear + gate

def test_linear_chain_and_gated_end():
    bp = Graph("wf", [
        Step("a"),
        Gate("g"),
        Effect("b"),
    ]).compile()
    byid = _by_id(bp)
    a = next(n for n in byid.values() if n.get("name") == "a")
    g = next(n for n in byid.values() if n.get("name") == "g")
    b = next(n for n in byid.values() if n.get("name") == "b")
    assert bp.definition["startNode"] == a["id"]
    assert a["kind"] == "TASK" and a["next"] == g["id"]
    assert g["kind"] == "PREDICATE" and g["next"] == b["id"]
    assert a["activity"] == "wf#a"
    # the gate's false path ends the instance as gated:g
    gated = byid[g["altNext"]]
    assert gated["kind"] == "END" and gated["reason"] == "gated:g"


def test_default_queue_is_workflow_name_and_per_step_override():
    bp = Graph("orders", [
        Step("a"),
        Step("b", queue="payments"),
    ]).compile()
    byid = _by_id(bp)
    assert next(n for n in byid.values() if n.get("name") == "a")["queue"] == "orders"
    assert next(n for n in byid.values() if n.get("name") == "b")["queue"] == "payments"
    assert bp.queues == ["orders", "payments"]


def test_retry_json_shape():
    bp = Graph("wf", [Step("a", retry=Retry.exponential(5, 0.1))]).compile()
    retry = next(n for n in bp.definition["nodes"] if n.get("name") == "a")["retry"]
    assert retry == {"maxAttempts": 5, "initialBackoffMillis": 100, "multiplier": 2.0,
                     "maxBackoffMillis": 300000, "jitter": 0.2}


def test_version_is_deterministic_and_positive_int32():
    def build():
        return Graph("wf", [Step("a"), Gate("g")]).compile()
    v1, v2 = build().version, build().version
    assert v1 == v2
    assert 0 < v1 <= 0x7FFFFFFF


def test_version_is_structural_independent_of_node_ids():
    from wiggle.workflow import _content_version
    bp = Graph("wf", [
        Step("a"),
        Fork([Branch("l", [Step("l1")]), Branch("r", [Step("r1")])], combine="merge"),
        Step("z"),
    ]).compile()
    # relabel every node id to a totally different scheme, rewriting all edge references too
    remap = {n["id"]: f"x{i}" for i, n in enumerate(bp.definition["nodes"])}
    relabelled = {**bp.definition, "startNode": remap[bp.definition["startNode"]], "nodes": []}
    for n in bp.definition["nodes"]:
        m = dict(n)
        m["id"] = remap[n["id"]]
        for e in ("next", "altNext"):
            if e in m:
                m[e] = remap[m[e]]
        if "branches" in m:
            m["branches"] = [remap[b] for b in m["branches"]]
        relabelled["nodes"].append(m)
    assert _content_version(relabelled) == bp.version, "hash depends on structure, not the id numbering"


def test_version_changes_when_structure_changes():
    a = Graph("wf", [Step("a")]).compile().version
    b = Graph("wf", [Step("a"), Step("b")]).compile().version
    assert a != b


def test_explicit_version_override_and_validation():
    bp = Graph("wf", [Step("a")], version=42).compile()
    assert bp.version == 42 and bp.definition["version"] == 42
    with pytest.raises(ValueError):
        Graph("wf", [Step("a")], version=0).compile()
    with pytest.raises(ValueError):
        Graph("wf", [Step("a")], version=0x80000000).compile()
    with pytest.raises(TypeError):
        Graph("wf", [Step("a")], version=True).compile()   # bool is not a valid version


def test_empty_workflow_rejected():
    with pytest.raises(ValueError, match="no steps"):
        Graph("wf", []).compile()


# ---------------------------------------------------------------- timers / signals

def test_sleep_and_signal_nodes():
    bp = Graph("wf", [
        Sleep("nap", millis=250),
        AwaitSignal("go", timeout_s=2),
        Step("done"),
    ]).compile()
    byid = _by_id(bp)
    nap = next(n for n in byid.values() if n.get("name") == "nap")
    go = next(n for n in byid.values() if n.get("name") == "go")
    assert nap["kind"] == "SLEEP" and nap["sleepMillis"] == 250
    assert go["kind"] == "SIGNAL" and go["sleepMillis"] == 2000
    # timers/signals carry no activity (no worker handler)
    assert "activity" not in nap and "activity" not in go


def test_await_signal_escalation_branch_wiring():
    bp = Graph("wf", [
        Step("request"),
        AwaitSignal("approval", timeout_s=60, escalation=[Step("auto-approve")]),
        Step("finish"),
    ]).compile()
    byid = _by_id(bp)
    sig = next(n for n in byid.values() if n.get("name") == "approval")
    auto = next(n for n in byid.values() if n.get("name") == "auto-approve")
    finish = next(n for n in byid.values() if n.get("name") == "finish")
    assert sig["kind"] == "SIGNAL" and sig["sleepMillis"] == 60000
    assert sig["next"] == finish["id"]        # delivery continues to `finish`
    assert sig["altNext"] == auto["id"]       # timeout escalates to the branch
    assert auto["next"] == finish["id"]       # escalation rejoins the flow at `finish`
    assert auto["activity"] == "wf#auto-approve"   # the escalation step is a real worker step


def test_await_signal_without_escalation_has_no_alt_edge():
    bp = Graph("wf", [Step("a"), AwaitSignal("s", timeout_s=5), Step("b")]).compile()
    sig = next(n for n in bp.definition["nodes"] if n.get("name") == "s")
    assert "altNext" not in sig


def test_await_signal_escalation_requires_a_timeout():
    with pytest.raises(ValueError, match="positive timeout"):
        Graph("wf", [AwaitSignal("s", escalation=[Step("x")])]).compile()


def test_await_signal_empty_escalation_rejected():
    with pytest.raises(ValueError, match="defines no steps"):
        Graph("wf", [AwaitSignal("s", timeout_s=5, escalation=[])]).compile()


# ---------------------------------------------------------------- fork / join

def test_fork_creates_fork_join_and_mandatory_combine():
    bp = Graph("wf", [
        Fork([
            Branch("l", [Step("l1")]),
            Branch("r", [Step("r1"), Step("r2")]),
        ], combine="merge"),
        Step("after"),
    ]).compile()
    fork = _kind(bp, "FORK")[0]
    join = _kind(bp, "JOIN")[0]
    byid = _by_id(bp)
    assert len(fork["branches"]) == 2
    assert join["expected"] == 2
    for start in fork["branches"]:
        assert start in byid                      # branch starts exist
    # the join flows into the mandatory combine (a TASK carrying the arm names on itemsKey),
    # and only then into "after" -- there is no implicit fold
    combine = next(n for n in byid.values() if n.get("name") == "merge")
    assert combine["kind"] == "TASK"
    assert combine["itemsKey"] == '["l","r"]'
    assert join["next"] == combine["id"]
    after = next(n for n in byid.values() if n.get("name") == "after")
    assert combine["next"] == after["id"]


def test_fork_requires_two_branches():
    with pytest.raises(ValueError):
        Graph("wf", [Fork([Branch("only", [Step("x")])], combine="merge")]).compile()


def test_fork_requires_a_combine():
    with pytest.raises(ValueError, match="combine"):
        Graph("wf", [Fork([Branch("l", [Step("l1")]), Branch("r", [Step("r1")])])]).compile()


# ---------------------------------------------------------------- forEach (dynamic)

def test_for_each_dynfork_join_and_mandatory_combine():
    bp = Graph("wf", [
        ForEach("each", over="items", body=[Step("price")], combine="collect"),
        Step("sum"),
    ]).compile()
    df = _kind(bp, "DYN_FORK")[0]
    join = _kind(bp, "JOIN")[0]
    byid = _by_id(bp)
    assert df["itemsKey"] == "items"   # the element is the item's context; no itemKey injection
    assert df["branches"] and df["next"] == join["id"]      # empty-collection skip -> join
    assert "expected" not in join                           # dynamic width, not a fixed count
    # the join flows into the mandatory combine, whose itemsKey is a JSON STRING (the scratch key
    # the collected item results stage under), and only then into "sum"
    combine = next(n for n in byid.values() if n.get("name") == "collect")
    assert combine["kind"] == "TASK" and combine["itemsKey"] == '"each"'
    assert join["next"] == combine["id"]
    assert combine["next"] == next(n for n in byid.values() if n.get("name") == "sum")["id"]


def test_for_each_requires_a_combine():
    with pytest.raises(ValueError, match="combine"):
        Graph("wf", [ForEach("each", over="items", body=[Step("price")])]).compile()


# ---------------------------------------------------------------- choose

def test_choose_guard_cascade_with_otherwise():
    bp = Graph("wf", [
        Choose([
            Case(when="vip", then=[Step("v")]),
            Case(when="big", then=[Step("g")]),
            Case(then=[Step("s")]),          # no `when` -> the otherwise case (must be last)
        ]),
        Step("after"),
    ]).compile()
    byid = _by_id(bp)
    vip = next(n for n in byid.values() if n.get("name") == "vip")
    big = next(n for n in byid.values() if n.get("name") == "big")
    # vip false -> big (the next guard); big false -> the otherwise branch (a task, not a guard)
    assert vip["altNext"] == big["id"]
    assert byid[big["altNext"]]["kind"] == "TASK"
    # only the two guarded cases are predicates
    assert {n["name"] for n in _kind(bp, "PREDICATE")} == {"vip", "big"}


def test_choose_rejects_otherwise_not_last():
    with pytest.raises(ValueError):
        Graph("wf", [Choose([
            Case(then=[Step("s")]),                # otherwise first -> invalid
            Case(when="vip", then=[Step("v")]),
        ])]).compile()


# ---------------------------------------------------------------- do_while

def test_do_while_is_a_cycle():
    bp = Graph("wf", [
        DoWhile(while_="again", body=[Step("body")]),
        Step("done"),
    ]).compile()
    byid = _by_id(bp)
    cond = next(n for n in byid.values() if n.get("name") == "again")
    body = next(n for n in byid.values() if n.get("name") == "body")
    assert body["next"] == cond["id"]        # body tail feeds the condition
    assert cond["next"] == body["id"]        # true edge loops back to the body
    assert byid[cond["altNext"]]["name"] == "done"   # false edge continues
    assert cond["loopBudget"] == -1          # unstated budget = engine-default sentinel


def test_do_while_explicit_budget():
    bp = Graph("wf", [
        DoWhile(while_="again", body=[Step("body")], max_iterations=500),
    ]).compile()
    cond = next(n for n in _by_id(bp).values() if n.get("name") == "again")
    assert cond["loopBudget"] == 500

    # a plain gate carries no budget at all
    bp2 = Graph("wf", [Gate("g"), Step("a")]).compile()
    gate = next(n for n in _by_id(bp2).values() if n.get("name") == "g")
    assert "loopBudget" not in gate

    with pytest.raises(ValueError, match="max_iterations"):
        Graph("wf", [DoWhile(while_="again", body=[Step("b")], max_iterations=-2)]).compile()


# ---------------------------------------------------------------- sub-workflow

def test_sub_workflow_carries_child_name():
    child = Graph("child", [Step("x")]).compile()
    bp = Graph("parent", [
        Step("prep"),
        SubWorkflow("call", child),          # accepts a Blueprint
    ]).compile()
    sub = _kind(bp, "SUB_WORKFLOW")[0]
    assert sub["activity"] == "child" and sub["name"] == "call"
    assert "activity" in sub and sub["name"] == "call"
    # also accepts a bare workflow name
    by_name = Graph("p2", [SubWorkflow("c", "child")]).compile()
    assert _kind(by_name, "SUB_WORKFLOW")[0]["activity"] == "child"


# ---------------------------------------------------------------- validation

def test_duplicate_step_name_rejected():
    with pytest.raises(ValueError):
        Graph("wf", [Step("a"), Step("a")]).compile()


# ---------------------------------------------------------------- conversions

def test_value_round_trip_and_int_coercion():
    obj = {"s": "x", "n": 3, "f": 1.5, "b": True, "none": None, "list": [1, 2], "nested": {"k": 4}}
    back = from_value(to_value(obj))
    assert back == obj
    assert isinstance(back["n"], int) and isinstance(back["nested"]["k"], int)
    assert isinstance(back["f"], float)


def test_task_return_is_sent_whole():
    # The return REPLACES the context server-side, so the handler's whole return goes on the wire.
    from wiggle.worker import Worker
    w = Worker.__new__(Worker)
    w._handlers = {}
    w._claims = []
    w.handle("wf", "s", lambda ctx: {"a": 1})
    assert w._handlers["wf#s"]({"a": 1, "b": 2}) == {"a": 1}
    w.handle("wf", "t", lambda ctx: None)
    assert w._handlers["wf#t"]({"a": 1}) is None   # None = context untouched


# ---------------------------------------------------------------- compensation

def test_compensate_flag_serializes_only_when_set():
    from wiggle.workflow import Effect
    d = Graph("saga", [
        Step("reserve", compensate=True),
        Effect("audit", compensate=True),
        Step("plain"),
    ]).compile().definition
    by_name = {n["name"]: n for n in d["nodes"] if "name" in n}
    assert by_name["reserve"]["compensable"] is True
    assert by_name["audit"]["compensable"] is True
    # Absent (not False) when unset — the field must not disturb existing content hashes.
    assert "compensable" not in by_name["plain"]
