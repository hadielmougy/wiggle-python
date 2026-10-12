"""Offline tests for the Wiggle Python topology and wire conversions -- no server required.

They define workflows with FlowSpec.define and assert on the compiled definition (node kinds, edge
wiring, queues, version determinism) plus the value/diff helpers.
"""
import pytest

from wiggle import FlowSpec, RetryPolicy, Wiggle
from wiggle._convert import from_value, to_value


def _by_id(bp):
    return {n["id"]: n for n in bp.definition["nodes"]}


def _kind(bp, kind):
    return [n for n in bp.definition["nodes"] if n["kind"] == kind]


def _named(bp, name):
    return next(n for n in bp.definition["nodes"] if n.get("name") == name)


# ---------------------------------------------------------------- linear + gate

def test_linear_chain_and_gated_end():
    bp = FlowSpec.define("wf", lambda f: f.then_apply("a").then_filter("g").then_accept("b"))
    byid = _by_id(bp)
    a, g, b = _named(bp, "a"), _named(bp, "g"), _named(bp, "b")
    assert bp.definition["startNode"] == a["id"]
    assert a["kind"] == "TASK" and a["next"] == g["id"]
    assert g["kind"] == "PREDICATE" and g["next"] == b["id"]
    assert b["kind"] == "TASK"
    assert a["activity"] == "wf#a"
    # the gate's false path ends the instance as gated:g
    gated = byid[g["altNext"]]
    assert gated["kind"] == "END" and gated["reason"] == "gated:g"


def test_default_queue_is_workflow_name_and_per_step_override():
    bp = FlowSpec.define("orders", lambda f: f.then_apply("a").then_apply("b", queue="payments"))
    assert _named(bp, "a")["queue"] == "orders"
    assert _named(bp, "b")["queue"] == "payments"
    assert bp.queues == ["orders", "payments"]


def test_default_queue_applies_to_the_steps_after_it():
    bp = FlowSpec.define("orders", lambda f: f.then_apply("a").default_queue("bulk").then_apply("b"))
    assert _named(bp, "a")["queue"] == "orders"
    assert _named(bp, "b")["queue"] == "bulk"


def test_retry_json_shape():
    bp = FlowSpec.define("wf", lambda f: f.then_apply("a", retry=RetryPolicy.exponential(5, 0.1)))
    assert _named(bp, "a")["retry"] == {"maxAttempts": 5, "initialBackoffMillis": 100, "multiplier": 2.0,
                                        "maxBackoffMillis": 300000, "jitter": 0.2}


def test_default_retry_applies_to_steps_without_their_own():
    bp = FlowSpec.define("wf", lambda f: f.then_apply("a").then_apply("b", retry=RetryPolicy.none()),
                         default_retry=RetryPolicy.fixed(3, 1))
    assert _named(bp, "a")["retry"]["maxAttempts"] == 3
    assert _named(bp, "b")["retry"]["maxAttempts"] == 1


def test_version_is_deterministic_and_positive_int32():
    def build():
        return FlowSpec.define("wf", lambda f: f.then_apply("a").then_filter("g"))
    v1, v2 = build().version, build().version
    assert v1 == v2
    assert 0 < v1 <= 0x7FFFFFFF


def test_version_is_structural_independent_of_node_ids():
    from wiggle.workflow import _content_version

    def body(f):
        a = f.then_apply("a")
        return Wiggle.all_of(a.then_apply("l1"), a.then_apply("r1")).combine("merge").then_apply("z")
    bp = FlowSpec.define("wf", body)
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
    a = FlowSpec.define("wf", lambda f: f.then_apply("a")).version
    b = FlowSpec.define("wf", lambda f: f.then_apply("a").then_apply("b")).version
    assert a != b


def test_explicit_version_override_and_validation():
    bp = FlowSpec.define("wf", lambda f: f.then_apply("a"), version=42)
    assert bp.version == 42 and bp.definition["version"] == 42
    with pytest.raises(ValueError):
        FlowSpec.define("wf", lambda f: f.then_apply("a"), version=0)
    with pytest.raises(ValueError):
        FlowSpec.define("wf", lambda f: f.then_apply("a"), version=0x80000000)
    with pytest.raises(TypeError):
        FlowSpec.define("wf", lambda f: f.then_apply("a"), version=True)   # bool is not a valid version


def test_empty_workflow_rejected():
    with pytest.raises(ValueError, match="no steps"):
        FlowSpec.define("wf", lambda f: f)


# ---------------------------------------------------------------- timers / signals

def test_sleep_and_signal_nodes():
    bp = FlowSpec.define("wf", lambda f: f
                         .then_sleep(millis=250, name="nap")
                         .then_await("go", timeout_s=2)
                         .then_apply("done"))
    nap, go = _named(bp, "nap"), _named(bp, "go")
    assert nap["kind"] == "SLEEP" and nap["sleepMillis"] == 250
    assert go["kind"] == "SIGNAL" and go["sleepMillis"] == 2000
    # timers/signals carry no activity (no worker handler)
    assert "activity" not in nap and "activity" not in go


def test_sleep_is_named_after_its_duration_by_default():
    bp = FlowSpec.define("wf", lambda f: f.then_sleep(1.5).then_apply("a"))
    assert _kind(bp, "SLEEP")[0]["name"] == "sleep-1500ms"


def test_await_signal_escalation_branch_wiring():
    bp = FlowSpec.define("wf", lambda f: f
                         .then_apply("request")
                         .then_await("approval", timeout_s=60,
                                     escalation=lambda e: e.then_apply("auto-approve"))
                         .then_apply("finish"))
    sig, auto, finish = _named(bp, "approval"), _named(bp, "auto-approve"), _named(bp, "finish")
    assert sig["kind"] == "SIGNAL" and sig["sleepMillis"] == 60000
    assert sig["next"] == finish["id"]        # delivery continues to `finish`
    assert sig["altNext"] == auto["id"]       # timeout escalates to the branch
    assert auto["next"] == finish["id"]       # escalation rejoins the flow at `finish`
    assert auto["activity"] == "wf#auto-approve"   # the escalation step is a real worker step


def test_await_signal_without_escalation_has_no_alt_edge():
    bp = FlowSpec.define("wf", lambda f: f.then_apply("a").then_await("s", timeout_s=5).then_apply("b"))
    assert "altNext" not in _named(bp, "s")


def test_await_signal_escalation_requires_a_timeout():
    with pytest.raises(ValueError, match="positive timeout"):
        FlowSpec.define("wf", lambda f: f.then_await("s", escalation=lambda e: e.then_apply("x")))


def test_await_signal_empty_escalation_rejected():
    with pytest.raises(ValueError, match="no steps"):
        FlowSpec.define("wf", lambda f: f.then_await("s", timeout_s=5, escalation=lambda e: e))


# ---------------------------------------------------------------- all_of: fork / join

def _forked(f):
    v = f.then_apply("v")
    payment = v.then_apply("charge")
    shipping = v.then_apply("reserve").then_apply("label")
    return Wiggle.all_of(payment, shipping).combine("merge").then_apply("after")


def test_all_of_creates_fork_join_and_mandatory_combine():
    bp = FlowSpec.define("wf", _forked)
    fork, join = _kind(bp, "FORK")[0], _kind(bp, "JOIN")[0]
    byid = _by_id(bp)
    assert _named(bp, "v")["next"] == fork["id"]  # the fork lands where the arms diverged
    assert len(fork["branches"]) == 2
    assert [byid[b]["name"] for b in fork["branches"]] == ["charge", "reserve"]
    assert join["expected"] == 2
    assert _named(bp, "charge")["next"] == join["id"]
    assert _named(bp, "label")["next"] == join["id"]
    # the join flows into the mandatory combine (a TASK carrying the arm names, which are the arms'
    # last steps), and only then into "after" -- there is no implicit fold
    combine = _named(bp, "merge")
    assert combine["kind"] == "TASK"
    assert combine["armNames"] == ["charge", "label"]
    assert join["next"] == combine["id"]
    assert combine["next"] == _named(bp, "after")["id"]


def test_a_gate_in_an_arm_short_circuits_to_the_join():
    def body(f):
        return Wiggle.all_of(f.then_filter("ok").then_apply("a"), f.then_apply("b")).combine("m")
    bp = FlowSpec.define("wf", body)
    assert _named(bp, "ok")["altNext"] == _kind(bp, "JOIN")[0]["id"]


def test_an_arm_ending_at_a_nested_all_of_is_named_after_its_combine():
    def body(f):
        a = f.then_apply("a")
        b = a.then_apply("b")
        inner = Wiggle.all_of(b.then_apply("c"), b.then_apply("d")).combine("inner")
        return Wiggle.all_of(inner, a.then_apply("e")).combine("outer")
    bp = FlowSpec.define("wf", body)
    assert _named(bp, "outer")["armNames"] == ["inner", "e"]
    assert _named(bp, "inner")["armNames"] == ["c", "d"]


def test_steps_chained_after_an_arm_is_joined_still_belong_to_it():
    def body(f):
        left = f.then_apply("l1")
        out = Wiggle.all_of(left, f.then_apply("r1")).combine("m")
        left.then_apply("l2")
        return out
    bp = FlowSpec.define("wf", body)
    assert _named(bp, "l1")["next"] == _named(bp, "l2")["id"]
    assert _named(bp, "l2")["next"] == _kind(bp, "JOIN")[0]["id"]


def test_a_split_that_is_never_joined_is_rejected():
    def body(f):
        v = f.then_apply("v")
        v.then_apply("a")
        v.then_apply("b")
    with pytest.raises(ValueError, match=r"after 'v' splits into .*'a'.*'b'.* never rejoined"):
        FlowSpec.define("wf", body)


def test_all_of_requires_two_arms():
    with pytest.raises(ValueError, match="at least two"):
        Wiggle.all_of(_start().then_apply("x"))


def test_all_of_requires_a_combine():
    with pytest.raises(ValueError, match="combine"):
        FlowSpec.define("wf", lambda f: Wiggle.all_of(f.then_apply("l1"), f.then_apply("r1")))


def test_all_of_joins_once():
    def body(f):
        arms = Wiggle.all_of(f.then_apply("a"), f.then_apply("b"))
        arms.combine("m")
        arms.combine("n")
    with pytest.raises(ValueError, match="joins once"):
        FlowSpec.define("wf", body)


def test_all_of_arms_must_be_siblings():
    def body(f):
        a = f.then_apply("a")
        return Wiggle.all_of(a, a.then_apply("b")).combine("m")
    with pytest.raises(ValueError, match="siblings"):
        FlowSpec.define("wf", body)


def test_all_of_rejects_arms_with_the_same_name():
    with pytest.raises(ValueError, match="distinguishable"):
        FlowSpec.define("wf", lambda f: Wiggle.all_of(f.then_sleep(1), f.then_sleep(1)).combine("m"))


def test_handles_from_another_definition_cannot_be_joined():
    other = _start().then_apply("x")
    with pytest.raises(ValueError, match="one common point"):
        FlowSpec.define("wf", lambda f: Wiggle.all_of(f.then_apply("a"), other).combine("m"))


def _start():
    from wiggle.flow import WiggleFlow, _Recorded
    return WiggleFlow(_Recorded(None, None, None))


# ---------------------------------------------------------------- then_for_each (dynamic)

def test_for_each_dynfork_join_and_mandatory_combine():
    bp = FlowSpec.define("wf", lambda f: f
                         .then_for_each("items", lambda i: i.then_apply("price"))
                         .combine("collect")
                         .then_apply("sum"))
    df, join = _kind(bp, "DYN_FORK")[0], _kind(bp, "JOIN")[0]
    assert df["name"] == "items"                 # named after the key unless given a name
    assert df["itemsKey"] == "items"
    assert df["branches"] and df["next"] == join["id"]      # empty-collection skip -> join
    assert "expected" not in join                           # dynamic width, not a fixed count
    # the join flows into the mandatory combine, whose collectKey is the scratch key the collected
    # item results stage under, and only then into "sum"
    combine = _named(bp, "collect")
    assert combine["kind"] == "TASK" and combine["collectKey"] == "__forEach__items"
    assert join["next"] == combine["id"]
    assert combine["next"] == _named(bp, "sum")["id"]


def test_for_each_explicit_name():
    bp = FlowSpec.define("wf", lambda f: f
                         .then_for_each("items", lambda i: i.then_apply("price"), name="each")
                         .combine("collect"))
    assert _kind(bp, "DYN_FORK")[0]["name"] == "each"
    assert _named(bp, "collect")["collectKey"] == "__forEach__each"


def test_for_each_requires_a_combine():
    with pytest.raises(ValueError, match="combine"):
        FlowSpec.define("wf", lambda f: f.then_for_each("items", lambda i: i.then_apply("price")))


# ---------------------------------------------------------------- one_of: choose

def test_one_of_guard_cascade_with_otherwise():
    def body(f):
        return Wiggle.one_of(
            f.when("vip").then_apply("v"),
            f.when("big").then_apply("g"),
            f.otherwise().then_apply("s"),
        ).then_apply("after")
    bp = FlowSpec.define("wf", body)
    byid = _by_id(bp)
    vip, big = _named(bp, "vip"), _named(bp, "big")
    # vip false -> big (the next guard); big false -> the otherwise arm (a task, not a guard)
    assert vip["altNext"] == big["id"]
    assert byid[big["altNext"]]["name"] == "s"
    # only the two guarded arms are predicates, and every arm continues to "after"
    assert {n["name"] for n in _kind(bp, "PREDICATE")} == {"vip", "big"}
    for name in ("v", "g", "s"):
        assert _named(bp, name)["next"] == _named(bp, "after")["id"]


def test_one_of_a_single_guarded_arm_skips_past_when_it_does_not_hold():
    bp = FlowSpec.define("wf", lambda f: Wiggle.one_of(f.then_apply("s").when("vip").then_apply("v"))
                         .then_apply("after"))
    assert _named(bp, "s")["next"] == _named(bp, "vip")["id"]
    assert _named(bp, "vip")["altNext"] == _named(bp, "after")["id"]


def test_one_of_rejects_otherwise_not_last():
    with pytest.raises(ValueError, match="last"):
        FlowSpec.define("wf", lambda f: Wiggle.one_of(f.otherwise().then_apply("s"),
                                                      f.when("vip").then_apply("v")))


def test_one_of_arms_must_open_with_a_guard():
    with pytest.raises(ValueError, match=r"open with when\(\.\.\.\) or otherwise"):
        FlowSpec.define("wf", lambda f: Wiggle.one_of(f.when("vip").then_apply("v"), f.then_apply("s")))


def test_when_outside_one_of_is_rejected():
    with pytest.raises(ValueError, match="one_of"):
        FlowSpec.define("wf", lambda f: f.when("vip").then_apply("v"))


# ---------------------------------------------------------------- repeat_while

def test_repeat_while_is_a_cycle():
    bp = FlowSpec.define("wf", lambda f: f
                         .repeat_while("again", lambda b: b.then_apply("body"))
                         .then_apply("done"))
    byid = _by_id(bp)
    cond, body = _named(bp, "again"), _named(bp, "body")
    assert body["next"] == cond["id"]        # body tail feeds the condition
    assert cond["next"] == body["id"]        # true edge loops back to the body
    assert byid[cond["altNext"]]["name"] == "done"   # false edge continues
    assert cond["loopBudget"] == -1          # unstated budget = engine-default sentinel


def test_repeat_while_explicit_budget():
    bp = FlowSpec.define("wf", lambda f: f.repeat_while("again", lambda b: b.then_apply("body"),
                                                        max_iterations=500))
    assert _named(bp, "again")["loopBudget"] == 500

    # a plain gate carries no budget at all
    bp2 = FlowSpec.define("wf", lambda f: f.then_filter("g").then_apply("a"))
    assert "loopBudget" not in _named(bp2, "g")

    with pytest.raises(ValueError, match="max_iterations"):
        FlowSpec.define("wf", lambda f: f.repeat_while("again", lambda b: b.then_apply("b"),
                                                       max_iterations=-2))


# ---------------------------------------------------------------- sub-flow

def test_sub_flow_carries_child_name():
    child = FlowSpec.define("child", lambda f: f.then_apply("x"))
    bp = FlowSpec.define("parent", lambda f: f.then_apply("prep").then_sub_flow("call", child))
    sub = _kind(bp, "SUB_WORKFLOW")[0]
    assert sub["activity"] == "child" and sub["name"] == "call"
    # also accepts a bare workflow name
    by_name = FlowSpec.define("p2", lambda f: f.then_sub_flow("c", "child"))
    assert _kind(by_name, "SUB_WORKFLOW")[0]["activity"] == "child"


# ---------------------------------------------------------------- spawning steps

def test_combine_must_follow_a_task():
    with pytest.raises(ValueError, match="directly follow"):
        FlowSpec.define("wf", lambda f: f.then_filter("g").combine("c"))
    with pytest.raises(ValueError, match="directly follow"):
        FlowSpec.define("wf", lambda f: f.then_apply("a").combine("c").combine("d"))


# ---------------------------------------------------------------- validation

def test_duplicate_step_name_rejected():
    with pytest.raises(ValueError, match="duplicate"):
        FlowSpec.define("wf", lambda f: f.then_apply("a").then_apply("a"))


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
    d = FlowSpec.define("saga", lambda f: f
                        .then_apply_compensable("reserve")
                        .then_apply("plain")).definition
    by_name = {n["name"]: n for n in d["nodes"] if "name" in n}
    assert by_name["reserve"]["compensable"] is True
    # Absent (not False) when unset — the field must not disturb existing content hashes.
    assert "compensable" not in by_name["plain"]
