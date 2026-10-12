"""Offline checks of spawning steps in the topology, the branch builder, its wire form, and queue
criteria validation and encoding."""
from decimal import Decimal

import pytest

from wiggle import AttrType, FlowSpec, criteria as c, step
from wiggle import _binder
from wiggle import step as _step_mod
from wiggle.branch import Branch, BranchError
from wiggle.client import _branch_step, _criteria
from wiggle.workflow import RetryPolicy


def test_a_step_with_a_combine_compiles_to_a_dynamic_join_and_a_spawn_combine():
    d = FlowSpec.define("wf", lambda f: f.then_apply("fan").combine("gather").then_apply("after")).definition
    by_name = {n.get("name"): n for n in d["nodes"]}
    by_id = {n["id"]: n for n in d["nodes"]}
    join = by_id[by_name["fan"]["next"]]
    assert join["kind"] == "JOIN" and "expected" not in join
    combine = by_id[join["next"]]
    assert combine["name"] == "gather" and combine["collectKey"] == "__spawn__fan"
    assert combine["next"] == by_name["after"]["id"]
    assert _binder.is_combine(combine)


def test_declared_attributes_ride_the_definition_and_change_the_version():
    plain = FlowSpec.define("wf", lambda f: f.then_apply("a"))
    with_attr = FlowSpec.define("wf", lambda f: f.attribute("tier", AttrType.STRING, "customer.tier")
                                .then_apply("a"))
    assert with_attr.definition["attributes"] == [{"name": "tier", "type": "STRING", "path": "customer.tier"}]
    assert "attributes" not in plain.definition
    assert plain.version != with_attr.version
    for name, type_, path in [("Tier", "STRING", "x"), ("tier", "TEXT", "x"), ("tier", "STRING", "a..b")]:
        with pytest.raises(ValueError):
            FlowSpec.define("wf", lambda f: f.attribute(name, type_, path).then_apply("a"))
    with pytest.raises(ValueError, match="declared twice"):
        FlowSpec.define("wf", lambda f: f.attribute("tier", AttrType.STRING, "a")
                        .attribute("tier", AttrType.LONG, "b").then_apply("a"))


def test_branch_builder_wire_form():
    b = (Branch({"x": 1}, "k")
         .then_apply_compensable("reserve", queue="stock", retry=RetryPolicy.none())
         .combine("packed")
         .then_filter("ok")
         .then_sleep(millis=5)
         .then_await("go", timeout_s=2, escalation=lambda e: e.then_apply("escalate"))
         .then_sub_flow("ship", "shipping")
         .then_all_of(lambda a: a.then_apply("pay"), lambda a: a.then_filter("check")).combine("settle")
         .repeat_while("more", lambda body: body.then_apply("inc"), max_iterations=4)
         .then_one_of(lambda a: a.when("bulk").then_apply("b"), lambda a: a.otherwise()))
    wire = [_branch_step(s) for s in b.steps]
    assert [w.kind for w in wire] == ["TASK", "PREDICATE", "SLEEP", "SIGNAL", "SUB_WORKFLOW", "FORK",
                                      "PREDICATE", "PREDICATE"]
    reserve = wire[0]
    assert (reserve.name, reserve.queue, reserve.compensable, reserve.combine) == ("reserve", "stock", True, "packed")
    assert reserve.retry.struct_value.fields["maxAttempts"].number_value == 1
    assert wire[2].sleep_millis == 5
    assert wire[3].sleep_millis == 2000 and wire[3].escalation[0].name == "escalate"
    assert wire[4].workflow == "shipping"
    assert [[s.name for s in arm.steps] for arm in wire[5].arms] == [["pay"], ["check"]] and wire[5].combine == "settle"
    assert wire[6].name == "more" and wire[6].loop_budget == 4 and wire[6].body[0].name == "inc"
    assert [(cs.guard, [s.name for s in cs.steps]) for cs in wire[7].cases] == [("bulk", ["b"]), ("", [])]


@pytest.mark.parametrize("build", [
    lambda b: b.combine("x"),                                                   # nothing to make spawn
    lambda b: b.then_filter("g").combine("x"),                                  # a gate cannot spawn
    lambda b: b.then_all_of(lambda a: a.then_apply("a")).combine("x"),          # one arm
    lambda b: b.then_all_of(lambda a: a.then_apply("a"), lambda a: a.then_apply("a")).combine("x"),
    lambda b: b.then_all_of(lambda a: a.then_apply("a"), lambda a: a.then_sleep(1)).combine("x"),
    lambda b: b.then_one_of(lambda a: a.otherwise().then_apply("a")),
    lambda b: b.then_one_of(lambda a: a.when("g"), lambda a: a.otherwise(), lambda a: a.when("h")),
    lambda b: b.repeat_while("more", lambda body: None),
    lambda b: b.then_await("go", escalation=lambda e: e.then_apply("x")),
    lambda b: b.then_sleep(-1),
])
def test_branch_builder_refuses_malformed_chains(build):
    with pytest.raises(BranchError):
        build(Branch())


def test_create_is_refused_outside_a_spawning_step():
    token = _step_mod._begin(_step_mod._Scope(name="plain"))
    try:
        with pytest.raises(BranchError, match="cannot create branches"):
            step.create({})
    finally:
        _step_mod._end(token)
    token = _step_mod._begin(_step_mod._Scope(name="fan", creates_branches=True))
    try:
        step.create({"a": 1}, key="k").then_apply("x")
        assert len(_step_mod._current().created) == 1
    finally:
        _step_mod._end(token)


def test_branch_steps_are_left_out_of_graph_matching_and_bound_on_dispatch():
    graph = FlowSpec.define("wf", lambda f: f.then_apply("fan").combine("gather")).definition

    class H:
        def fan(self, ctx): return ctx
        def gather(self, ctx) -> dict: return ctx
        def reserve(self, ctx): return {**ctx, "r": 1}
        def compensate_reserve(self, comp) -> None: pass
        def ok(self, ctx) -> bool: return True

    cands = _binder.scan(H())
    with pytest.raises(ValueError, match="matches no step"):
        _binder.bind("wf", cands, graph)
    result = _binder.bind("wf", cands, graph, frozenset({"reserve", "ok"}))
    assert sorted(b.step for b in result.bindings) == ["fan", "gather"]
    assert _binder.bind_created("wf", cands, "reserve", "TASK", False, False)({"a": 0}) == {"a": 0, "r": 1}
    assert _binder.bind_created("wf", cands, "reserve", "TASK", False, True) is not None
    assert _binder.bind_created("wf", cands, "ok", "PREDICATE", False, False)({}) is True
    assert _binder.bind_created("wf", cands, "missing", "TASK", False, False) is None
    with pytest.raises(ValueError, match="gate"):
        _binder.bind_created("wf", cands, "ok", "TASK", False, False)


def test_criteria_encode_to_the_wire():
    q = c.QueueCriteria("pay", c.and_(c.equal(c.Task_.WORKFLOW, "orders"), c.not_(c.in_(c.Task_.ATTEMPT, 3, 4)),
                                      c.or_(c.between(c.Task_.attr("amount"), Decimal("1.50"), 10.25),
                                            c.equal(c.Task_.attr("vip"), True)),
                                      c.is_null(c.Task_.CORRELATION_ID)))
    w = _criteria(q)
    terms = w.where.all_of.terms
    assert (terms[0].cmp.field, terms[0].cmp.op, terms[0].cmp.args[0].text) == ("WORKFLOW", "EQUAL", "orders")
    assert [a.integer for a in terms[1].negated.cmp.args] == [3, 4]
    amount = terms[2].any_of.terms[0].cmp
    assert (amount.field, amount.attribute, [a.decimal for a in amount.args]) == ("ATTRIBUTE", "amount", ["1.5", "10.25"])
    assert terms[2].any_of.terms[1].cmp.args[0].flag is True
    assert terms[3].cmp.op == "IS_NULL" and not terms[3].cmp.args


@pytest.mark.parametrize("build", [
    lambda: c.equal(c.Task_.WORKFLOW, 3),                       # text field, integer literal
    lambda: c.less_than(c.Task_.ATTEMPT, "3"),                  # integer field, text literal
    lambda: c.like(c.Task_.ATTEMPT, "1%"),                      # like on an integer field
    lambda: c.less_than(c.Task_.STEP, "a"),                     # STEP: equality only
    lambda: c.between(c.Task_.VERSION, 1),                      # arity
    lambda: c.in_(c.Task_.attr("x"), "a", 1),                   # mixed attribute types
    lambda: c.less_than(c.Task_.attr("flag"), True),            # ordering a boolean
    lambda: c.equal(c.Task_.attr("x"), Decimal("1.12345678901")),   # past the decimal scale
    lambda: c.equal(c.Task_.WORKFLOW, "x" * 300),               # text too long
    lambda: c.Task_.attr("Bad"),
    lambda: c.QueueCriteria(""),
    lambda: c.QueueCriteria("q", c.not_(c.not_(c.not_(c.not_(c.not_(c.not_(c.not_(c.not_(
        c.is_null(c.Task_.STEP)))))))))),                      # deeper than 8
])
def test_criteria_refuse_what_the_server_would(build):
    with pytest.raises((ValueError, TypeError)):
        build()


def test_when_and_otherwise_open_an_arm_of_then_one_of_only():
    with pytest.raises(BranchError, match="opens an arm"):
        Branch().when("g")
    with pytest.raises(BranchError, match="opens an arm"):
        Branch().then_one_of(lambda a: a.then_apply("x").when("g"))
