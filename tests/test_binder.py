"""wiggle._binder in isolation — the binder is pure (graph in → bindings out), so every signature
rule is testable with no server, no worker, no I/O. Mirrors the Java client's HandlerBinderTest."""
import pytest

from wiggle import step
from wiggle import _binder
from wiggle.workflow import Branch, ForEach, Fork, Gate, Graph, Step


# ---------------------------------------------------------------- scan

def test_scan_rejects_case_fold_collisions():
    class H:
        def in_stock(self, ctx): return ctx
        def inStock(self, ctx): return ctx   # noqa: N802 - the collision is the point
    with pytest.raises(ValueError, match="ambiguous"):
        _binder.scan(H())


def test_scan_rejects_non_single_context_methods():
    class H:
        def work(self, ctx, extra): return ctx
    with pytest.raises(ValueError, match="single context argument"):
        _binder.scan(H())


def test_scan_rejects_an_object_with_no_handlers():
    class H:
        def _helper(self): return 1
    with pytest.raises(ValueError, match="no handler methods"):
        _binder.scan(H())


def test_scan_skips_underscore_helpers_and_workflow_attr():
    class H:
        workflow = "wf"
        def work(self, ctx): return ctx
        def _helper(self, a, b, c): return None
    assert set(_binder.scan(H())) == {"work"}


# ---------------------------------------------------------------- bind: kinds & wiring

def _linear():
    return Graph("wf", [Step("work", queue="special"), Gate("ok"), Step("other")]).compile().definition


def test_bind_kinds_queues_and_unserved():
    class H:
        def work(self, ctx): return {**ctx, "done": True}
        def ok(self, ctx) -> bool: return True
    result = _binder.bind("wf", _binder.scan(H()), _linear())

    by_step = {b.step: b for b in result.bindings}
    assert set(by_step) == {"work", "ok"}
    assert by_step["work"].queue == "special"          # explicit queue respected
    assert by_step["ok"].queue == "wf"                 # default queue = the workflow name
    assert result.unserved == ["other"]

    # the task wrapper reports the WHOLE return (it replaces server-side); the gate returns bool
    assert by_step["work"].handler({"a": 1}) == {"a": 1, "done": True}
    assert by_step["ok"].handler({}) is True


def test_bind_effect_returns_none():
    class H:
        def work(self, ctx) -> None:                    # `-> None` = a declared side effect
            ctx["seen"] = True
    result = _binder.bind("wf", _binder.scan(H()), _linear())
    handler = next(b.handler for b in result.bindings if b.step == "work")
    assert handler({"a": 1}) is None                    # context untouched on the wire


def test_bind_rejects_kind_mismatches():
    class BoolTask:
        def work(self, ctx) -> bool: return True        # a TASK must not return bool
    with pytest.raises(ValueError, match="did you mean a PREDICATE"):
        _binder.bind("wf", _binder.scan(BoolTask()), _linear())

    class DictGate:
        def ok(self, ctx) -> dict: return {}            # a gate must return bool
    with pytest.raises(ValueError, match="must return bool"):
        _binder.bind("wf", _binder.scan(DictGate()), _linear())


def test_bind_rejects_a_handler_matching_no_step():
    class H:
        def nonexistent(self, ctx): return ctx
    with pytest.raises(ValueError, match="matches no step"):
        _binder.bind("wf", _binder.scan(H()), _linear())


# ---------------------------------------------------------------- combines

def _forked():
    return Graph("wf", [
        Fork([Branch("a", [Step("a1")]), Branch("b", [Step("b1")])], combine="merge"),
    ]).compile().definition


def test_fork_combine_exposes_step_base_and_returns_verbatim():
    class H:
        def a1(self, ctx): return ctx
        def b1(self, ctx): return ctx
        def merge(self, ctx) -> dict:
            # ambient style: the pre-fork base, staged arm keys excluded
            out = dict(step.base())
            out.update(ctx["a"])
            out.update(ctx["b"])
            return out
    result = _binder.bind("wf", _binder.scan(H()), _forked())
    merge = next(b.handler for b in result.bindings if b.step == "merge")
    out = merge({"pre": "P", "a": {"x": 1}, "b": {"y": 2}})
    assert out == {"pre": "P", "x": 1, "y": 2}
    with pytest.raises(RuntimeError):
        step.base()   # the scope is confined to the invocation


def test_combine_rejects_bool_or_none_annotations():
    class H:
        def a1(self, ctx): return ctx
        def b1(self, ctx): return ctx
        def merge(self, ctx) -> None: return None       # a combine must return the full context
    with pytest.raises(ValueError, match="complete post-join context"):
        _binder.bind("wf", _binder.scan(H()), _forked())


def test_for_each_combine_base_excludes_the_scratch_key():
    graph = Graph("wf", [
        ForEach("per-item", over="items", body=[Step("norm")], combine="collect"),
    ]).compile().definition

    class H:
        def norm(self, item): return item
        def collect(self, ctx) -> dict:
            base = step.base()
            assert "per-item" not in base               # collected results excluded from the base
            return {**base, "n": len(ctx["per-item"])}
    result = _binder.bind("wf", _binder.scan(H()), graph)
    collect = next(b.handler for b in result.bindings if b.step == "collect")
    assert collect({"items": [1, 2], "per-item": ["a", "b"]}) == {"items": [1, 2], "n": 2}
