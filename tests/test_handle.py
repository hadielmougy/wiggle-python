"""Offline tests for name-only handler binding (``Worker.handle``) and its start-time reconciliation
against the server's registered graph -- no server required. A tiny fake client stands in for the
control plane, returning a canned graph from ``get_workflow``; the tests drive ``_reconcile`` and the
stored wrappers directly, so ``gradle build`` exercises the binding without a live stack.
"""
import grpc
import pytest

from wiggle import Effect, Gate, Graph, Handlers, Step, Worker
from wiggle.worker import _canonical


def _graph(*, authorise_queue="payments"):
    """The registered graph as ``get_workflow`` returns it: a Java-authored order flow that a Python
    worker will implement one step of, by name."""
    return Graph("order-fulfilment", [
        Step("validate"),
        Gate("in-stock"),
        Step("authorise", queue=authorise_queue),
        Effect("audit"),
    ]).compile().definition


class _FakeRpcError(grpc.RpcError):
    def __init__(self, code):
        self._code = code

    def code(self):
        return self._code


class _FakeClient:
    """Only the surface ``_reconcile`` touches: ``get_workflow`` returning a canned dict (or raising
    NOT_FOUND for an unknown name)."""

    def __init__(self, graphs):
        self._graphs = graphs

    def get_workflow(self, name):
        if name not in self._graphs:
            raise _FakeRpcError(grpc.StatusCode.NOT_FOUND)
        return self._graphs[name]


def _worker(graphs, **kw):
    w = Worker(_FakeClient(graphs), "w-test", **kw)
    return w


# ---------------------------------------------------------------- binding + reconcile

def test_handle_registers_by_activity_name():
    w = _worker({"order-fulfilment": _graph()})
    w.handle("order-fulfilment", "authorise", lambda o: {**o, "paid": True})
    assert "order-fulfilment#authorise" in w._handlers


def test_reconcile_discovers_the_steps_queue():
    w = _worker({"order-fulfilment": _graph(authorise_queue="payments")})
    w.handle("order-fulfilment", "authorise", lambda o: o)
    w._reconcile()
    # the worker had no blueprint, so the only queue it now polls is the one learned from the graph
    assert w._served_queues == {"payments"}


def test_reconcile_defaults_queue_to_workflow_name_when_unset():
    graph = _graph()
    # drop the explicit queue so the node falls back to the workflow-name default
    for n in graph["nodes"]:
        if n.get("name") == "authorise":
            n.pop("queue", None)
    w = _worker({"order-fulfilment": graph})
    w.handle("order-fulfilment", "authorise", lambda o: o)
    w._reconcile()
    assert w._served_queues == {"order-fulfilment"}


def test_reconcile_rejects_unknown_step():
    w = _worker({"order-fulfilment": _graph()})
    w.handle("order-fulfilment", "autorise", lambda o: o)   # typo
    with pytest.raises(ValueError, match="no step 'autorise'.*available steps"):
        w._reconcile()


def test_reconcile_rejects_kind_mismatch():
    w = _worker({"order-fulfilment": _graph()})
    # "in-stock" is a PREDICATE in the graph, but bound as a task
    w.handle("order-fulfilment", "in-stock", lambda o: o)
    with pytest.raises(ValueError, match="is a PREDICATE.*handle_gate"):
        w._reconcile()


def test_handle_gate_matches_a_predicate_node():
    w = _worker({"order-fulfilment": _graph()})
    w.handle_gate("order-fulfilment", "in-stock", lambda o: o["qty"] > 0)
    w._reconcile()   # no raise: kinds agree


def test_reconcile_missing_workflow_is_fatal():
    w = _worker({})   # nothing registered
    w.handle("order-fulfilment", "authorise", lambda o: o)
    with pytest.raises(ValueError, match="is not registered"):
        w._reconcile()


def test_duplicate_binding_rejected():
    w = _worker({"order-fulfilment": _graph()})
    w.handle("order-fulfilment", "authorise", lambda o: o)
    with pytest.raises(ValueError, match="duplicate handler"):
        w.handle("order-fulfilment", "authorise", lambda o: o)


# ---------------------------------------------------------------- wrapper semantics

def test_handle_wrapper_sends_the_whole_return():
    # The return REPLACES the context server-side, so the whole return goes on the wire.
    w = _worker({"order-fulfilment": _graph()})
    w.handle("order-fulfilment", "authorise", lambda o: {**o, "paid": True})
    wrapper = w._handlers["order-fulfilment#authorise"]
    assert wrapper({"orderId": "o1", "qty": 1}) == {"orderId": "o1", "qty": 1, "paid": True}


def test_handle_effect_wrapper_returns_none():
    w = _worker({"order-fulfilment": _graph()})
    seen = {}
    w.handle_effect("order-fulfilment", "audit", lambda o: seen.update(o))
    wrapper = w._handlers["order-fulfilment#audit"]
    assert wrapper({"orderId": "o1"}) is None
    assert seen == {"orderId": "o1"}


def test_handle_gate_wrapper_returns_bool():
    w = _worker({"order-fulfilment": _graph()})
    w.handle_gate("order-fulfilment", "in-stock", lambda o: o["qty"] > 0)
    wrapper = w._handlers["order-fulfilment#in-stock"]
    assert wrapper({"qty": 3}) is True
    assert wrapper({"qty": 0}) is False


# ---------------------------------------------------------------- register_handlers (object)

def test_canonical_folds_case_styles():
    for name in ("in-stock", "in_stock", "inStock", "InStock", "instock", "IN_STOCK", "in stock"):
        assert _canonical(name) == "instock"
    assert _canonical("autoApprove") == _canonical("auto-approve") == "autoapprove"
    assert _canonical("HTTPServer") == "httpserver"


class _OrderHandlers(Handlers):
    """One method per step; names in mixed styles, kinds by return annotation. The graph step names
    are ``validate`` / ``in-stock`` / ``authorise`` / ``audit``."""
    workflow = "order-fulfilment"

    def validate(self, o) -> dict:
        return {**o, "status": "VALIDATED"}

    def inStock(self, o) -> bool:            # camelCase -> matches graph's "in-stock" gate
        return o["qty"] > 0

    def authorise(self, o) -> dict:
        return {**o, "paid": True}

    def audit(self, o) -> None:              # -> None: a side effect
        o.setdefault("_audited", []).append(o.get("orderId"))

    def _helper(self, o):                    # underscore-prefixed: ignored
        return o


def test_register_handlers_matches_by_name_and_binds_all_kinds():
    w = _worker({"order-fulfilment": _graph()})
    w.register_handlers(_OrderHandlers())    # workflow taken from the class attribute
    w._reconcile()
    handlers = w._handlers
    assert set(handlers) == {"order-fulfilment#validate", "order-fulfilment#in-stock",
                             "order-fulfilment#authorise", "order-fulfilment#audit"}
    # gate wrapper -> bool
    assert handlers["order-fulfilment#in-stock"]({"qty": 2}) is True
    assert handlers["order-fulfilment#in-stock"]({"qty": 0}) is False
    # task wrapper -> the WHOLE next context (it replaces server-side)
    assert handlers["order-fulfilment#authorise"]({"orderId": "o1", "qty": 1}) == {
        "orderId": "o1", "qty": 1, "paid": True}
    # effect wrapper -> None (context unchanged on the wire)
    assert handlers["order-fulfilment#audit"]({"orderId": "o1"}) is None
    # queue discovered from the graph
    assert "payments" in w._served_queues


def test_register_handlers_takes_explicit_workflow_over_attribute():
    class H(Handlers):
        def validate(self, o) -> dict:
            return o
    w = _worker({"order-fulfilment": _graph()})
    w.register_handlers(H(), workflow="order-fulfilment")
    w._reconcile()
    assert "order-fulfilment#validate" in w._handlers


def test_register_handlers_needs_a_workflow_name():
    class H(Handlers):
        def validate(self, o) -> dict:
            return o
    w = _worker({"order-fulfilment": _graph()})
    with pytest.raises(ValueError, match="needs a workflow name"):
        w.register_handlers(H())          # no attribute, no arg


def test_register_handlers_rejects_case_fold_duplicates():
    class H(Handlers):
        workflow = "order-fulfilment"

        def in_stock(self, o) -> bool:
            return True

        def inStock(self, o) -> bool:     # folds to the same step name -> ambiguous
            return True
    w = _worker({"order-fulfilment": _graph()})
    with pytest.raises(ValueError, match="both map to the same step name 'instock'"):
        w.register_handlers(H())


def test_register_handlers_rejects_method_matching_no_step():
    class H(Handlers):
        workflow = "order-fulfilment"

        def validate(self, o) -> dict:
            return o

        def shipItNow(self, o) -> dict:   # no such step in the graph
            return o
    w = _worker({"order-fulfilment": _graph()})
    w.register_handlers(H())
    with pytest.raises(ValueError, match="handler 'shipItNow' matches no step"):
        w._reconcile()


def test_register_handlers_rejects_kind_mismatch_from_annotation():
    class H(Handlers):
        workflow = "order-fulfilment"

        def inStock(self, o) -> dict:     # graph "in-stock" is a gate, but annotated to return dict
            return o
    w = _worker({"order-fulfilment": _graph()})
    w.register_handlers(H())
    with pytest.raises(ValueError, match="is a gate .* must return bool"):
        w._reconcile()


def test_register_handlers_gate_kind_follows_graph_when_unannotated():
    class H(Handlers):
        workflow = "order-fulfilment"

        def in_stock(self, o):            # no annotation: graph says PREDICATE -> gate wrapper
            return o.get("qty", 0) > 0
    w = _worker({"order-fulfilment": _graph()})
    w.register_handlers(H())
    w._reconcile()
    wrapper = w._handlers["order-fulfilment#in-stock"]
    assert wrapper({"qty": 5}) is True and wrapper({"qty": 0}) is False


def test_register_handlers_rejects_bad_arity():
    class H(Handlers):
        workflow = "order-fulfilment"

        def validate(self, o, extra) -> dict:   # two required args, not a handler shape
            return o
    w = _worker({"order-fulfilment": _graph()})
    with pytest.raises(ValueError, match="must accept a single context argument"):
        w.register_handlers(H())


def test_register_handlers_and_handle_conflict_is_a_duplicate():
    w = _worker({"order-fulfilment": _graph()})
    w.handle("order-fulfilment", "validate", lambda o: o)
    w.register_handlers(_OrderHandlers())
    with pytest.raises(ValueError, match="duplicate handler for activity 'order-fulfilment#validate'"):
        w._reconcile()
