"""A workflow defined as a chain of steps, as in the Java client: :meth:`FlowSpec.define` hands its
body a :class:`WiggleFlow` -- a handle on the point the flow has reached while it is being *defined*
-- and each ``then_*`` call records a step and returns a handle on the new end. Nothing executes
here; the recording is compiled once, at the end, to the graph the server stores. Steps are named
as in the graph and bound on a worker by those names.

    def order(f):
        validated = f.then_apply("validate").then_filter("in-stock")

        payment = validated.then_apply("charge", queue="payments")
        shipping = validated.then_apply("reserve").then_sleep(2).then_apply("label")

        return Wiggle.all_of(payment, shipping).combine("merge").then_accept("notify")

    wf = FlowSpec.define("order", order)

Continuing a handle twice is a fan-out: ``validated`` above is continued twice, and that makes the
two arms. Arms run on isolated copies of the context, so they must be rejoined with
:meth:`Wiggle.all_of` and a combine; a split that never is is rejected when the workflow is defined.
An arm is named after its last step, which is how the combine receives its result.

The body runs once, so a Python ``for`` loop around ``then_apply`` records that many steps. Anything
that depends on a step's *result* is decided at run time instead, with :meth:`Wiggle.one_of`,
:meth:`WiggleFlow.repeat_while`, :meth:`WiggleFlow.then_for_each`, or a step that creates branches
(``then_apply("plan").combine("collect")`` with ``wiggle.step.create`` in the handler).
"""
from __future__ import annotations

from typing import Any, Callable, Optional, Union

from .workflow import (
    Attribute,
    FlowSpec,
    RetryPolicy,
    _AwaitSignal,
    _Case,
    _Choose,
    _compile,
    _DeclareAttribute,
    _DefaultQueue,
    _DoWhile,
    _ForEach,
    _Fork,
    _Gate,
    _Node,
    _Sleep,
    _SubWorkflow,
    _Task,
)

Body = Callable[["WiggleFlow"], Any]


class WiggleFlow:
    """A handle on the point a flow has reached while it is being defined. Each ``then_*`` call
    records a step after this point and returns a handle on it; this handle is unchanged, so
    continuing it again starts a second arm."""

    def __init__(self, step: "_Recorded"):
        self._step = step

    # ---- steps run on a worker

    def then_apply(self, name: str, *, queue: Optional[str] = None,
                   retry: Optional[RetryPolicy] = None) -> "WiggleFlow":
        """A task (``Worker.handle``): its handler's return becomes the context."""
        _require_name(name, "then_apply")
        return self._record(name, lambda out: out.append(_Task(name, queue, retry)), task=True)

    def then_apply_compensable(self, name: str, *, queue: Optional[str] = None,
                               retry: Optional[RetryPolicy] = None) -> "WiggleFlow":
        """A task with an undo: if the instance later fails, its compensator
        (``Worker.handle_compensation``) runs in the reverse pass with the step's input and result."""
        _require_name(name, "then_apply_compensable")
        return self._record(name, lambda out: out.append(_Task(name, queue, retry, compensate=True)),
                            task=True)

    def then_accept(self, name: str, *, queue: Optional[str] = None,
                    retry: Optional[RetryPolicy] = None) -> "WiggleFlow":
        """A side effect (``Worker.handle_effect``): the context is unchanged."""
        _require_name(name, "then_accept")
        return self._record(name, lambda out: out.append(_Task(name, queue, retry)), task=True)

    def then_filter(self, name: str, *, queue: Optional[str] = None,
                    retry: Optional[RetryPolicy] = None) -> "WiggleFlow":
        """A gate (``Worker.handle_gate``): the flow continues only while it holds. False ends the
        instance as ``gated:<name>``, or inside an arm short-circuits to that arm's join."""
        _require_name(name, "then_filter")
        return self._record(name, lambda out: out.append(_Gate(name, queue, retry)))

    def combine(self, name: str, *, queue: Optional[str] = None,
                retry: Optional[RetryPolicy] = None) -> "WiggleFlow":
        """The merge for the branches the step just chained creates at run time: inside its handler
        ``wiggle.step.create`` opens them, and this combine (``Worker.handle_combine``) receives
        their results through ``wiggle.step.results()``. It also runs when the step created none.
        Must directly follow a ``then_apply`` or ``then_accept``."""
        _require_name(name, "combine")
        creator = self._step
        if not creator.task or creator.spawns:
            raise ValueError(f"combine('{name}') must directly follow the step that creates the "
                             f"branches it combines: a then_apply or then_accept")
        creator.spawns = True

        def apply(out: list[_Node]) -> None:
            task = out[-1] if out else None
            if not isinstance(task, _Task) or task.name != creator.label:
                raise ValueError(f"combine('{name}') must directly follow the step that creates "
                                 f"the branches it combines")
            task.combine, task.combine_queue, task.combine_retry = name, queue, retry
        return self._record(name, apply)

    # ---- server-side steps

    def then_sleep(self, seconds: float = 0.0, *, millis: int = 0,
                   name: Optional[str] = None) -> "WiggleFlow":
        """A server-side timer; no worker is held while the instance waits. ``name`` labels the
        node (default ``sleep-<ms>ms``)."""
        total = int(seconds * 1000) + int(millis)
        if total < 0:
            raise ValueError(f"a sleep cannot be negative: {total}ms")
        label = name or f"sleep-{total}ms"
        return self._record(label, lambda out: out.append(_Sleep(label, total)))

    def then_await(self, signal: str, *, timeout_s: float = 0.0,
                   escalation: Optional[Body] = None) -> "WiggleFlow":
        """Waits, without holding a worker, for ``signal`` (``client.signal``); its payload merges
        into the context. With ``timeout_s`` the instance fails when it passes -- or, given an
        ``escalation`` body, that runs instead and the flow goes on after it."""
        _require_name(signal, "then_await")
        on_timeout = _body(escalation, f"the escalation of '{signal}'") if escalation else None
        return self._record(signal, lambda out: out.append(_AwaitSignal(signal, timeout_s, on_timeout)))

    def then_sub_flow(self, node: str, workflow: Union[str, FlowSpec]) -> "WiggleFlow":
        """Runs the registered workflow ``workflow`` (a name or a :class:`FlowSpec`) as a child on
        this context; its final context merges back. ``node`` names the step. The child is
        registered separately."""
        _require_name(node, "then_sub_flow")
        child = workflow.name if isinstance(workflow, FlowSpec) else workflow
        if not isinstance(child, str) or not child:
            raise ValueError("then_sub_flow needs a workflow name or a FlowSpec")
        return self._record(node, lambda out: out.append(_SubWorkflow(node, child)))

    # ---- run-time fan-out, choice and loops

    def then_for_each(self, items_key: str, body: Body, *, name: Optional[str] = None) -> "Items":
        """Runtime fan-out: the body runs once per element of the list (or map) at ``items_key``, in
        parallel, **each element being that branch's whole context**. Follow it with
        :meth:`Items.combine`, which receives every item's final value and returns the complete
        post-join context. ``name`` names the node (default ``items_key``) -- needed when the same
        key is fanned over twice."""
        _require_name(items_key, "then_for_each")
        node_name = name or items_key
        steps = _body(body, f"the body of then_for_each('{node_name}')")
        holder = _ForEach(node_name, items_key, steps)
        return Items(self._record(node_name, lambda out: out.append(holder)), holder)

    def when(self, guard: str, *, queue: Optional[str] = None,
             retry: Optional[RetryPolicy] = None) -> "WiggleFlow":
        """Opens an arm of :meth:`Wiggle.one_of`: the steps chained after it run when ``guard`` is
        the first of the choice's guards to hold. Unlike :meth:`then_filter`, a guard that fails
        hands the choice to the next arm."""
        _require_name(guard, "when")
        return WiggleFlow(_Guard(self._step, guard, queue, retry))

    def otherwise(self) -> "WiggleFlow":
        """Opens the last arm of :meth:`Wiggle.one_of`, run when none of its guards held."""
        return WiggleFlow(_Guard(self._step, None, None, None))

    def repeat_while(self, condition: str, body: Body, *, max_iterations: int = 0,
                     queue: Optional[str] = None) -> "WiggleFlow":
        """A do-while loop: ``body`` runs once, then the gate ``condition`` is evaluated on a worker,
        and while it holds the body runs again. Past ``max_iterations`` true guards the instance
        fails (0 = the engine default, 10,000)."""
        _require_name(condition, "repeat_while")
        steps = _body(body, f"the body of repeat_while('{condition}')")
        return self._record(condition,
                            lambda out: out.append(_DoWhile(condition, steps, max_iterations, queue)))

    # ---- settings recorded in the chain

    def attribute(self, name: str, type: str, path: str) -> "WiggleFlow":
        """Declares a value queue criteria can test (``Task_.attr(name)``): ``path`` is a dotted
        path into the context a step receives, read as ``type`` -- an :class:`AttrType`."""
        attr = Attribute(name, type, path)
        return self._record(None, lambda out: out.append(_DeclareAttribute(attr)))

    def default_queue(self, queue: str) -> "WiggleFlow":
        """Sets the queue of every step defined after this point that names none (before it: the
        workflow's name)."""
        _require_name(queue, "default_queue")
        return self._record(None, lambda out: out.append(_DefaultQueue(queue)))

    def _record(self, label: Optional[str], apply: Callable[[list[_Node]], None], *,
                task: bool = False) -> "WiggleFlow":
        return WiggleFlow(_Recorded(self._step, apply, label, task))


class Wiggle:
    """Joins handles that branched from a common point: :meth:`all_of` runs them in parallel,
    :meth:`one_of` runs exactly one."""

    @staticmethod
    def all_of(*arms: WiggleFlow) -> "Arms":
        """Fans the flow out over handles that branched from a common point; follow it with
        :meth:`Arms.combine`. Each arm runs on its own isolated copy of the context, so the combine
        is the only way an arm's result reaches the flow: it receives each arm's result under the
        arm's name -- the name of the arm's last step."""
        if len(arms) < 2:
            raise ValueError("all_of needs at least two arms to fan out over")
        leaves = [_leaf(a, "all_of") for a in arms]
        junction = _junction(leaves)
        paths, names = [], []
        for leaf in leaves:
            path = _claim(junction, leaf)
            paths.append(path)
            names.append(_arm_name(path))
        if len(set(names)) != len(names):
            raise ValueError(f"all_of arms must be distinguishable, but two of them end at steps "
                             f"with the same name {names}. Step names are unique, so an arm ends "
                             f"at a sleep -- give those sleeps different names with "
                             f"then_sleep(..., name=...).")
        return Arms(_ForkStep(junction, paths, names))

    @staticmethod
    def one_of(*arms: WiggleFlow) -> WiggleFlow:
        """Runs exactly one of ``arms``: the first whose guard holds, or the
        :meth:`~WiggleFlow.otherwise` arm if none did. Every arm opens with
        :meth:`~WiggleFlow.when` or ``otherwise()``, and they are evaluated in the order given. No
        combine: the arms are alternatives on the one context, so the flow continues from whichever
        ran. With no ``otherwise``, a choice where nothing matched goes on past it."""
        if not arms:
            raise ValueError("one_of needs at least one arm")
        leaves = [_leaf(a, "one_of") for a in arms]
        junction = _sole_arm_junction(leaves[0]) if len(leaves) == 1 else _junction(leaves)
        cases = []
        for leaf in leaves:
            path = _claim(junction, leaf)
            guard = path[0]
            if not isinstance(guard, _Guard):
                raise ValueError(f"every arm of one_of must open with when(...) or otherwise(): the "
                                 f"arm ending at {_describe(leaf)} starts with {_describe(guard)}")
            what = f"the when('{guard.name}') arm" if guard.name else "the otherwise arm"
            if len(path) == 1:
                raise ValueError(f"{what} of one_of has no steps")
            cases.append((guard, path[1:], what))
        return WiggleFlow(_ChoiceStep(junction, cases))


class Arms:
    """The arms of :meth:`Wiggle.all_of`, waiting for the combine they rejoin at."""

    def __init__(self, fork: "_ForkStep"):
        self._fork = fork

    def combine(self, name: str, *, queue: Optional[str] = None,
                retry: Optional[RetryPolicy] = None) -> WiggleFlow:
        """The merge handler (``Worker.handle_combine``): it receives each arm's result under the
        arm's name and returns the complete post-join context."""
        _require_name(name, "combine")
        if self._fork.combine is not None:
            raise ValueError(f"all_of({', '.join(self._fork.names)}) already has a combine "
                             f"('{self._fork.combine}'); a fan-out joins once")
        self._fork.combine, self._fork.queue, self._fork.retry = name, queue, retry
        return WiggleFlow(self._fork)


class Items:
    """The stage :meth:`WiggleFlow.then_for_each` returns, waiting for its combine."""

    def __init__(self, step: WiggleFlow, node: _ForEach):
        self._step = step
        self._node = node

    def combine(self, name: str, *, queue: Optional[str] = None,
                retry: Optional[RetryPolicy] = None) -> WiggleFlow:
        """The merge handler (``Worker.handle_combine``): it receives every item's final value
        collected under the for-each's name -- a list for a list input, a dict for a map input --
        and returns the complete post-join context."""
        _require_name(name, "combine")
        if self._node.combine:
            raise ValueError(f"then_for_each('{self._node.name}') already has a combine "
                             f"('{self._node.combine}')")
        self._node.combine, self._node.combine_queue, self._node.combine_retry = name, queue, retry
        return self._step


# ---- the recording: a tree of steps, compiled once the whole definition has been described ----
#
# A handle must be continuable more than once -- the second continuation is what makes a fan-out --
# so each then_* call appends a child to the step it was called on rather than appending to a list.
# all_of / one_of find where their arms diverged, lift each arm's path out of the trunk ("claim"
# it), and hang a fork or choice step there. A step left with two unclaimed children is a fan-out
# that was never rejoined, which compiling rejects by name.


class _Recorded:
    def __init__(self, parent: Optional["_Recorded"], apply: Optional[Callable[[list[_Node]], None]],
                 label: Optional[str], task: bool = False):
        self.parent = parent
        self._apply = apply
        self.label = label           # the graph node this step adds, for arm names and errors
        self.task = task             # a then_apply / then_accept, which a combine may follow
        self.spawns = False          # a combine already follows it
        self.children: list[_Recorded] = []
        self.claimed = False         # taken into an arm of all_of / one_of; it leaves the trunk
        if parent is not None:
            parent.children.append(self)

    def apply(self, out: list[_Node]) -> None:
        if self._apply is not None:
            self._apply(out)


class _Guard(_Recorded):
    """The marker ``when`` / ``otherwise`` records at the head of a ``one_of`` arm. It adds no node:
    the choice builds its guards itself, in arm order."""

    def __init__(self, parent: _Recorded, name: Optional[str], queue: Optional[str],
                 retry: Optional[RetryPolicy]):
        super().__init__(parent, None, None)
        self.name, self.queue, self.retry = name, queue, retry

    def apply(self, out: list[_Node]) -> None:
        what = f"when('{self.name}')" if self.name else "otherwise()"
        raise ValueError(f"{what} opens an arm of Wiggle.one_of, but its arm was never passed to one")


class _ForkStep(_Recorded):
    def __init__(self, parent: _Recorded, paths: list[list[_Recorded]], names: list[str]):
        super().__init__(parent, None, None)
        self.paths, self.names = paths, names
        self.combine: Optional[str] = None
        self.queue: Optional[str] = None
        self.retry: Optional[RetryPolicy] = None

    def apply(self, out: list[_Node]) -> None:
        if self.combine is None:
            raise ValueError(f"all_of({', '.join(self.names)}) has no merge: follow it with combine(...)")
        arms = [(name, _replay(path, f"arm '{name}'")) for name, path in zip(self.names, self.paths)]
        out.append(_Fork(arms, self.combine, self.queue, self.retry))


class _ChoiceStep(_Recorded):
    def __init__(self, parent: _Recorded, cases: list[tuple[_Guard, list[_Recorded], str]]):
        super().__init__(parent, None, None)
        self.cases = cases

    def apply(self, out: list[_Node]) -> None:
        out.append(_Choose([_Case(_replay(body, what), guard.name, guard.queue, guard.retry)
                            for guard, body, what in self.cases]))


def _define(name: str, body: Body, version: Optional[int],
            default_retry: Optional[RetryPolicy]) -> FlowSpec:
    if body is None:
        raise ValueError(f"workflow '{name}' has no body")
    nodes = _body(body, f"workflow '{name}'", allow_empty=True)
    return _compile(name, nodes, version, default_retry)


def _body(build: Body, what: str, allow_empty: bool = False) -> list[_Node]:
    """Records ``build`` on a fresh root and compiles it to a node stream."""
    root = _Recorded(None, None, None)
    build(WiggleFlow(root))
    out: list[_Node] = []
    _walk(root, out, what)
    if not out and not allow_empty:
        raise ValueError(f"{what} has no steps")
    return out


def _walk(start: _Recorded, out: list[_Node], what: str) -> None:
    """Appends ``start``'s descendants, following the single trunk at each step."""
    step = start
    while True:
        live = _unclaimed(step)
        if not live:
            return
        if len(live) > 1:
            raise ValueError(_split_error(step, what))
        step = live[0]
        step.apply(out)


def _replay(path: list[_Recorded], what: str) -> list[_Node]:
    """Compiles one arm: its claimed path, then anything chained after the arm's end."""
    out: list[_Node] = []
    for step in path:
        step.apply(out)
    _walk(path[-1], out, what)
    return out


def _unclaimed(step: _Recorded) -> list[_Recorded]:
    return [c for c in step.children if not c.claimed]


def _leaf(handle: Any, what: str) -> _Recorded:
    if not isinstance(handle, WiggleFlow):
        raise TypeError(f"{what} takes WiggleFlow handles, got {handle!r}")
    return handle._step


def _junction(leaves: list[_Recorded]) -> _Recorded:
    """The nearest step every leaf descends from -- where the fork or choice belongs."""
    ancestors = set()
    s = leaves[0].parent
    while s is not None:
        ancestors.add(id(s))
        s = s.parent
    junction = None
    for leaf in leaves[1:]:
        s = leaf.parent
        while s is not None and id(s) not in ancestors:
            s = s.parent
        if s is None:
            raise ValueError("the arms were given handles from different definitions (or from inside "
                             "a body they do not enclose); every arm must branch from one common point")
        if junction is None or _is_ancestor(junction, s):
            junction = s
    if any(a is not b and _is_ancestor(a, b) for a in leaves for b in leaves):
        raise ValueError("an arm was given a handle that is an ancestor of another; arms must be siblings")
    return junction


def _sole_arm_junction(leaf: _Recorded) -> _Recorded:
    s: Optional[_Recorded] = leaf
    while s is not None:
        if isinstance(s, _Guard):
            return s.parent
        s = s.parent
    raise ValueError(f"every arm of one_of must open with when(...) or otherwise(): the arm ending at "
                     f"{_describe(leaf)} has no guard")


def _is_ancestor(maybe: _Recorded, step: _Recorded) -> bool:
    s: Optional[_Recorded] = step
    while s is not None:
        if s is maybe:
            return True
        s = s.parent
    return False


def _claim(junction: _Recorded, leaf: _Recorded) -> list[_Recorded]:
    """The steps from just below ``junction`` down to ``leaf``, taken out of the trunk."""
    path = []
    s: Optional[_Recorded] = leaf
    while s is not junction:
        if s is None:
            raise ValueError("an arm does not descend from the point the arms branch from")
        if s.claimed:
            raise ValueError(f"the arm ending at {_describe(s)} is already an arm of another all_of or one_of")
        path.append(s)
        s = s.parent
    path.reverse()
    for p in path:
        p.claimed = True
    return path


def _arm_name(path: list[_Recorded]) -> str:
    """An arm is named after its last step that adds a named node; one ending at a nested fan-out
    after that fan-out's combine."""
    for s in reversed(path):
        if s.label is not None:
            return s.label
        if isinstance(s, _ForkStep) and s.combine is not None:
            return s.combine
    raise ValueError("an all_of arm has no step to take its name from -- it must contain at least one "
                     "step, gate, combine or sleep, since the engine keys the arm's result by that name")


def _split_error(step: _Recorded, what: str) -> str:
    heads = [_describe(c) for c in _unclaimed(step)]
    return (f"in {what}, the flow after {_describe(step)} splits into {heads} but is never rejoined. "
            f"Continuing one handle twice is a fan-out: pass both to Wiggle.all_of(...) and combine "
            f"them, or continue only one of them.")


def _describe(step: _Recorded) -> str:
    if step.label is not None:
        return f"'{step.label}'"
    if isinstance(step, _Guard):
        return f"when('{step.name}')" if step.name else "otherwise()"
    return "the start of the flow" if step.parent is None else "a fork"


def _require_name(name: str, what: str) -> None:
    if not isinstance(name, str) or not name:
        raise ValueError(f"{what} needs a step name")
