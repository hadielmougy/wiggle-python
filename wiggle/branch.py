"""Branches a running step creates (``wiggle.step.create``): each one runs its own chain of steps on
an isolated context once the step's report is committed, and its result reaches the combine after
the step. Steps are named as in the topology -- by step name, bound on the worker by that name --
and nothing here runs them.

    for item in order["items"]:
        step.create(item, key=item["sku"]).then_apply("reserve").then_apply("ship", queue="shipping")

A step without its own queue or retry policy takes the creating step's. A step on a queue no graph
mentions runs only on a worker that polls that queue.
"""
from __future__ import annotations

from typing import Any, Callable, Optional

from .workflow import RetryPolicy


class BranchError(ValueError):
    """A branch was built wrongly. The step fails without a retry: its handler would build it the
    same way again."""


class Branch:
    """A chain of steps a branch runs, built with the ``then_*`` operators."""

    def __init__(self, input: Any = None, key: Optional[str] = None):
        self.input = input
        self.key = key
        self.steps: list[dict] = []
        # set on an arm of then_one_of: the guard that opened it, or _OTHERWISE
        self._choice_arm = False
        self._guard: Optional[str] = None

    def then_apply(self, name: str, *, queue: Optional[str] = None,
                   retry: Optional[RetryPolicy] = None) -> "Branch":
        """A task: its handler's return becomes the branch's context."""
        return self._add(name, "TASK", queue, retry, False)

    def then_apply_compensable(self, name: str, *, queue: Optional[str] = None,
                               retry: Optional[RetryPolicy] = None) -> "Branch":
        """A task with an undo: its compensator (``Worker.handle_compensation``) runs in the reverse
        pass if the instance later fails."""
        return self._add(name, "TASK", queue, retry, True)

    def then_accept(self, name: str, *, queue: Optional[str] = None,
                    retry: Optional[RetryPolicy] = None) -> "Branch":
        """A side effect: the branch's context is left unchanged."""
        return self._add(name, "TASK", queue, retry, False)

    def then_filter(self, name: str, *, queue: Optional[str] = None,
                    retry: Optional[RetryPolicy] = None) -> "Branch":
        """A gate: false ends this branch at the join, as a false gate ends a fork arm."""
        return self._add(name, "PREDICATE", queue, retry, False)

    def combine(self, name: str) -> "Branch":
        """Makes the task just chained create branches of its own: inside its handler
        ``step.create`` opens them, and the combine ``name`` receives their results. The branch goes
        on with the combine's return."""
        last = self.steps[-1] if self.steps else None
        if last is None or last["kind"] != "TASK" or last.get("combine") or not name:
            raise BranchError(f"combine({name!r}) must directly follow the step that creates the "
                              f"branches it combines: a then_apply or then_accept")
        last["combine"] = name
        return self

    def then_sleep(self, seconds: float = 0.0, *, millis: int = 0) -> "Branch":
        total = int(seconds * 1000) + int(millis)
        if total < 0:
            raise BranchError(f"a sleep cannot be negative: {total}ms")
        self.steps.append({"kind": "SLEEP", "sleep_millis": total})
        return self

    def then_await(self, signal: str, *, timeout_s: float = 0.0,
                   escalation: Optional[Callable[["Branch"], Any]] = None) -> "Branch":
        """Waits, without holding a worker, for ``signal``. With ``timeout_s`` the instance fails
        when it passes -- or ``escalation`` runs instead and the branch goes on after it."""
        if not signal:
            raise BranchError("then_await needs a signal name")
        if timeout_s < 0 or (escalation is not None and timeout_s <= 0):
            raise BranchError(f"a wait's timeout must be positive: {timeout_s}")
        on_timeout = _chain(escalation, f"the escalation of '{signal}'") if escalation else []
        self.steps.append({"name": signal, "kind": "SIGNAL", "sleep_millis": int(timeout_s * 1000),
                           "escalation": on_timeout})
        return self

    def then_sub_flow(self, node: str, workflow: str) -> "Branch":
        """Runs the registered workflow ``workflow`` as a child on this branch's context; its final
        context becomes the branch's. ``node`` names the step."""
        if not node or not workflow:
            raise BranchError("then_sub_flow needs a node name and a workflow")
        self.steps.append({"name": node, "kind": "SUB_WORKFLOW", "workflow": workflow})
        return self

    def then_all_of(self, *arms: Callable[["Branch"], Any]) -> "_Arms":
        """Runs ``arms`` in parallel, each on its own copy of this branch's context; the combine
        named by ``.combine(name)`` receives each arm's result under the arm's name -- its last
        step's, which must be a task or a gate."""
        if len(arms) < 2:
            raise BranchError("then_all_of needs at least two arms")
        chains = [_chain(a, f"arm {i}") for i, a in enumerate(arms)]
        names = []
        for i, chain in enumerate(chains):
            last = chain[-1]
            if last["kind"] not in ("TASK", "PREDICATE"):
                raise BranchError(f"arm {i} of then_all_of must end in a task or a gate")
            if last["name"] in names:
                raise BranchError(f"two arms of then_all_of end in '{last['name']}'; an arm is named "
                                  f"after its last step")
            names.append(last["name"])
        return _Arms(self, chains)

    def repeat_while(self, condition: str, body: Callable[["Branch"], Any], *,
                     max_iterations: int = 0) -> "Branch":
        """Runs ``body``, then again while the gate ``condition`` holds; past ``max_iterations``
        (0 = the engine's default) the instance fails."""
        if not condition:
            raise BranchError("repeat_while needs a condition step name")
        if max_iterations < 0:
            raise BranchError(f"max_iterations must be positive: {max_iterations}")
        self.steps.append({"name": condition, "kind": "PREDICATE",
                           "body": _chain(body, f"the body of repeat_while({condition})"),
                           "loop_budget": max_iterations})
        return self

    def then_one_of(self, *arms: Callable[["Branch"], Any]) -> "Branch":
        """Runs the first of ``arms`` whose guard holds: each arm opens with :meth:`when` or, last,
        :meth:`otherwise`. Without an otherwise, a choice where no guard held goes on past it.

            b.then_one_of(lambda a: a.when("bulk").then_apply("pallet"),
                          lambda a: a.otherwise().then_apply("parcel"))"""
        cases = []
        for i, build in enumerate(arms):
            arm = Branch()
            arm._choice_arm = True
            build(arm)
            if arm._guard is None:
                raise BranchError(f"arm {i} of then_one_of must open with when(...) or otherwise()")
            if arm._guard is _OTHERWISE and i != len(arms) - 1:
                raise BranchError("otherwise() must be the last arm of then_one_of")
            cases.append((None if arm._guard is _OTHERWISE else arm._guard, arm.steps))
        if not cases or cases[0][0] is None:
            raise BranchError("then_one_of needs an arm opened with when(...)")
        self.steps.append({"kind": "PREDICATE", "cases": cases})
        return self

    def when(self, guard: str) -> "Branch":
        """Opens an arm of :meth:`then_one_of`: the steps chained after it run when the gate
        ``guard`` holds."""
        if not guard:
            raise BranchError("when() needs a guard step name")
        self._open_arm("when")
        self._guard = guard
        return self

    def otherwise(self) -> "Branch":
        """Opens the last arm of :meth:`then_one_of`, run when no guard held."""
        self._open_arm("otherwise")
        self._guard = _OTHERWISE
        return self

    def _open_arm(self, what: str) -> None:
        if not self._choice_arm or self.steps or self._guard is not None:
            raise BranchError(f"{what}(...) opens an arm of then_one_of, so it comes first in one")

    def _add(self, name: str, kind: str, queue: Optional[str], retry: Optional[RetryPolicy],
             compensate: bool) -> "Branch":
        if not name:
            raise BranchError("a branch step needs a name")
        step = {"name": name, "kind": kind, "compensable": compensate}
        if queue:
            step["queue"] = queue
        if retry is not None:
            step["retry"] = retry.to_json()
        self.steps.append(step)
        return self


class _Arms:
    """The arms of :meth:`Branch.then_all_of`, waiting for the combine they rejoin at."""

    def __init__(self, branch: Branch, chains: list[list[dict]]):
        self._branch = branch
        self._chains = chains

    def combine(self, name: str) -> Branch:
        """The combine step ``name`` receives each arm's result under the arm's name; the branch
        goes on with its return."""
        if not name:
            raise BranchError("then_all_of needs a combine step name")
        self._branch.steps.append({"kind": "FORK", "combine": name, "arms": self._chains})
        return self._branch


_OTHERWISE = object()


def _chain(build: Callable[[Branch], Any], what: str) -> list[dict]:
    sub = Branch()
    build(sub)
    if not sub.steps:
        raise BranchError(f"{what} has no steps")
    return sub.steps
