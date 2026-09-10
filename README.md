# Wiggle — Python client

An idiomatic Python client and worker for [Wiggle](../../README.md), the durable state-machine
platform. It speaks the same gRPC control plane as the Java client, so **Python and Java workers
interoperate** on the same server: define a flow in either language, and any worker that registers the
matching handlers can run its steps. In a coordinator-sharded deployment the client resolves the
owning cell per instance.

- **Control client** — register workflows, start and track instances, deliver signals, manage schedules.
- **Worker** — implement steps by name; pull tasks you have capacity for, run handlers, report results; automatic lease heartbeats and retries.
- **Declarative topology** — describe a workflow as a `Graph` of nodes (`Step`/`Gate`/`Sleep`/`Fork`/…) that mirror the graph schema, with optional per-step `retry` and `queue`.

## Install

```bash
pip install wiggle-client
# or from a checkout:
pip install -e .
```

Requires Python 3.9+ and a running Wiggle server — see the [engine repo](https://github.com/hadielmougy/wiggle)
(`docker run … hadielmougy/wiggle`, or `./gradlew :dist:run` there).

## Quick start

```python
from wiggle import Graph, Step, Gate, Effect, Sleep, Retry, WiggleClient, Worker

# 1. Describe the topology as declarative data -- a Graph that mirrors the graph schema.
wf = Graph("order", [
    Step("validate"),
    Gate("in-stock"),                                   # false -> ends as gated:in-stock
    Step("charge", queue="payments", retry=Retry.exponential(5, 0.1)),
    Sleep("cool-off", seconds=1),
    Effect("notify"),
]).compile()

# 2. Implement the steps by name. The context is a plain dict; a step returns the whole context.
def bind(w: Worker) -> Worker:
    return (w
            .handle("order", "validate", lambda o: {**o, "status": "VALIDATED"})
            .handle_gate("order", "in-stock", lambda o: o["quantity"] > 0)
            .handle("order", "charge", charge)
            .handle_effect("order", "notify", lambda o: print("shipped", o["orderId"])))

with WiggleClient("localhost:8080") as client:
    client.register(wf)
    worker = bind(Worker(client, "worker-1")).start()      # background threads
    try:
        iid = client.start(wf, {"orderId": "A-1", "quantity": 3})
        view = client.await_completion(iid, timeout_s=30)
        print(view.status, view.context)                   # COMPLETED {...}
    finally:
        worker.stop()
```

Run the bundled example against a server on `:8080`:

```bash
python examples/order.py               # after `pip install -e .` (or: PYTHONPATH=. python examples/order.py)
```

## Topology

A workflow's shape is declarative data — a `Graph(name, steps)` whose `steps` is a list of `Node`
values that mirror the graph schema. `compile()` validates the shape and returns a `Blueprint` you
register. Handlers are **not** part of the topology — a worker binds them separately, by name (below).

| Node | Meaning |
|---|---|
| `Step(name, queue=None, retry=None, compensate=False)` | a task run on a worker (`handle`); only changed context keys are merged back. `compensate=True` declares an undo run in the reverse pass if the instance later fails |
| `Effect(name, queue=None, retry=None, compensate=False)` | a side-effect step (`handle_effect`); context unchanged |
| `Gate(name, queue=None, retry=None)` | a predicate (`handle_gate`); false ends the instance as `gated:<name>` |
| `Fork([Branch(name, steps), …], combine=…)` | run branches **in parallel** on isolated context copies, then rejoin at the **mandatory** `combine` step (`handle_combine`) — no implicit fold; needs ≥ 2 |
| `ForEach(name, over, body, combine=…)` | runtime fan-out: one **isolated** branch per element of the list (or map) at `over`. **The element IS the item's context** — body handlers receive the item's value (scalars included), their return replaces it, and the frozen base rides on `wiggle.step.base()`. The **mandatory** `combine` handler receives every item's final value collected under `name` and returns the complete post-join context |
| `Choose([Case(when, then), …, Case(then)])` | exclusive choice: the first `Case` whose `when` guard holds runs; a `Case` with no `when` is the otherwise (last) |
| `DoWhile(while_, body)` | run `body`, then repeat while the `while_` predicate holds (body runs at least once) |
| `SubWorkflow(name, workflow)` | run another workflow (a `Blueprint`, `Graph`, or name) as a child; its result merges back |
| `Sleep(name, seconds=, millis=)` | server-side timer; no worker is held |
| `AwaitSignal(name, timeout_s=0, escalation=None)` | wait for a signal via `client.signal(...)`; on timeout, fail — or run the `escalation` nodes and rejoin |

A fork's combine is bound with `handle_combine(workflow, step, fn)`: `fn` receives the context with
each branch's result staged under the branch's name, and must return the **complete** post-join
context — the engine replaces the context with it, so keys the handler omits do not survive the
join (there is no implicit union of the arms):

```python
def merge(ctx):
    out = {k: v for k, v in ctx.items() if k not in ("payment", "shipping")}  # carry the base
    out.update(ctx.get("payment") or {})    # fold what each arm produced
    out.update(ctx.get("shipping") or {})
    return out

worker.handle_combine("order", "merge", merge)
```

`Branch(name, steps)`, `Case(when, then)`, and the `body`/`escalation` fields are themselves lists of
`Node`, so branches and bodies nest arbitrarily:

```python
wf = Graph("order", [
    Step("validate"),
    Fork([                                                # parallel, joined
        Branch("payment",  [Step("charge")]),
        Branch("shipping", [Step("reserve"), Step("label")]),
    ], combine="merge"),   # mandatory: branches rejoin at an explicit merge handler (handle_combine)
    Choose([                                              # exactly one arm runs
        Case(when="vip", then=[Step("concierge")]),
        Case(then=[Step("thanks")]),                      # no `when` -> the otherwise case (must be last)
    ]),
    Effect("notify"),
]).compile()
```

Branches touching different fields merge cleanly; if two write the same key, the later write wins.
A `Gate` inside a branch short-circuits to that fork's join (not the whole instance).

Runtime fan-out spawns one ISOLATED branch per element (list or map). The element IS each item's
context — the handler receives the value itself, and its return replaces it:

```python
from wiggle import step

wf = Graph("charge", [
    ForEach("charge-items", over="items", body=[
        Step("price"),
    ], combine="collect"),
    Step("summarise"),
]).compile()
# start(wf, {"items": [1, 2, 3], "rate": 2}) -> one branch per item;
# empty/missing skips the body AND the combine

def price(item):                              # the parameter IS the element (scalars included)
    return item * step.base()["rate"]         # frozen pre-forEach context via wiggle.step (read-only)

def collect(ctx):
    # base is also available as step.base() here (the staged results excluded) — your choice
    prices = ctx.pop("charge-items")          # every item's FINAL VALUE, collected by the engine:
    ctx["prices"] = prices                    # a list for a list input, a map for a map input
    return ctx                                # the COMPLETE post-join context — it replaces

worker.handle("charge", "price", price)
worker.handle_combine("charge", "collect", collect)
```

Items can never touch the shared context; the combine assembles the final shape explicitly.

Per-step `queue` defaults to `default_queue` (a `Graph` field), else the workflow name. Retry policies:
`Retry.exponential(attempts, initial_s)`, `Retry.fixed(attempts, backoff_s)`, `Retry.none()`,
`Retry.forever()`. Raise `wiggle.PermanentError` from a handler to fail a step **without** retrying.

The node set —
`Step`/`Gate`/`Effect`/`Fork`/`ForEach`/`Choose`/`DoWhile`/`SubWorkflow`/`Sleep`/`AwaitSignal` —
matches the Java DSL and the Go client's declarative structs. A `SubWorkflow`'s child must be
registered separately (`client.register(child)`), and some worker must serve the child's steps too.
Because dispatch is by activity name (`"<workflow>#<step>"`), Python, Java, and Go workers interoperate:
any can run another's steps.

## Compensation (sagas)

A step declared `compensate=True` is undone when the instance later fails: the engine runs the
step's **compensator** in the reverse pass (newest-completed first) as a real durable task, handing
it a `wiggle.Compensation` with the step's **input/result context snapshots** captured at
completion — not the instance's latest context, which a later step may have replaced. The instance
settles `COMPENSATED` (or `COMPENSATION_FAILED` if an undo exhausts its retries).

```python
w.handle("order", "reserve", reserve)
w.handle_compensation("order", "reserve",
                      lambda c: release_reservation(c.result["reservationRef"]))  # idempotent!
```

On a `register_handlers` object the compensator is a method named `compensate_<step>` taking the
`Compensation` (`compensate_reserve` undoes `reserve`). The pairing is checked both ways at start:
a compensable step served without its compensator — or a compensator targeting a non-compensable
step — refuses to bind. Compensators are at-least-once like every handler; make them idempotent.

## Binding handlers by name (polyglot)

You don't have to author a workflow in Python to implement one of its steps in Python. If the graph is
already registered (by any client — Java, Go, or Python), bind handlers **by name** — no topology
re-declaration:

```python
# implement just `charge` on a flow whose topology was authored elsewhere (e.g. in Java)
worker = Worker(client, "payments").handle(
    "order-fulfilment", "charge",
    lambda o: {**o, "paymentRef": f"auth-{o['orderId']}"})
worker.start()
```

| Method | Binds a… |
|---|---|
| `handle(workflow, step, fn)` | task — `fn(ctx) -> ctx`, only the changed keys are sent back |
| `handle_gate(workflow, step, test)` | predicate — `test(ctx) -> bool` (a gate / choose guard / do-while condition) |
| `handle_effect(workflow, step, fn)` | side effect — `fn(ctx)` runs, the context is unchanged |

On `start()` the worker **reconciles** every binding against the registered graph: it checks each
step exists and is the right kind (a typo or a task bound as a gate fails fast, listing the real step
names), and it **discovers which queue each step polls** — so a name-only worker needs no queue
config. The graph must be registered *before* the worker starts; pass `await_registration_s=…` to
ride out a startup race instead of failing fast. See
[`examples/polyglot_worker.py`](examples/polyglot_worker.py) for a Java-authored flow served from
Python.

### A whole object of handlers: `register_handlers`

Instead of one `handle(...)` call per step, hand the worker an object whose methods *are* the steps:

```python
from wiggle import Handlers, Worker

class OrderHandlers(Handlers):
    workflow = "order-fulfilment"                 # or pass workflow=... to register_handlers

    def validate(self, o) -> dict:                # -> a task (returns the new context)
        return {**o, "status": "VALIDATED"}

    def inStock(self, o) -> bool:                 # -> bool: a gate; matches the step named "in-stock"
        return o["qty"] > 0

    def notify(self, o) -> None:                  # -> None: a side effect, context unchanged
        print("shipped", o["orderId"])

    def _receipt(self, o):                        # underscore -> a helper, not a step handler
        ...

worker = Worker(client, "orders").register_handlers(OrderHandlers())
worker.start()
```

Each public method is matched to a step **by name, regardless of case style** — `inStock`,
`in_stock`, and a graph step named `in-stock` all fold to the same key — and its **kind comes from the
return annotation**: `-> bool` is a gate, `-> None` a side effect, anything else (or no annotation) a
task. The graph stays the source of truth for the exact step name and for gate-vs-task, so an
annotation that contradicts it fails fast. Two methods whose names collide under case-folding are
rejected at `register_handlers` time (ambiguous); a method matching no step is caught on `start()`.
Prefix helpers with `_` to skip them.

## Client API

```python
client.register(blueprint) -> int                     # version
client.get_workflow(name) -> dict                     # the registered graph (steps, kinds, queues)
client.start(blueprint_or_name, context, *, version=None, correlation_id=None) -> instance_id
client.instance(id) -> InstanceView                   # .status .context .termination_reason .error
client.await_completion(id, timeout_s=30) -> InstanceView
client.list_instances(workflow=None, status=None, limit=100)
client.cancel(id, reason="cancelled")
client.signal(id, name, payload=None)
client.create_schedule(workflow, every_s=..|cron=.., context=None) / list_schedules() / delete_schedule(id)
client.health() / client.cluster()
```

## Versioning

By default a workflow's version is a **content hash of its structure** — node kinds, names,
activities, queues, retries, and the edge topology — *independent of internal node-id numbering*.
So the same structure always yields the same version: re-registering is idempotent, the server
de-duplicates, and changing the graph mints a new version (in-flight instances keep running on the
old one). This is the safe, content-addressed default; you never set a number.

Pin an explicit version when you want a stable, human-meaningful one (or to match another client):

```python
wf = Graph("order", [Step("validate"), ...], version=3).compile()
```

With an explicit version **you** own bumping it when the graph changes — the server overwrites the
stored graph for a reused `name:version`, which affects instances already running on it. Leave it
unset unless you have a specific reason.

> Note: `client.start("name", …)` uses the **latest** registered version. Don't register the *same*
> workflow name from two clients/definitions with different graphs; give them distinct names, or
> define the workflow in one place. (The Python content hash is its own — it does not equal the Java
> client's number for the "same" workflow; cross-language interop is by activity name, not version.)

## Tests

Offline tests (no server needed) cover the topology graph shapes and the wire conversions:

```bash
pip install -e '.[dev]'
pytest -q
```

## Notes & limits

- **Execution mode:** Python-defined workflows run in `SERVER` mode. The worker does not implement
  the `LOCAL_SYNC`/`LOCAL_ASYNC` (client-side chaining) protocol, so serve those with a Java worker.
- **Numbers:** context travels as protobuf `Value` (doubles). Whole numbers come back as `int`;
  fractional values as `float` — the same JSON-number reality as the rest of the system.
- **TLS:** pass `WiggleClient(target, credentials=grpc.ssl_channel_credentials(...))`.
- **Regenerating stubs:** `pip install grpcio-tools && ./codegen.sh` after changing the proto.
