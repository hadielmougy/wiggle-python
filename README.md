# Wiggle — Python client

An idiomatic Python client and worker for [Wiggle](https://github.com/hadielmougy/wiggle), the durable state-machine
platform. It speaks the same gRPC control plane as the Java client, so **Python and Java workers
interoperate** on the same server: define a flow in either language, and any worker that registers the
matching handlers can run its steps.

- **Control client** — register workflows, start and track instances, deliver signals, manage schedules.
- **Worker** — implement steps by name; pull tasks you have capacity for, run handlers, report results; automatic lease heartbeats and retries.
- **Runtime branches & queue criteria** — open branches from inside a step (`wiggle.step.create`), and let a worker claim only the tasks a predicate selects.
- **Flow definitions** — define a workflow as a chain of named steps (`FlowSpec.define(name, lambda f: f.then_apply(...)...)`), with the same vocabulary as the Java client and optional per-step `retry` and `queue`.

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
from wiggle import FlowSpec, RetryPolicy, WiggleClient, Worker

# 1. Define the topology as a chain of named steps. Nothing runs here; it compiles to a graph.
wf = FlowSpec.define("order", lambda f: f
    .then_apply("validate")
    .then_filter("in-stock")                            # false -> ends as gated:in-stock
    .then_apply("charge", queue="payments", retry=RetryPolicy.exponential(5, 0.1))
    .then_sleep(1)
    .then_accept("notify"))

# 2. Implement the steps by name. The context is a plain dict; a step returns the whole context.
def charge(o):
    return {**o, "paymentRef": f"auth-{o['orderId']}"}

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
        worker.close()
```

Run the bundled examples against a server on `:8080` (after `pip install -e .`, or with `PYTHONPATH=.`):

```bash
python examples/order.py                   # a Python-authored flow, end to end
python examples/polyglot_worker.py         # serve one step of a Java-authored flow
python examples/polyglot_typed_worker.py   # the same, for the Java typed-record demo
```

## Defining a flow

`FlowSpec.define(name, body)` calls `body` once with a `WiggleFlow`, a handle on the start of the
flow. Each `then_*` call records a step and returns a handle on the new end; the recording compiles
to the `FlowSpec` you register. Steps are named as in the graph, and handlers are **not** part of
it: a worker binds them separately, by those names (below). The vocabulary is the Java client's.

| Operator | Records |
|---|---|
| `then_apply(name, queue=None, retry=None)` | a task (`handle`); its return replaces the context |
| `then_apply_compensable(name, …)` | a task with an undo, run in the reverse pass if the instance later fails (see [Compensation](#compensation-sagas)) |
| `then_accept(name, …)` | a side effect (`handle_effect`); context unchanged |
| `then_filter(name, …)` | a gate (`handle_gate`); false ends the instance as `gated:<name>` |
| `.combine(name, …)` after `then_apply`/`then_accept` | makes that step a spawning step (see [Branches created at run time](#branches-created-at-run-time)) |
| `Wiggle.all_of(*arms).combine(name, …)` | run arms **in parallel** on isolated context copies, then rejoin at the **mandatory** combine (`handle_combine`); needs ≥ 2 |
| `Wiggle.one_of(*arms)` | exclusive choice: each arm opens with `when(guard)` or, last, `otherwise()`; the first whose guard holds runs |
| `then_for_each(items_key, body, name=None).combine(name, …)` | runtime fan-out: one **isolated** branch per element of the list (or map) at `items_key`, rejoined at the **mandatory** combine |
| `repeat_while(condition, body, max_iterations=0, queue=None)` | run `body`, then repeat while the `condition` gate holds (body runs at least once). Past `max_iterations` true guards the instance fails (`0` = engine default, 10,000) |
| `then_sub_flow(node, workflow)` | run another workflow (a `FlowSpec` or name) as a child; its result merges back |
| `then_sleep(seconds=0, millis=0, name=None)` | server-side timer; no worker is held |
| `then_await(signal, timeout_s=0, escalation=None)` | wait for a signal via `client.signal(...)`; on timeout, fail — or run the `escalation` body and rejoin |
| `default_queue(queue)` | the queue of every later step that names none (before it: the workflow name) |
| `attribute(name, type, path)` | a value [queue criteria](#queue-criteria) can test |

`body`, `escalation` and the loop and for-each bodies are functions that take a handle and chain
steps onto it. `FlowSpec.define` also takes `version=` (see [Versioning](#versioning)) and
`default_retry=`, the policy for steps that name none (default: retry forever).

### Fan-out and choice

Continuing a handle twice is a fan-out. Pass the arms to `Wiggle.all_of` and name the combine
where they rejoin; a split that is never joined is rejected when the flow is defined, naming the
steps left dangling. `Wiggle.one_of` takes arms that open with `when(guard)` or `otherwise()`:

```python
from wiggle import FlowSpec, Wiggle

def order(f):
    validated = f.then_apply("validate")

    payment = validated.then_apply("charge")                        # two continuations of
    shipping = validated.then_apply("reserve").then_apply("label")  # `validated`: a fan-out
    paid = Wiggle.all_of(payment, shipping).combine("merge")        # mandatory explicit rejoin

    return Wiggle.one_of(                                           # exactly one arm runs
        paid.when("vip").then_apply("concierge"),
        paid.otherwise().then_apply("thanks"),                      # last; without it, no match skips on
    ).then_accept("notify")

wf = FlowSpec.define("order", order)
```

An arm is named after its last step (`charge` and `label` above; an arm ending in a nested
`all_of` takes its combine's name), and the combine receives each arm's result under that name.
A gate inside an arm short-circuits to that arm's join, not the whole instance. Python control flow
runs once while the flow is defined, so a `for` loop around `then_apply` records that many steps;
anything that depends on a step's result belongs in `one_of`, `repeat_while` or `then_for_each`.

The combine is bound with `handle_combine(workflow, step, fn)`: `fn` receives the context with
each arm's result under the arm's name, and must return the **complete** post-join context — the
engine replaces the context with it, so keys the handler omits do not survive the join (there is
no implicit union of the arms). The engine stages the arm results under reserved keys; the worker
hands them to `fn` under the arm names, and fails the step if an arm name is also a key of the
context itself (rename one of them):

```python
def merge(ctx):
    out = {k: v for k, v in ctx.items() if k not in ("charge", "label")}  # carry the base
    out.update(ctx.get("charge") or {})     # fold what each arm produced
    out.update(ctx.get("label") or {})
    return out

worker.handle_combine("order", "merge", merge)
```

### For each

`then_for_each` spawns one ISOLATED branch per element (list or map). The element IS each item's
context — the handler receives the value itself, and its return replaces it:

```python
from wiggle import step

wf = FlowSpec.define("charge", lambda f: f
    .then_for_each("items", lambda item: item.then_apply("price"), name="charge-items")
    .combine("collect")
    .then_apply("summarise"))
# start(wf, {"items": [1, 2, 3], "rate": 2}) -> one branch per item;
# empty/missing skips the body AND the combine

def price(item):                              # the parameter IS the element (scalars included)
    return item * step.base()["rate"]         # frozen pre-forEach context via wiggle.step (read-only)
                                              # step.item_index() / step.item_map_key() locate the element

def collect(ctx):
    # base is also available as step.base() here (the staged results excluded) — your choice
    prices = ctx.pop("charge-items")          # every item's FINAL VALUE, collected by the engine
    ctx["prices"] = prices                    # under the for-each's name: a list for a list input,
    return ctx                                # a map for a map input. Return the COMPLETE context

worker.handle("charge", "price", price)
worker.handle_combine("charge", "collect", collect)
```

Items can never touch the shared context; the combine assembles the final shape explicitly. The
for-each node is named after `items_key` unless given `name=`.

### Queues, retries, interop

Per-step `queue` defaults to the queue set by `default_queue(...)`, else the workflow name. Retry policies:
`RetryPolicy.exponential(attempts, initial_s)`, `RetryPolicy.fixed(attempts, backoff_s)`, `RetryPolicy.none()`,
`RetryPolicy.forever()`. Raise `wiggle.PermanentActivityError` from a handler to fail a step **without** retrying.

The same operators exist in the Java client (`thenApply`, `Wiggle.allOf`, …), and a flow defined in
either compiles to the same kind of graph. A sub-flow's child must be registered separately
(`client.register(child)`), and some worker must serve the child's steps too. Because dispatch is by
activity name (`"<workflow>#<step>"`), Python, Java, and Go workers interoperate: any can run
another's steps.

## Compensation (sagas)

A step recorded with `then_apply_compensable` is undone when the instance later fails: the engine runs the
step's **compensator** in the reverse pass (newest-completed first) as a real durable task, handing
it a `wiggle.Compensation` with the step's **input/result context snapshots** captured at
completion — not the instance's latest context, which a later step may have replaced. The instance
settles `COMPENSATED` (or `COMPENSATION_FAILED` if an undo exhausts its retries).

```python
w.handle("order", "reserve", reserve)
w.handle_compensation("order", "reserve",
                      lambda c: release_reservation(c.result["reservationRef"]))  # idempotent!
```

On a `register_handler` object the compensator is a method named `compensate_<step>` taking the
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
| `handle_combine(workflow, step, fn)` | combine — `fn(ctx) -> ctx`, the complete post-join context (fork, for-each, or spawning step) |
| `handle_compensation(workflow, step, fn)` | compensator — `fn(Compensation)` undoes a `then_apply_compensable` step |
| `branch_steps(workflow, *steps)` | declares steps that only run in runtime-created branches |

On `start()` the worker **reconciles** every binding against the registered graph: it checks each
step exists and is the right kind (a typo or a task bound as a gate fails fast, listing the real step
names), and it **discovers which queue each step polls** — so a name-only worker needs no queue
config. The graph must be registered *before* the worker starts; pass `await_registration_s=…` to
ride out a startup race instead of failing fast. See
[`examples/polyglot_worker.py`](examples/polyglot_worker.py) for a Java-authored flow served from
Python.

### A whole object of handlers: `register_handler`

Instead of one `handle(...)` call per step, hand the worker an object whose methods *are* the steps:

```python
from wiggle import Worker, for_flow

@for_flow("order-fulfilment")                     # as Java's @ForFlow; or pass workflow=... to register_handler
class OrderHandlers:

    def validate(self, o) -> dict:                # -> a task (returns the new context)
        return {**o, "status": "VALIDATED"}

    def inStock(self, o) -> bool:                 # -> bool: a gate; matches the step named "in-stock"
        return o["qty"] > 0

    def notify(self, o) -> None:                  # -> None: a side effect, context unchanged
        print("shipped", o["orderId"])

    def _receipt(self, o):                        # underscore -> a helper, not a step handler
        ...

worker = Worker(client, "orders").register_handler(OrderHandlers())
worker.start()
```

Each public method is matched to a step **by name, regardless of case style** — `inStock`,
`in_stock`, and a graph step named `in-stock` all fold to the same key — and its **kind comes from the
return annotation**: `-> bool` is a gate, `-> None` a side effect, anything else (or no annotation) a
task. The graph stays the source of truth for the exact step name and for gate-vs-task, so an
annotation that contradicts it fails fast. Two methods whose names collide under case-folding are
rejected at `register_handler` time (ambiguous); a method matching no step is caught on `start()`.
Prefix helpers with `_` to skip them.

### Worker options

```python
Worker(client, worker_id, *,
       concurrency=None,             # handler threads; default os.cpu_count()
       lease_s=30.0,                 # task lease, heartbeated while the handler runs
       long_poll_wait_s=10.0,
       queues=None,                  # restrict to these queues (default: the bound steps' queues)
       criteria=None,                # or: per-queue predicates (see Queue criteria); not with queues=
       await_registration_s=0.0)     # wait this long for the graph to be registered on start()

worker.start()          # background threads
worker.run_forever()    # or block until interrupted
worker.close()          # drain in-flight work and stop
```

## Client API

```python
client.register(flow_spec) -> int                     # version
client.get_workflow(name, version=None) -> dict       # the registered graph (steps, kinds, queues)
client.start(flow_spec_or_name, context, *, version=None, correlation_id=None) -> instance_id
client.instance(id) -> InstanceView                   # .status .context .termination_reason .error
client.await_completion(id, timeout_s=30) -> InstanceView
client.list_instances(workflow=None, status=None, limit=100)
client.cancel(id, reason="cancelled")
client.signal(id, name, payload=None)
client.create_schedule(workflow, every_s, context=None) / create_cron_schedule(workflow, cron, context=None)
client.schedules() / delete_schedule(id)
client.health() / client.cluster()

client.register(flow_spec, force=True)                # replace a differing graph at the same version (server must allow it)
client.poll_events(consumer, max_events=100, wait_s=10, start_from=0) -> EventBatch   # the durable event log
client.ack_events(consumer, cursor=batch.events[-1].cursor)
client.search(text, workflow=None, status=None, semantic=False, ...) -> SearchResult  # needs search shards
client.step_stats(workflow, version=0) -> StepStats    # per-step duration and queue-wait percentiles
client.backlog_coverage() -> BacklogCoverage           # ready work, and whether any worker would claim it
```

## Step metadata and events

Inside a handler, `wiggle.step` describes the running step and buffers domain events:

```python
from wiggle import step

def ship(ctx):
    step.emit("order.shipped", {"orderId": ctx["orderId"], "attempt": step.attempt()})
    return ctx
```

An emitted event rides the step's report and is committed in the same transaction as the step: an
attempt that raises leaves none behind, and its retry emits again. Consumers read emitted and
lifecycle (`wf.*`) events through `client.poll_events` / `client.ack_events`. `step.name()` and
`step.instance_id()` are available too, as are `step.base()`, `step.item_index()` and
`step.item_map_key()` inside a `then_for_each` body, and `step.create()` / `step.results()` for
[runtime branches](#branches-created-at-run-time). The worker reports each step's start and finish times, which
feed `client.step_stats`.

## Versioning

By default a workflow's version is a **content hash of its structure** — node kinds, names,
activities, queues, retries, and the edge topology — *independent of internal node-id numbering*.
So the same structure always yields the same version: re-registering is idempotent, the server
de-duplicates, and changing the graph mints a new version (in-flight instances keep running on the
old one). This is the safe, content-addressed default; you never set a number.

Pin an explicit version when you want a stable, human-meaningful one (or to match another client):

```python
wf = FlowSpec.define("order", lambda f: f.then_apply("validate"), version=3)
```

With an explicit version **you** own bumping it when the graph changes — the server overwrites the
stored graph for a reused `name:version`, which affects instances already running on it. Leave it
unset unless you have a specific reason.

> Note: `client.start("name", …)` uses the **latest** registered version. Don't register the *same*
> workflow name from two clients/definitions with different graphs; give them distinct names, or
> define the workflow in one place. (The Python content hash is its own — it does not equal the Java
> client's number for the "same" workflow; cross-language interop is by activity name, not version.)

## Branches created at run time

A step followed by `.combine(name)` is a **spawning step**: its handler may open branches with
`wiggle.step.create(input, key=None)`, each running its own chain of steps on an isolated copy of
`input`. The branches ride the step's report and commit with it. The combine then receives their
results through `wiggle.step.results()`: a list in creation order, or a dict by key when every branch
has one.

```python
from wiggle import FlowSpec, Worker, step

flow = FlowSpec.define("orders", lambda f: f.then_apply("fulfil").combine("summarise").then_apply("notify"))

def fulfil(order):
    for item in order["items"]:
        if item["digital"]:
            step.create(item).then_accept("deliver-link", queue="digital")
        else:
            step.create(item, key=item["sku"]).then_apply("reserve").then_apply("ship")
    return order

def summarise(order):                      # the context is what fulfil returned
    return {**order, "shipped": step.results()}

worker = (Worker(client, "w1")
          .branch_steps("orders", "deliver-link", "reserve", "ship")   # in no graph: bound on dispatch
          .handle("orders", "fulfil", fulfil)
          .handle_effect("orders", "deliver-link", deliver)
          .handle("orders", "reserve", reserve)                    # step.base() is fulfil's return
          .handle("orders", "ship", ship)
          .handle_combine("orders", "summarise", summarise))
```

A branch offers `then_apply`, `then_accept`, `then_filter` (a false gate ends the branch), and
`then_apply_compensable` with an undo bound by `handle_compensation`. It also offers
`then_sleep`, `then_await(signal, timeout_s=, escalation=)`, `then_sub_flow(node, workflow)`,
`then_all_of(*arms).combine(name)`, `repeat_while(condition, body, max_iterations=)` and
`then_one_of(*arms)`, where each arm opens with `when(guard)` or, last, `otherwise()`:

```python
step.create(order).then_one_of(lambda a: a.when("bulk").then_apply("pallet"),
                               lambda a: a.otherwise().then_apply("parcel"))
```

A step chained in a branch takes the
creating step's queue and retry policy unless given its own. `.combine(name)` after a branch task
makes that task spawn too, so branches nest. A combine may itself `create`, which starts another
round.

Branch-only steps are in no registered graph, so declare them with `Worker.branch_steps` (or a
`branch_steps` attribute on a `register_handler` object). They are then bound by name the first time
one is dispatched. A branch step on a queue no graph mentions runs only on a worker that polls that
queue. Calling `step.create` anywhere but a spawning step or its combine fails the step.

## Queue criteria

A worker can claim only some of a queue's tasks: a predicate over the task's workflow, version,
step, node, kind, attempt, correlation id, creation time (the fields of `Task_`), or **attributes**
the workflow declares from its context (`Task_.attr(name)`).

```python
from wiggle import AttrType, FlowSpec, Worker, criteria as c
from wiggle.criteria import Task_

flow = FlowSpec.define("orders", lambda f: f
    .attribute("tier", AttrType.STRING, "customer.tier")
    .then_apply("charge", queue="payments"))

worker = Worker(client, "gold-1", criteria=[
    c.QueueCriteria("payments", c.and_(c.equal(Task_.attr("tier"), "gold"), c.less_than(Task_.ATTEMPT, 3))),
]).handle("orders", "charge", charge)
```

The operators are `equal`, `not_equal`, `in_`, `less_than(_or_equal_to)`, `greater_than(_or_equal_to)`,
`between`, `like`, `is_null` and `is_not_null`, combined with `and_`, `or_` and `not_`. A comparison
on an absent value is false. Criteria are checked against the server's limits when built.
`criteria=` replaces `queues=`. `client.backlog_coverage()` shows ready work no live worker's criteria
would claim.

## Tests

Offline tests (no server needed) cover the topology graph shapes and the wire conversions; the
integration tests run every construct end to end against a server:

```bash
pip install -e '.[dev]'
pytest -q                                              # integration tests skip without a server
WIGGLE_TEST_URL=localhost:8080 pytest -q tests/
```

## Notes & limits

- **Execution mode:** Python-defined workflows run in `SERVER` mode. The worker does not implement
  the `LOCAL_SYNC`/`LOCAL_ASYNC` (client-side chaining) protocol, so serve those with a Java worker.
- **Numbers:** context travels as protobuf `Value` (doubles). Whole numbers come back as `int`;
  fractional values as `float` — the same JSON-number reality as the rest of the system.
- **TLS:** pass `WiggleClient(target, credentials=grpc.ssl_channel_credentials(...))`.
- **Regenerating stubs:** `pip install grpcio-tools && ./codegen.sh` after changing the proto.
