# Wiggle — Python client (`wiggle-python`)

An idiomatic **Python client + worker** for the Wiggle workflow engine. Import name: `wiggle`
(`from wiggle import Workflow, Retry, WiggleClient, Worker`).

## Relationship to the engine
The engine/server is a **separate repo** (`wiggle`, Java). This repo is the Python client only. It was
originally developed inside that repo under `clients/python/` and split out. The two talk over the
**same gRPC control plane**, so **Java and Python workers interoperate on one server**.

- **Wire contract:** `proto/wiggle.proto` is a **vendored copy** of the canonical proto, whose source
  of truth is the `wiggle` (engine) repo at `proto/src/main/proto/wiggle.proto`. Keep the vendored
  copy in sync when the server's proto changes.
- **Stub generation:** `./codegen.sh` runs `grpc_tools.protoc` on `proto/wiggle.proto` into
  `wiggle/_proto/` (needs `grpcio-tools`), then rewrites the generated stub's absolute
  `import wiggle_pb2` to a relative import. Manual step — not part of install/test.

## Core design invariants (do not break)
- **Interop is by activity name**, not shared version. The worker is graph-agnostic and dispatches by
  `"<workflow>#<step>"`. That's why a Python worker can serve a Java-authored workflow (and vice versa).
- **Context is a plain `dict`.** A step returns the **whole** context; the engine shallow-diffs the
  return against what it was given and merges only changed keys. There is **no typed codec** (no
  equivalent of Java's `VersionedContextCodec`).
- **Worker result conventions:** `PREDICATE` → `{"value": bool}`; `TASK` → the shallow-diff of the
  returned dict; `effect` → `null`. Raise `wiggle.PermanentError` to fail a step **non-retryably**.
- **Versioning** is a structural content hash (sha256 → 31-bit int) computed after a deterministic BFS
  relabel of node ids, so it depends on graph structure, not id numbering. `Workflow(name, version=N)`
  pins an explicit version. It is **not** equal to Java's version for the "same" workflow — again,
  interop is by activity name.
- **Execution mode: SERVER only.** No `LOCAL_SYNC`/`LOCAL_ASYNC` client-side step chaining (that's a
  Java-only capability). Serve those with a Java worker.

## API surface
- **`WiggleClient`** (control plane): `register`, `get_workflow`, `start`, `instance`,
  `await_completion`, `list_instances`, `cancel`, `signal`, schedule CRUD, `health`, `cluster`, plus
  the low-level worker RPCs (`poll`/`complete`/`fail`/`heartbeat`). TLS via
  `WiggleClient(target, credentials=grpc.ssl_channel_credentials(...))`.
- **`Worker`**: threaded poll loop + `ThreadPoolExecutor` + per-task lease heartbeat. Register
  blueprints for their handlers, or bind by name:
  - **Name-only binding:** `worker.handle(wf, step, fn)` / `handle_gate(...)` / `handle_effect(...)`
    bind a handler to an already-registered workflow **without re-declaring the topology**. On
    `start()` the worker **reconciles** against the server graph: verifies each step exists and is the
    right kind (a typo or wrong kind fails fast with the available step names) and **discovers which
    queue each step polls**. `Worker(..., await_registration_s=N)` rides out a registration race.
- **`Workflow`** DSL — full parity with the Java DSL:
  `step`/`then`/`effect`/`gate`/`fork`/`fork_each`/`choose`/`do_while`/`sub_workflow`/`sleep`/
  `await_signal` (with a timeout **escalation** branch), per-step `queue`/`retry`.
  `Retry.exponential/fixed/none/forever`.

## Testing
- **Offline** (`tests/`, no server): build workflows and assert on the compiled graph (node kinds,
  edges, queues, version determinism) + the value/diff wire conversions. This is what CI runs.
- **Live/integration:** run a server first — the Docker image `wiggle:local` (or `hadielmougy/wiggle`),
  or `./gradlew :dist:run` in the `wiggle` repo — then point the client at it. Target resolution:
  `WIGGLE_URL` env, default `localhost:8080`.

## Packaging decisions
- **Repo:** `wiggle-python`. **Import name:** `wiggle`. **PyPI distribution name:** verify `wiggle` is
  free on PyPI; if taken, publish as e.g. `wiggle-workflow`/`wiggle-sdk` via `[project].name` in
  `pyproject.toml` while the package directory stays `wiggle/`.

## Status
Split out of the `wiggle` monorepo's `clients/python/` tree with its git history preserved (via
`git subtree split`). The monorepo Gradle wiring has been removed and the proto vendored, so the repo
is self-contained: `pip install -e '.[dev]'`, `pytest -q`, `./codegen.sh`.
