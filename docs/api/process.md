# Process & Pipeline Internals

Core building blocks that power the pipeline execution engine.

## `ProcessKind`

Enum describing the transformation semantics of a pipeline stage.

```python
from anyiostream import ProcessKind
```

| Value | Meaning | Input → Output |
|-------|---------|----------------|
| `MAP` | 1:1 transform | Each input produces exactly one output |
| `FLAT_MAP` | 1:N expand | Each input produces zero or more outputs |
| `FILTER` | 1:0\|1 predicate | Each input is kept or dropped |
| `FOREACH` | Side-effect | Passes input through unchanged |

---

## `ProcessConfig`

Per-stage tunables. Frozen dataclass with `slots=True`.

```python
from anyiostream import ProcessConfig

cfg = ProcessConfig(workers=4, buffer_size=8, name="fetch")
```

### Fields

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `workers` | `int` | `1` | Concurrent fan-out workers. `1` = sequential. |
| `buffer_size` | `float` | `0` | Backpressure buffer between stages. `0` = rendezvous, `math.inf` = unbounded. |
| `name` | `str \| None` | `None` | Human-readable label for debugging / tracing. |

### Validation

- `workers` must be ≥ 1 (raises `ValueError`)
- `buffer_size` must be ≥ 0 (raises `ValueError`)

---

## `Process`

A single unit of work in the pipeline graph.

```python
from anyiostream import Process, ProcessKind, ProcessConfig
```

### Fields

| Field | Type | Description |
|-------|------|-------------|
| `func` | `Callable` | The transformation function |
| `kind` | `ProcessKind` | Transformation semantics |
| `config` | `ProcessConfig` | Concurrency & buffering tunables |

### Execution Model

Each `Process` runs inside an anyio `TaskGroup`:

1. **Input channel** — receives items from the previous stage
2. **Worker tasks** — `config.workers` concurrent tasks pull from the input
3. **Output channel** — bounded by `config.buffer_size`, feeds the next stage

Workers respect structured concurrency: if any worker raises, the entire pipeline cancels cleanly.

---

## `ResultStages` (mixin)

Mixed into `Stream` to provide Result-aware pipeline stages. You don't instantiate this directly — use it through `Stream`.

### Stages

| Method | Kind | Description |
|--------|------|-------------|
| `.try_map(fn)` | MAP | Wraps output in `Ok`; catches exceptions as `Err` |
| `.try_flat_map(fn)` | FLAT_MAP | Like `try_map` but fn returns an iterable |
| `.try_filter(fn)` | FILTER | Drops items where predicate fails; catches exceptions as `Err` |
| `.try_foreach(fn)` | FOREACH | Side-effect with error capture |

All `try_*` stages accept:

- `workers=` / `buffer_size=` / `name=` — same as regular stages
- `err=handler` — optional callback `(PipelineError) → T` invoked on error instead of emitting `Err`

### Exit Ramps

| Method | Input | Output | Description |
|--------|-------|--------|-------------|
| `.recover(fn)` | `Result[T, E]` | `T` | Unwrap `Ok`; apply `fn(error)` to `Err` |
| `.ok_only()` | `Result[T, E]` | `T` | Keep only `Ok` values, drop `Err` |
| `.errors_only()` | `Result[T, E]` | `PipelineError` | Keep only `Err` values |
| `.collect_split()` | `Result[T, E]` | `([T], [E])` | Terminal — collect into two lists |
