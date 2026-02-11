<p align="center">
  <h1 align="center">anyiostream</h1>
  <p align="center">
    <em>Composable async pipelines with structured concurrency</em>
  </p>
  <p align="center">
    <a href="https://github.com/JPRobotix/anyiostream/actions"><img src="https://img.shields.io/github/actions/workflow/status/JPRobotix/anyiostream/ci.yml?branch=main&style=flat-square" alt="CI"></a>
    <a href="https://pypi.org/project/anyiostream/"><img src="https://img.shields.io/pypi/v/anyiostream?style=flat-square" alt="PyPI"></a>
    <a href="https://pypi.org/project/anyiostream/"><img src="https://img.shields.io/pypi/pyversions/anyiostream?style=flat-square" alt="Python"></a>
    <a href="LICENSE"><img src="https://img.shields.io/github/license/JPRobotix/anyiostream?style=flat-square" alt="License"></a>
  </p>
</p>

---

**anyiostream** provides lazy, composable async pipelines with true inter-stage concurrency, backpressure, and Rust-inspired error handling — all built on [anyio](https://github.com/agronholm/anyio) for seamless asyncio + trio support.

## Features

- **Lazy pipelines** — nothing runs until a terminal operation (`collect`, `drain`, `reduce`, `first`, `take`)
- **True inter-stage concurrency** — each stage runs in its own task, items flow between stages via bounded channels
- **Backpressure** — bounded memory object streams prevent fast producers from overwhelming slow consumers
- **Fan-out workers** — scale any stage horizontally with `workers=N`
- **Two composition styles** — method chaining or aiostream-inspired `|` pipe syntax
- **Rust-inspired `Ok`/`Err`** — `try_map`, `try_filter`, `recover`, `collect_split` for railway-oriented error handling
- **Backend-portable** — runs on both **asyncio** and **trio** via anyio
- **Structured concurrency** — automatic cleanup via `TaskGroup`, no leaked tasks

## Installation

```bash
pip install anyiostream
```

Or with [uv](https://docs.astral.sh/uv/):

```bash
uv add anyiostream
```

## Quick Start

### Method Chaining

```python
from anyiostream import Stream

result = await (
    Stream.from_iterable(range(100))
    .map(lambda x: x * 2, workers=4)
    .filter(lambda x: x > 50)
    .collect()
)
```

### Pipe Operator (aiostream-style)

```python
from anyiostream import Stream, pipe

result = await (
    Stream.from_iterable(urls)
    | pipe.map(fetch, workers=10)
    | pipe.flat_map(extract_links, workers=5)
    | pipe.filter(is_valid)
    | pipe.map(normalize)
    | pipe.collect()
)
```

### Error Handling (Rust-style Ok/Err)

```python
from anyiostream import Stream, pipe, Ok, Err

# Exceptions become Err(PipelineError(...)) instead of crashing
oks, errs = await (
    Stream.from_iterable(urls)
    | pipe.try_map(fetch, workers=5)        # Ok(response) or Err(PipelineError)
    | pipe.try_map(parse)                    # chains on Ok, passes Err through
    | pipe.collect_split()                   # partition into (successes, failures)
)

# Or recover from errors
results = await (
    Stream.from_iterable(urls)
    | pipe.try_map(fetch, workers=5)
    | pipe.recover(lambda err: fallback(err.item))  # Err → fallback value
    | pipe.collect()
)
```

### Context Manager (manual iteration)

```python
from anyiostream import Stream

pipeline = (
    Stream.from_iterable(events)
    .map(process, workers=4)
    .filter(is_important)
)

async with pipeline.open() as items:
    async for item in items:
        await handle(item)
```

## API Overview

### Stream Constructors

| Method | Description |
|--------|-------------|
| `Stream.from_iterable(items)` | Create from sync or async iterable |
| `Stream.from_callable(factory)` | Lazy — factory called at execution time |

### Transform Stages

| Method | Pipe Syntax | Description |
|--------|------------|-------------|
| `.map(fn)` | `\| pipe.map(fn)` | 1:1 transform (sync or async) |
| `.flat_map(fn)` | `\| pipe.flat_map(fn)` | 1:N transform |
| `.filter(pred)` | `\| pipe.filter(pred)` | Keep items where pred is truthy |
| `.foreach(fn)` | `\| pipe.foreach(fn)` | Side-effect, passes items through |

### Result-Aware Stages

| Method | Pipe Syntax | Description |
|--------|------------|-------------|
| `.try_map(fn)` | `\| pipe.try_map(fn)` | Map with Ok/Err wrapping |
| `.try_flat_map(fn)` | `\| pipe.try_flat_map(fn)` | Flat map with Ok/Err wrapping |
| `.try_filter(pred)` | `\| pipe.try_filter(pred)` | Filter Ok values, Err passes through |
| `.try_foreach(fn)` | `\| pipe.try_foreach(fn)` | Side-effect on Ok values |
| `.recover(fn)` | `\| pipe.recover(fn)` | Convert Err → value, unwrap Ok |
| `.ok_only()` | `\| pipe.ok_only()` | Keep Ok values, drop Err |
| `.errors_only()` | `\| pipe.errors_only()` | Keep Err values, drop Ok |

### Terminal Operations

| Method | Pipe Syntax | Description |
|--------|------------|-------------|
| `.collect()` | `\| pipe.collect()` | Collect all items into a list |
| `.drain()` | `\| pipe.drain()` | Consume all, return count |
| `.collect_split()` | `\| pipe.collect_split()` | Partition into `(oks, errs)` |
| `.reduce(fn, init)` | — | Fold into single value |
| `.first()` | — | Return first item or None |
| `.take(n)` | — | Collect at most n items |
| `.open()` | — | Context manager for manual iteration |

### Stage Options

Every stage accepts these keyword arguments:

| Option | Default | Description |
|--------|---------|-------------|
| `workers` | `1` | Number of concurrent workers |
| `buffer_size` | `0` | Backpressure buffer (0 = rendezvous, `math.inf` = unbounded) |
| `name` | `None` | Human-readable label for debugging |

## How It Works

```
Source → [channel] → Stage 1 → [channel] → Stage 2 → [channel] → Terminal
           ↑ bounded      workers=N            workers=M
           backpressure    (fan-out)            (fan-out)
```

1. **Lazy recipe** — `Stream` holds a list of `Process` descriptors. Nothing runs yet.
2. **Terminal triggers execution** — `collect()`, `drain()`, etc. materialize the pipeline.
3. **Channel chain** — anyio `MemoryObjectStream` pairs connect each stage with bounded backpressure.
4. **Structured concurrency** — all tasks run inside a single `TaskGroup`. Cleanup is automatic.
5. **Fan-out** — `workers=N` clones the receive stream so N workers pull from the same channel (first-available-wins).

## Development

```bash
# Install dependencies
uv sync --all-extras

# Run tests (both asyncio and trio backends)
uv run pytest

# Lint & format
uv run ruff check .
uv run ruff format .
```

## Inspiration & Acknowledgements

**anyiostream** stands on the shoulders of excellent projects:

- **[aiostream](https://github.com/vxgmichel/aiostream)** by Vincent Michel — pioneered composable async stream operators with pipe syntax for asyncio. anyiostream's `| pipe.map(fn)` API is directly inspired by aiostream's elegant design.

- **[anyio](https://github.com/agronholm/anyio)** by Alex Grönholm — the structured concurrency foundation that makes anyiostream backend-portable. Memory object streams and task groups from anyio are the core execution primitives.

- **[trio](https://github.com/python-trio/trio)** by Nathaniel J. Smith — pioneered structured concurrency in Python and inspired anyio's design. Trio's philosophy of "make concurrency correct by default" deeply influences anyiostream's automatic cleanup guarantees.

- **Rust's [`Result<T, E>`](https://doc.rust-lang.org/std/result/)** — the `Ok`/`Err` pattern for railway-oriented error handling. anyiostream's `try_map`, `recover`, and `collect_split` bring this pattern to async Python pipelines, letting errors flow as values instead of crashing silently.

## License

[Apache License 2.0](LICENSE)
