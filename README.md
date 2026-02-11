<p align="center">
  <h1 align="center">anyiostream</h1>
  <p align="center">
    <em>Composable async pipelines with structured concurrency</em>
  </p>
  <p align="center">
    [![CI](https://github.com/hotung1027/anyiostream/actions/workflows/ci.yml/badge.svg)](https://github.com/hotung1027/anyiostream/actions/workflows/ci.yml)
    <a href="https://github.com/hotung1027/anyiostream/actions"><img src="https://img.shields.io/github/actions/workflow/status/hotung1027/anyiostream/ci.yml?branch=main&style=flat-square" alt="CI"></a>
    <a href="https://pypi.org/project/anyiostream/"><img src="https://img.shields.io/pypi/v/anyiostream?" alt="PyPI"></a>
    <a href="https://pypi.org/project/anyiostream/"><img src="https://img.shields.io/pypi/pyversions/anyiostream?" alt="Python"></a>
    <a href="LICENSE"><img src="https://img.shields.io/github/license/hotung1027/anyiostream?style=flat-square" alt="License"></a>
  </p>
</p>

---

**anyiostream** provides lazy, composable async pipelines with true inter-stage concurrency, backpressure, and Rust-inspired error handling — all built on [anyio](https://github.com/agronholm/anyio) for seamless asyncio + trio support.

## Why anyiostream?

Python has excellent async primitives, but a gap exists between raw concurrency tools and declarative pipeline APIs:

- **[aiostream](https://github.com/vxgmichel/aiostream)** pioneered composable `|` pipe syntax — but uses nested async generators in a **single task**. Stages execute sequentially via `__anext__()` pull chains, not as concurrent tasks. asyncio-only.
- **[anyio](https://github.com/agronholm/anyio)** provides the right primitives (`TaskGroup`, `MemoryObjectStream`, `.clone()`) — but no pipeline abstraction. Wiring a 3-stage concurrent pipeline requires ~30 lines of boilerplate.

anyiostream bridges this gap with the **CSP (Communicating Sequential Processes) pattern**: each stage runs as an independent task, connected by bounded channels — the same model as Go channels.

| Feature | aiostream | anyio (raw) | **anyiostream** |
|---------|-----------|-------------|-----------------|
| Inter-stage concurrency | No — single-task generator pull | Manual (~30 LOC per pipeline) | **Yes** — task-per-stage in TaskGroup |
| Fan-out `workers=N` | No (`task_limit` within one stage) | Manual (clone streams yourself) | **Yes** — load-balanced via `clone()` |
| Backpressure | No (pull-based) | Yes — manual wiring | **Yes** — `buffer_size` per stage |
| Result `Ok`/`Err` types | No | No | **Yes** — railway-oriented error handling |
| Backend | asyncio only | asyncio + trio | **asyncio + trio** |
| Pipe `\|` syntax | Yes | No | **Yes** |
| Structured concurrency | Partial | Yes | **Yes** — automatic cleanup |

<details>
<summary><b>30 lines of raw anyio → 4 lines of anyiostream</b></summary>

```python
# Raw anyio — manual channel wiring
async def manual_pipeline():
    s0, r0 = anyio.create_memory_object_stream(10)
    s1, r1 = anyio.create_memory_object_stream(10)
    s2, r2 = anyio.create_memory_object_stream(10)

    async def source(send):
        async with send:
            for url in urls:
                await send.send(url)

    async def worker(recv, send):
        async with recv, send:
            async for url in recv:
                await send.send(await fetch(url))

    async with anyio.create_task_group() as tg:
        tg.start_soon(source, s0)
        for _ in range(3):
            tg.start_soon(worker, r0.clone(), s1.clone())
        r0.close(); s1.close()
        # ... repeat for stage 2 ...

# anyiostream — same behavior
result = await (
    Stream.from_iterable(urls)
    .map(fetch, workers=3, buffer_size=10)
    .map(parse)
    .collect()
)
```

</details>

## Features

- **Lazy pipelines** — nothing runs until a terminal operation (`collect`, `count`, `reduce`, `first`, `take`)
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

# Custom error handler — transform Err items instead of passing through
results = await (
    Stream.from_iterable(urls)
    | pipe.try_map(fetch, workers=5, err=lambda e: log_and_rewrap(e))
    | pipe.try_map(parse, err=lambda e: e)   # pass Err unchanged explicitly
    | pipe.collect()
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
| `.try_map(fn, err=handler)` | `\| pipe.try_map(fn, err=handler)` | Map with Ok/Err wrapping |
| `.try_flat_map(fn, err=handler)` | `\| pipe.try_flat_map(fn, err=handler)` | Flat map with Ok/Err wrapping |
| `.try_filter(pred)` | `\| pipe.try_filter(pred)` | Filter Ok values, Err passes through |
| `.try_foreach(fn, err=handler)` | `\| pipe.try_foreach(fn, err=handler)` | Side-effect on Ok values |
| `.recover(fn)` | `\| pipe.recover(fn)` | Convert Err → value, unwrap Ok |
| `.ok_only()` | `\| pipe.ok_only()` | Keep Ok values, drop Err |
| `.errors_only()` | `\| pipe.errors_only()` | Keep Err values, drop Ok |

> **`err=handler`** (optional): When provided, `Err` items are transformed by `handler(error)` instead of passing through unchanged. Omit to let errors flow downstream as-is.

### Terminal Operations

| Method | Pipe Syntax | Description |
|--------|------------|-------------|
| `.collect()` | `\| pipe.collect()` | Collect all items into a list |
| `.count()` | `\| pipe.count()` | Consume all, return count |
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
2. **Terminal triggers execution** — `collect()`, `count()`, etc. materialize the pipeline.
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
