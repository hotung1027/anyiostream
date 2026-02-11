# anyiostream

<p align="center">
  <em>Composable async pipelines with structured concurrency</em>
</p>

<p align="center">
  <a href="https://github.com/hotung1027/anyiostream/actions"><img src="https://img.shields.io/github/actions/workflow/status/hotung1027/anyiostream/ci.yml?branch=main&style=flat-square" alt="CI"></a>
  <a href="https://pypi.org/project/anyiostream/"><img src="https://img.shields.io/pypi/v/anyiostream?style=flat-square" alt="PyPI"></a>
  <a href="https://pypi.org/project/anyiostream/"><img src="https://img.shields.io/pypi/pyversions/anyiostream?style=flat-square" alt="Python"></a>
  <a href="https://github.com/hotung1027/anyiostream/blob/main/LICENSE"><img src="https://img.shields.io/github/license/hotung1027/anyiostream?style=flat-square" alt="License"></a>
</p>

---

**anyiostream** provides lazy, composable async pipelines with true inter-stage concurrency, backpressure, and Rust-inspired error handling — all built on [anyio](https://github.com/agronholm/anyio) for seamless asyncio + trio support.

## Quick Example

`````{tab-set}

````{tab-item} Method Chaining
```python
from anyiostream import Stream

result = await (
    Stream.from_iterable(range(100))
    .map(lambda x: x * 2, workers=4)
    .filter(lambda x: x > 50)
    .collect()
)
```
````

````{tab-item} Pipe Operator
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
````

````{tab-item} Error Handling
```python
from anyiostream import Stream, pipe

oks, errs = await (
    Stream.from_iterable(urls)
    | pipe.try_map(fetch, workers=5)
    | pipe.try_map(parse)
    | pipe.collect_split()
)
```
````

`````

## Key Features

- **True inter-stage concurrency** — each stage runs in its own task via `TaskGroup`
- **Fan-out workers** — `workers=N` for load-balanced parallelism per stage
- **Backpressure** — bounded channels between stages prevent resource exhaustion
- **Rust-inspired `Ok`/`Err`** — railway-oriented error handling with `try_map`, `recover`, `collect_split`
- **Backend-portable** — asyncio + trio via anyio
- **Structured concurrency** — automatic cleanup, no leaked tasks

## Installation

```bash
pip install anyiostream
```

Or with [uv](https://docs.astral.sh/uv/):

```bash
uv add anyiostream
```

## Next Steps

```{toctree}
:maxdepth: 2
:caption: Getting Started

getting-started
```

```{toctree}
:maxdepth: 2
:caption: Guide

guide/pipeline-basics
guide/concurrency
guide/error-handling
```

```{toctree}
:maxdepth: 2
:caption: API Reference

api/stream
api/pipe
api/result
api/process
```

```{toctree}
:maxdepth: 1
:caption: More

comparison
```
