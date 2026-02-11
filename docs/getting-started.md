# Getting Started

## Installation

=== "pip"

    ```bash
    pip install anyiostream
    ```

=== "uv"

    ```bash
    uv add anyiostream
    ```

=== "poetry"

    ```bash
    poetry add anyiostream
    ```

**Requirements:** Python 3.12+. The only runtime dependency is [anyio](https://github.com/agronholm/anyio) ≥ 4.8.0.

## Your First Pipeline

```python
import anyio
from anyiostream import Stream

async def main():
    result = await (
        Stream.from_iterable(range(10))
        .map(lambda x: x * 2)
        .filter(lambda x: x > 10)
        .collect()
    )
    print(result)  # [12, 14, 16, 18]

anyio.run(main)
```

## What Just Happened?

1. `Stream.from_iterable(range(10))` — creates a lazy pipeline source
2. `.map(lambda x: x * 2)` — adds a 1:1 transform stage (nothing runs yet)
3. `.filter(lambda x: x > 10)` — adds a filter stage (still nothing runs)
4. `.collect()` — **terminal operation** that materializes the pipeline

When `collect()` is called, anyiostream:

- Creates bounded channels between each stage
- Spawns each stage as a separate task inside a `TaskGroup`
- Items flow concurrently through the channel chain
- Results are collected into a list

## Adding Concurrency

Scale any stage with `workers=N`:

```python
async def fetch(url: str) -> str:
    async with httpx.AsyncClient() as client:
        r = await client.get(url)
        return r.text

result = await (
    Stream.from_iterable(urls)
    .map(fetch, workers=10, buffer_size=20)  # 10 concurrent fetchers
    .map(parse, workers=4)                    # 4 concurrent parsers
    .collect()
)
```

## Using Pipe Syntax

The `|` operator provides an alternative composition style inspired by [aiostream](https://github.com/vxgmichel/aiostream):

```python
from anyiostream import Stream, pipe

result = await (
    Stream.from_iterable(urls)
    | pipe.map(fetch, workers=10)
    | pipe.filter(is_valid)
    | pipe.map(normalize)
    | pipe.collect()
)
```

Both styles produce identical pipelines — choose whichever reads better for your use case.

## Next Steps

- [Pipeline Basics](guide/pipeline-basics.md) — all constructors, stages, and terminals
- [Concurrency & Workers](guide/concurrency.md) — tuning workers and buffer sizes
- [Error Handling](guide/error-handling.md) — Ok/Err result types
