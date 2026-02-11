# Concurrency & Workers

## Execution Model

anyiostream uses the **CSP (Communicating Sequential Processes)** pattern:

```
Source → [channel] → Stage 1 → [channel] → Stage 2 → [channel] → Terminal
           ↑ bounded      workers=N            workers=M
           backpressure    (fan-out)            (fan-out)
```

Each stage runs as an **independent task** inside a single `TaskGroup`. Items flow between stages via bounded `MemoryObjectStream` channels. This means:

- Stage 2 processes items **while Stage 1 is still producing**
- Multiple workers per stage process items in parallel
- Bounded channels provide automatic backpressure

## Fan-Out Workers

Set `workers=N` to run N concurrent workers for a stage:

```python
result = await (
    Stream.from_iterable(urls)
    .map(fetch, workers=10)   # 10 concurrent HTTP fetchers
    .map(parse, workers=4)    # 4 concurrent parsers
    .collect()
)
```

Workers are load-balanced via anyio's `MemoryObjectReceiveStream.clone()` — the first worker to call `receive()` gets the next item.

!!! note "Order is not preserved"
    With `workers > 1`, output order depends on which worker finishes first. If you need ordered results, use `workers=1` or sort after collecting.

## Backpressure & Buffer Sizing

The `buffer_size` parameter controls the bounded channel between stages:

| Value | Behavior |
|-------|----------|
| `0` (default) | **Rendezvous** — sender blocks until receiver is ready. Strongest backpressure. |
| `N` | **Bounded buffer** — sender blocks when N items are buffered. |
| `math.inf` | **Unbounded** — no backpressure. Use with caution. |

```python
import math

result = await (
    Stream.from_iterable(data)
    .map(fast_transform, buffer_size=100)    # allow 100-item buffer
    .map(slow_io, workers=5, buffer_size=0)  # rendezvous — no pile-up
    .collect()
)
```

### Tuning Guidelines

- **CPU-bound stages**: small buffer (0–10), match workers to CPU cores
- **I/O-bound stages**: larger buffer (50–200), more workers (10–50)
- **Memory-sensitive**: use `buffer_size=0` (rendezvous) to minimize in-flight items
- **Throughput-sensitive**: increase buffer to decouple fast producers from slow consumers

## Structured Concurrency

All tasks run inside a single `anyio.create_task_group()`:

- If any stage raises an unhandled exception, all tasks are cancelled
- When the terminal operation completes, all tasks are cleaned up
- No leaked tasks, no dangling coroutines

```python
# Automatic cleanup — even on error
async with stream.open() as items:
    async for item in items:
        if should_stop(item):
            break  # TaskGroup cancels all stages
```

## Both Backends

anyiostream works on **asyncio** and **trio** via anyio:

```python
import anyio

async def main():
    result = await Stream.from_iterable(range(10)).map(fn).collect()
    print(result)

# asyncio (default)
anyio.run(main)

# trio
anyio.run(main, backend="trio")
```

Tests run on both backends automatically via `pytest-anyio`.
