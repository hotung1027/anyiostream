# Comparison

## anyiostream vs aiostream vs raw anyio

| Feature | aiostream | anyio (raw) | **anyiostream** |
|---------|-----------|-------------|-----------------|
| Inter-stage concurrency | No — single-task generator pull | Manual (~30 LOC per pipeline) | **Yes** — task-per-stage in TaskGroup |
| Fan-out `workers=N` | No (`task_limit` within one stage) | Manual (clone streams yourself) | **Yes** — load-balanced via `clone()` |
| Backpressure | No (pull-based) | Yes — manual wiring | **Yes** — `buffer_size` per stage |
| Result `Ok`/`Err` types | No | No | **Yes** — railway-oriented error handling |
| Backend | asyncio only | asyncio + trio | **asyncio + trio** |
| Pipe `|` syntax | Yes | No | **Yes** |
| Structured concurrency | Partial | Yes | **Yes** — automatic cleanup |
| Operator count | 30+ (7 categories) | 0 (primitives only) | ~10 (focused on pipelines) |

## Concurrency Model: The Core Difference

### aiostream — Generator Pull Chain

aiostream composes pipelines via **nested async generators**. Each `|` wraps the previous generator:

```
consumer ← filter.__anext__() ← map.__anext__() ← source.__anext__()
```

All stages run in a **single task**. Stage 2 cannot run while Stage 1 is producing — they execute sequentially in one call stack. aiostream's `task_limit=N` runs N coroutines concurrently **within one stage**, not across stages.

### anyiostream — CSP Channel Graph

anyiostream builds a **task graph** with bounded channels:

```
source (task) → [channel] → stage1 (task) → [channel] → stage2 (task) → [channel] → terminal
```

Each stage runs as an **independent task** in a `TaskGroup`. Items flow through stage 2 while stage 1 is still producing. This is the CSP (Communicating Sequential Processes) pattern — the same model as Go channels.

## What Raw anyio Requires

To build a 2-stage concurrent pipeline with fan-out workers using raw anyio:

```python
async def manual_pipeline(urls):
    s0, r0 = anyio.create_memory_object_stream(10)
    s1, r1 = anyio.create_memory_object_stream(10)
    s2, r2 = anyio.create_memory_object_stream(10)

    async def source(send):
        async with send:
            for url in urls:
                await send.send(url)

    async def fetch_worker(recv, send):
        async with recv, send:
            async for url in recv:
                await send.send(await fetch(url))

    async def parse_worker(recv, send):
        async with recv, send:
            async for page in recv:
                await send.send(parse(page))

    async with anyio.create_task_group() as tg:
        tg.start_soon(source, s0)
        for _ in range(3):
            tg.start_soon(fetch_worker, r0.clone(), s1.clone())
        r0.close(); s1.close()
        tg.start_soon(parse_worker, r1, s2)

        results = []
        async for item in r2:
            results.append(item)
    return results
```

The equivalent in anyiostream:

```python
result = await (
    Stream.from_iterable(urls)
    .map(fetch, workers=3, buffer_size=10)
    .map(parse)
    .collect()
)
```

## When to Use What

| Use Case | Recommendation |
|----------|---------------|
| Rich operator set (merge, zip, chain, timing) | **aiostream** — 30+ operators |
| Simple single-task async iteration | **aiostream** — lightweight, mature |
| I/O-bound pipelines with concurrent stages | **anyiostream** — true inter-stage concurrency |
| Fan-out workers for parallel processing | **anyiostream** — `workers=N` per stage |
| Error handling without crashing | **anyiostream** — Ok/Err result types |
| trio backend support | **anyiostream** — via anyio |
| Full control over task/channel wiring | **raw anyio** — maximum flexibility |

## Honest Gaps

anyiostream is younger and more focused than aiostream:

- **Fewer operators** — no merge, zip, chain, accumulate, timing operators (yet)
- **No order preservation** — `workers > 1` is inherently unordered
- **New library** — less battle-tested than aiostream (10 years, 900+ stars)

anyiostream focuses on doing one thing well: **concurrent multi-stage pipelines with backpressure and error handling**.
