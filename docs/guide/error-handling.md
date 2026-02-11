# Error Handling

anyiostream provides Rust-inspired `Ok`/`Err` types for **railway-oriented error handling** — errors flow as values through the pipeline instead of crashing silently.

## The Problem

With plain `.map()`, exceptions in a stage skip the item and log a warning:

```python
result = await Stream.from_iterable([1, 2, 3]).map(risky_fn).collect()
# If risky_fn(2) throws, item 2 is silently dropped
```

## Result Types

Use `try_*` stages to wrap results as `Ok(value)` or `Err(PipelineError(...))`:

```python
from anyiostream import Stream, Ok, Err, PipelineError
```

### `Ok[T]`

Wraps a successful value:

```python
ok = Ok(42)
ok.value        # 42
ok.is_ok()      # True
ok.unwrap()     # 42
ok.unwrap_or(0) # 42
```

### `Err[E]`

Wraps an error (typically `PipelineError`):

```python
err = Err(PipelineError(exception=ValueError("boom"), item=2, stage="parse"))
err.error           # PipelineError(...)
err.is_err()        # True
err.unwrap_or(0)    # 0
err.error.exception # ValueError("boom")
err.error.item      # 2
err.error.stage     # "parse"
```

### Pattern Matching (Python 3.12+)

```python
match result:
    case Ok(value=v):
        print(f"Success: {v}")
    case Err(error=e):
        print(f"Failed at {e.stage}: {e.exception}")
```

## Result-Aware Stages

### `try_map(fn, *, err=None)`

Map with automatic Ok/Err wrapping:

```python
results = await (
    Stream.from_iterable(urls)
    | pipe.try_map(fetch, workers=5)  # Ok(response) or Err(PipelineError)
    | pipe.try_map(parse)             # chains on Ok, passes Err through
    | pipe.collect()
)
# results: [Ok(data1), Err(PipelineError(...)), Ok(data3), ...]
```

### `try_flat_map(fn, *, err=None)`

Flat map with Ok/Err wrapping — each sub-item becomes `Ok`:

```python
results = await (
    Stream.from_iterable(pages)
    | pipe.try_flat_map(extract_links, workers=3)
    | pipe.collect()
)
```

### `try_filter(pred)`

Filter `Ok` values; `Err` always passes through:

```python
results = await (
    Stream.from_iterable(items)
    | pipe.try_map(validate)
    | pipe.try_filter(lambda x: x.score > 0.5)  # only filters Ok values
    | pipe.collect()
)
```

### `try_foreach(fn, *, err=None)`

Side-effect on `Ok` values; items pass through unchanged:

```python
results = await (
    Stream.from_iterable(items)
    | pipe.try_map(process)
    | pipe.try_foreach(log_success, err=log_failure)
    | pipe.collect()
)
```

## The `err=handler` Option

`try_map`, `try_flat_map`, and `try_foreach` accept an optional `err` parameter to transform `Err` items:

```python
# Without err= : Err passes through unchanged
| pipe.try_map(parse)

# With err= : transform the error
| pipe.try_map(parse, err=lambda e: log_and_rewrap(e))
```

The handler receives the `PipelineError` and returns a new error value (wrapped back into `Err`).

## Exit Ramps

### `recover(fn)`

Convert `Err` → value, unwrap `Ok`. After this stage, items are plain values:

```python
results = await (
    Stream.from_iterable(urls)
    | pipe.try_map(fetch, workers=5)
    | pipe.recover(lambda err: f"FAILED: {err.item}")
    | pipe.collect()
)
# results: ["<html>...", "FAILED: http://bad.url", "<html>...", ...]
```

### `ok_only()`

Keep `Ok` values (unwrapped), drop all `Err`:

```python
successes = await (
    Stream.from_iterable(urls)
    | pipe.try_map(fetch)
    | pipe.ok_only()
    | pipe.collect()
)
```

### `errors_only()`

Keep `Err` values (unwrapped to `PipelineError`), drop all `Ok`:

```python
failures = await (
    Stream.from_iterable(urls)
    | pipe.try_map(fetch)
    | pipe.errors_only()
    | pipe.collect()
)
for err in failures:
    print(f"{err.stage}: {err.exception} (item={err.item})")
```

### `collect_split()`

Partition into `(successes, failures)` in one pass:

```python
oks, errs = await (
    Stream.from_iterable(urls)
    | pipe.try_map(fetch, workers=5)
    | pipe.try_map(parse)
    | pipe.collect_split()
)
print(f"{len(oks)} succeeded, {len(errs)} failed")
```

## Full Example

```python
from anyiostream import Stream, pipe

async def process_urls(urls: list[str]) -> None:
    oks, errs = await (
        Stream.from_iterable(urls)
        | pipe.try_map(fetch, workers=10)
        | pipe.try_map(parse, err=lambda e: e)  # pass errors through
        | pipe.try_filter(lambda doc: doc.is_relevant)
        | pipe.try_foreach(save_to_db, workers=3)
        | pipe.collect_split()
    )

    print(f"Processed {len(oks)} documents")
    for err in errs:
        print(f"  Failed: {err.stage} - {err.exception}")
```
