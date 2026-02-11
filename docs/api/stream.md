# Stream

::: anyiostream.stream.Stream

The `Stream` class is the core pipeline builder. It holds a lazy recipe of `Process` stages that execute only when a terminal operation is called.

## Constructors

### `Stream.from_iterable(source)`

Create a stream from a sync or async iterable.

```python
# Sync
stream = Stream.from_iterable([1, 2, 3])

# Async generator
async def gen():
    for i in range(10):
        yield i
stream = Stream.from_iterable(gen())
```

**Parameters:**

| Parameter | Type | Description |
|-----------|------|-------------|
| `source` | `Iterable[T] \| AsyncIterable[T]` | Items to stream |

### `Stream.from_callable(factory)`

Lazy construction — factory is called at execution time.

```python
stream = Stream.from_callable(lambda: range(10))
stream = Stream.from_callable(async_generator_function)
```

**Parameters:**

| Parameter | Type | Description |
|-----------|------|-------------|
| `factory` | `Callable[[], Iterable[T] \| AsyncIterable[T]]` | Called when pipeline materializes |

## Transform Stages

All stages return a new `Stream` (lazy — nothing executes yet).

### `.map(fn, *, workers=1, buffer_size=0, name=None)`

1:1 transform. Accepts sync or async functions.

### `.flat_map(fn, *, workers=1, buffer_size=0, name=None)`

1:N transform. Function returns an iterable or async iterable.

### `.filter(pred, *, workers=1, buffer_size=0, name=None)`

Keep items where predicate is truthy.

### `.foreach(fn, *, workers=1, buffer_size=0, name=None)`

Side-effect — calls `fn(item)`, passes item through unchanged.

**Common Parameters:**

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `fn` / `pred` | `Callable` | — | Transform or predicate function |
| `workers` | `int` | `1` | Number of concurrent workers |
| `buffer_size` | `float` | `0` | Channel buffer (0=rendezvous, `math.inf`=unbounded) |
| `name` | `str \| None` | `None` | Debug label |

## Result-Aware Stages

See [Result Types](result.md) for `Ok`/`Err` details.

### `.try_map(fn, *, err=None, workers=1, buffer_size=0, name=None)`

Map with Ok/Err wrapping. Exceptions become `Err(PipelineError(...))`.

### `.try_flat_map(fn, *, err=None, workers=1, buffer_size=0, name=None)`

Flat map with Ok/Err wrapping.

### `.try_filter(pred, *, workers=1, buffer_size=0, name=None)`

Filter `Ok` values. `Err` always passes through.

### `.try_foreach(fn, *, err=None, workers=1, buffer_size=0, name=None)`

Side-effect on `Ok` values.

### `.recover(fn, *, workers=1, buffer_size=0, name=None)`

Convert `Err` → value via `fn(error)`, unwrap `Ok`. Exits Result mode.

### `.ok_only()`

Keep and unwrap `Ok` values, drop `Err`.

### `.errors_only()`

Keep `Err` values (unwrapped to error), drop `Ok`.

## Terminal Operations

Terminals materialize the pipeline and return a result.

### `await stream.collect() → list[T]`

Collect all items into a list.

### `await stream.count() → int`

Consume all items, return the count.

### `await stream.reduce(fn, initial) → U`

Fold items: `fn(accumulator, item) → accumulator`.

### `await stream.first() → T | None`

Return the first item, or `None` if empty.

### `await stream.take(n) → list[T]`

Collect at most `n` items.

### `async with stream.open() as recv`

Context manager for manual iteration.

### `await stream.collect_split() → tuple[list, list]`

Partition into `(ok_values, errors)`.

## Pipe Operator

```python
stream | pipe.map(fn) | pipe.collect()
```

The `__or__` method supports pipe composition. See [Pipe Operators](pipe.md).
