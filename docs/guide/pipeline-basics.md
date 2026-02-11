# Pipeline Basics

## Stream Constructors

### `Stream.from_iterable(items)`

Create a stream from any sync or async iterable:

```python
# Sync iterable
stream = Stream.from_iterable([1, 2, 3])
stream = Stream.from_iterable(range(100))

# Async generator
async def generate():
    for i in range(10):
        yield i

stream = Stream.from_iterable(generate())
```

### `Stream.from_callable(factory)`

Lazy construction — the factory is called each time the pipeline executes:

```python
stream = Stream.from_callable(lambda: range(10))

# Useful for async generators (pass the function, not the result)
stream = Stream.from_callable(generate)  # not generate()
```

## Transform Stages

All stages are lazy — they return a new `Stream` without executing anything.

### `.map(fn, *, workers=1, buffer_size=0, name=None)`

1:1 transform. Works with sync or async functions:

```python
# Sync
stream.map(lambda x: x * 2)

# Async
stream.map(async_fetch, workers=5)
```

### `.flat_map(fn, *, workers=1, buffer_size=0, name=None)`

1:N transform. The function returns an iterable (sync or async):

```python
# Each URL → multiple links
stream.flat_map(extract_links, workers=5)

# Sync expansion
stream.flat_map(lambda x: [x, x + 1])
```

### `.filter(pred, *, workers=1, buffer_size=0, name=None)`

Keep items where the predicate is truthy:

```python
stream.filter(lambda x: x > 0)
stream.filter(is_valid, workers=4)
```

### `.foreach(fn, *, workers=1, buffer_size=0, name=None)`

Side-effect only — items pass through unchanged:

```python
stream.foreach(print)
stream.foreach(log_item, workers=2)
```

## Terminal Operations

Terminals materialize the pipeline and return a result.

### `.collect() → list[T]`

Collect all items into a list:

```python
items = await stream.collect()
```

### `.count() → int`

Consume all items, return the count:

```python
n = await stream.count()
```

### `.reduce(fn, initial) → U`

Fold all items into a single value:

```python
total = await stream.reduce(lambda acc, x: acc + x, 0)
```

### `.first() → T | None`

Return the first item, or `None` if empty:

```python
item = await stream.first()
```

### `.take(n) → list[T]`

Collect at most `n` items:

```python
top5 = await stream.take(5)
```

### `.open() → AsyncContextManager`

Manual iteration with structured concurrency:

```python
async with stream.open() as items:
    async for item in items:
        await process(item)
```

### `.collect_split() → tuple[list, list]`

Partition `Ok`/`Err` results into `(successes, failures)`:

```python
oks, errs = await stream.try_map(risky_fn).collect_split()
```

## Stage Options

Every stage accepts these keyword arguments:

| Option | Default | Description |
|--------|---------|-------------|
| `workers` | `1` | Number of concurrent workers for this stage |
| `buffer_size` | `0` | Channel buffer size (0 = rendezvous) |
| `name` | `None` | Label for debugging |

See [Concurrency & Workers](concurrency.md) for tuning guidance.
