# Pipe Operators

```{eval-rst}
.. autoclass:: anyiostream.operators._Pipe
   :members:
   :undoc-members:
```

The `pipe` singleton provides static methods that return callables for use with the `|` operator.

```python
from anyiostream import pipe

result = await (
    Stream.from_iterable(items)
    | pipe.map(fn, workers=4)
    | pipe.filter(pred)
    | pipe.collect()
)
```

## Transform Operators

### `pipe.map(fn, *, workers=1, buffer_size=0, name=None)`

Returns a callable that applies `.map()` to the stream.

### `pipe.flat_map(fn, *, workers=1, buffer_size=0, name=None)`

Returns a callable that applies `.flat_map()` to the stream.

### `pipe.filter(pred, *, workers=1, buffer_size=0, name=None)`

Returns a callable that applies `.filter()` to the stream.

### `pipe.foreach(fn, *, workers=1, buffer_size=0, name=None)`

Returns a callable that applies `.foreach()` to the stream.

## Terminal Operators

### `pipe.collect()`

Returns a sentinel that triggers `.collect()` via `__or__`.

### `pipe.count()`

Returns a sentinel that triggers `.count()` via `__or__`.

### `pipe.collect_split()`

Returns a sentinel that triggers `.collect_split()` via `__or__`.

## Result-Aware Operators

### `pipe.try_map(fn, *, err=None, workers=1, buffer_size=0, name=None)`

Map with Ok/Err wrapping.

### `pipe.try_flat_map(fn, *, err=None, workers=1, buffer_size=0, name=None)`

Flat map with Ok/Err wrapping.

### `pipe.try_filter(pred, *, workers=1, buffer_size=0, name=None)`

Filter Ok values; Err passes through.

### `pipe.try_foreach(fn, *, err=None, workers=1, buffer_size=0, name=None)`

Side-effect on Ok values.

### `pipe.recover(fn, *, workers=1, buffer_size=0, name=None)`

Convert Err → value, unwrap Ok.

### `pipe.ok_only()`

Keep and unwrap Ok, drop Err.

### `pipe.errors_only()`

Keep Err (unwrapped), drop Ok.
