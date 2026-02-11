# Result Types

::: anyiostream.result

Rust-inspired `Ok`/`Err` discriminated union for pipeline error handling.

## `Ok[T]`

Wraps a successful value.

```python
from anyiostream import Ok

ok = Ok(42)
```

### Methods

| Method | Returns | Description |
|--------|---------|-------------|
| `.value` | `T` | The wrapped value |
| `.is_ok()` | `True` | Always true for Ok |
| `.is_err()` | `False` | Always false for Ok |
| `.unwrap()` | `T` | Return the value |
| `.unwrap_or(default)` | `T` | Return the value (ignores default) |
| `.unwrap_err()` | raises `ValueError` | Ok has no error |
| `.map(fn)` | `Ok[U]` | Apply fn to value |
| `.map_err(fn)` | `Ok[T]` | No-op for Ok |

## `Err[E]`

Wraps an error value.

```python
from anyiostream import Err, PipelineError

err = Err(PipelineError(exception=ValueError("boom"), item=42, stage="parse"))
```

### Methods

| Method | Returns | Description |
|--------|---------|-------------|
| `.error` | `E` | The wrapped error |
| `.is_ok()` | `False` | Always false for Err |
| `.is_err()` | `True` | Always true for Err |
| `.unwrap()` | raises `ValueError` | Err has no value |
| `.unwrap_or(default)` | `default` | Return the default |
| `.unwrap_err()` | `E` | Return the error |
| `.map(fn)` | `Err[E]` | No-op for Err |
| `.map_err(fn)` | `Err[U]` | Apply fn to error |

## `PipelineError`

Context captured when a pipeline stage raises an exception.

```python
from anyiostream import PipelineError
```

### Fields

| Field | Type | Description |
|-------|------|-------------|
| `exception` | `Exception` | The caught exception |
| `item` | `Any` | The input item that caused the error |
| `stage` | `str \| None` | Stage name (from `name=` parameter) |
| `traceback` | `str \| None` | Formatted traceback at failure point |

### String Representation

```python
str(err.error)  # "[parse] ValueError: boom"
```

## `Result` Type Alias

```python
Result = Ok[T] | Err[E]
```

## Pattern Matching

```python
match item:
    case Ok(value=v):
        print(f"Success: {v}")
    case Err(error=e):
        print(f"Failed at {e.stage}: {e.exception}")
```

Both `Ok` and `Err` are frozen dataclasses with `slots=True`, supporting structural pattern matching in Python 3.12+.
