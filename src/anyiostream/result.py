"""
Result type for pipeline error handling — Rust-inspired Ok/Err.

Provides a discriminated union for representing success (``Ok``) or
failure (``Err``) as values that flow through pipeline channels,
instead of silently dropping errors.

Works with Python 3.12+ ``match/case``::

    match item:
        case Ok(value=v):
            print(f"Success: {v}")
        case Err(error=e):
            print(f"Failed at {e.stage}: {e.exception}")
"""

from __future__ import annotations

import traceback as _tb
from collections.abc import AsyncIterable, Awaitable, Callable
from dataclasses import dataclass
from typing import Any, Generic, NoReturn, TypeAlias, TypeVar

T = TypeVar("T")
E = TypeVar("E")


# ---------------------------------------------------------------------------
# Error context
# ---------------------------------------------------------------------------


@dataclass(frozen=True, slots=True)
class PipelineError:
	"""Context captured when a pipeline stage raises an exception.

	Attributes:
	    exception: The caught exception instance.
	    item: The original input item that caused the error.
	    stage: Human-readable label of the stage that failed.
	    traceback: Formatted traceback string at the point of failure.
	"""

	exception: Exception
	item: Any = None
	stage: str | None = None
	traceback: str | None = None

	def __str__(self) -> str:
		prefix = f"[{self.stage}] " if self.stage else ""
		return f"{prefix}{self.exception}"


# ---------------------------------------------------------------------------
# Result variants
# ---------------------------------------------------------------------------


@dataclass(frozen=True, slots=True)
class Ok(Generic[T]):
	"""Successful result wrapping a value."""

	value: T

	def is_ok(self) -> bool:
		return True

	def is_err(self) -> bool:
		return False

	def unwrap(self) -> T:
		"""Return the contained value."""
		return self.value

	def unwrap_or(self, default: T) -> T:  # noqa: ARG002
		"""Return the contained value (ignores *default*)."""
		return self.value

	def unwrap_err(self) -> NoReturn:
		"""Raises — an ``Ok`` has no error."""
		raise ValueError(f"Called unwrap_err on Ok({self.value!r})")

	def map(self, func: Any) -> Ok[Any]:
		"""Apply *func* to the contained value."""
		return Ok(func(self.value))

	def map_err(self, func: Any) -> Ok[T]:  # noqa: ARG002
		"""No-op — ``Ok`` has no error to transform."""
		return self


@dataclass(frozen=True, slots=True)
class Err(Generic[E]):
	"""Error result wrapping an error."""

	error: E

	def is_ok(self) -> bool:
		return False

	def is_err(self) -> bool:
		return True

	def unwrap(self) -> NoReturn:
		"""Raises — an ``Err`` has no value."""
		raise ValueError(f"Called unwrap on Err({self.error!r})")

	def unwrap_or(self, default: Any) -> Any:
		"""Return *default* since ``Err`` has no value."""
		return default

	def unwrap_err(self) -> E:
		"""Return the contained error."""
		return self.error

	def map(self, func: Any) -> Err[E]:  # noqa: ARG002
		"""No-op — ``Err`` has no value to transform."""
		return self

	def map_err(self, func: Any) -> Err[Any]:
		"""Apply *func* to the contained error."""
		return Err(func(self.error))


Result: TypeAlias = Ok[T] | Err[E]


# ---------------------------------------------------------------------------
# Result wrapper factories (used by process.py mixin)
# ---------------------------------------------------------------------------


def _try_map_wrap(
	func: Callable[..., Any],
	label: str,
	err: Callable[..., Any] | None = None,
) -> Callable[..., Any]:
	"""Universal MAP wrapper for Result-aware processing.

	- Raw value: apply *func*, wrap as ``Ok`` (exception → ``Err``).
	- ``Ok(value)``: unwrap, apply *func*, wrap as ``Ok``.
	- ``Err(error)``: if *err* provided, apply to error → ``Err(result)``.
	  Otherwise pass through unchanged.
	"""

	async def _wrapped(item: Any) -> Ok[Any] | Err[Any]:
		if isinstance(item, Err):
			if err is not None:
				result = err(item.error)
				if isinstance(result, Awaitable):
					result = await result
				return Err(result)
			return item
		value = item.value if isinstance(item, Ok) else item
		try:
			result = func(value)
			if isinstance(result, Awaitable):
				result = await result
			return Ok(result)
		except Exception as exc:
			return Err(
				PipelineError(
					exception=exc,
					item=value,
					stage=label,
					traceback=_tb.format_exc(),
				)
			)

	return _wrapped


def _try_flat_map_wrap(
	func: Callable[..., Any],
	label: str,
	err: Callable[..., Any] | None = None,
) -> Callable[..., Any]:
	"""Universal FLAT_MAP wrapper for Result-aware processing.

	Each sub-item → ``Ok``, exception → single ``Err``.
	``Err`` items use the *err* handler or pass through as ``[Err(...)]``.
	"""

	async def _wrapped(item: Any) -> list[Ok[Any] | Err[Any]]:
		if isinstance(item, Err):
			if err is not None:
				result = err(item.error)
				if isinstance(result, Awaitable):
					result = await result
				return [Err(result)]
			return [item]
		value = item.value if isinstance(item, Ok) else item
		try:
			result = func(value)
			if isinstance(result, AsyncIterable):
				return [Ok(sub) async for sub in result]
			if isinstance(result, Awaitable):
				resolved = await result
				if isinstance(resolved, AsyncIterable):
					return [Ok(sub) async for sub in resolved]
				return [Ok(sub) for sub in resolved]
			return [Ok(sub) for sub in result]
		except Exception as exc:
			return [
				Err(
					PipelineError(
						exception=exc,
						item=value,
						stage=label,
						traceback=_tb.format_exc(),
					)
				)
			]

	return _wrapped
