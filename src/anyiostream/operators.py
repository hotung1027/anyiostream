"""
Pipe operators — aiostream-style ``|`` composition.

Usage::

	from anyiostream import Stream, pipe

	result = await (
		Stream.from_iterable(range(10))
		| pipe.map(lambda x: x * 2, workers=3)
		| pipe.filter(lambda x: x > 5)
		| pipe.collect()
	)

Each ``pipe.*`` call returns a callable that accepts a ``Stream``
and returns a new ``Stream`` (or a coroutine for terminals).
"""

from __future__ import annotations

from collections.abc import AsyncIterable, Awaitable, Callable, Iterable
from typing import Any, TypeVar

from anyiostream.stream import (
	_COLLECT_SENTINEL,
	_COLLECT_SPLIT_SENTINEL,
	_COUNT_SENTINEL,
	Stream,
)

T = TypeVar("T")
U = TypeVar("U")


# ---------------------------------------------------------------------------
# Pipe namespace
# ---------------------------------------------------------------------------


class _Pipe:
	"""
	Namespace for pipe operators.

	All methods return a ``_PipeOp`` (a callable ``Stream -> Stream``)
	so they can be used with the ``|`` operator.
	"""

	__slots__ = ()

	@staticmethod
	def map(
		func: Callable[[T], U | Awaitable[U]],
		*,
		workers: int = 1,
		buffer_size: float = 0,
		max_buffer_bytes: int = 10_000_000,
		size_func: Callable[[Any], int] | None = None,
		name: str | None = None,
	) -> Callable[[Stream[T]], Stream[U]]:
		"""
		``| pipe.map(fn)`` — 1:1 transform.

		Args:
			func: Transform function.
			workers: Concurrent workers.
			buffer_size: Backpressure buffer (item count).
			max_buffer_bytes: Memory-based buffer limit in bytes.
				Defaults to 10MB (10_000_000 bytes).
			size_func: Optional function to calculate item size in bytes.
			name: Debug label.

		Returns:
			A pipe operator that can be used with ``|``.
		"""

		def _apply(stream: Stream[T]) -> Stream[U]:
			return stream.map(
				func,
				workers=workers,
				buffer_size=buffer_size,
				max_buffer_bytes=max_buffer_bytes,
				size_func=size_func,
				name=name,
			)

		return _apply

	@staticmethod
	def flat_map(
		func: Callable[[T], AsyncIterable[U] | Iterable[U] | Awaitable[Any]],
		*,
		workers: int = 1,
		buffer_size: float = 0,
		max_buffer_bytes: int = 10_000_000,
		size_func: Callable[[Any], int] | None = None,
		name: str | None = None,
	) -> Callable[[Stream[T]], Stream[U]]:
		"""
		``| pipe.flat_map(fn)`` — 1:N transform.

		Args:
			func: Function returning iterable or async iterable.
			workers: Concurrent workers.
			buffer_size: Backpressure buffer.
			max_buffer_bytes: Memory-based buffer limit in bytes.
				Defaults to 10MB (10_000_000 bytes).
			size_func: Optional function to calculate item size in bytes.
			name: Debug label.

		Returns:
			A pipe operator.
		"""

		def _apply(stream: Stream[T]) -> Stream[U]:
			return stream.flat_map(
				func,
				workers=workers,
				buffer_size=buffer_size,
				max_buffer_bytes=max_buffer_bytes,
				size_func=size_func,
				name=name,
			)

		return _apply

	@staticmethod
	def filter(
		predicate: Callable[[T], bool | Awaitable[bool]],
		*,
		workers: int = 1,
		buffer_size: float = 0,
		max_buffer_bytes: int = 10_000_000,
		size_func: Callable[[Any], int] | None = None,
		name: str | None = None,
	) -> Callable[[Stream[T]], Stream[T]]:
		"""
		``| pipe.filter(pred)`` — keep items where predicate is truthy.

		Args:
			predicate: Filter function.
			workers: Concurrent workers.
			buffer_size: Backpressure buffer.
			max_buffer_bytes: Memory-based buffer limit in bytes.
				Defaults to 10MB (10_000_000 bytes).
			size_func: Optional function to calculate item size in bytes.
			name: Debug label.

		Returns:
			A pipe operator.
		"""

		def _apply(stream: Stream[T]) -> Stream[T]:
			return stream.filter(
				predicate,
				workers=workers,
				buffer_size=buffer_size,
				max_buffer_bytes=max_buffer_bytes,
				size_func=size_func,
				name=name,
			)

		return _apply

	@staticmethod
	def foreach(
		func: Callable[[T], Any],
		*,
		workers: int = 1,
		buffer_size: float = 0,
		max_buffer_bytes: int = 10_000_000,
		size_func: Callable[[Any], int] | None = None,
		name: str | None = None,
	) -> Callable[[Stream[T]], Stream[T]]:
		"""
		``| pipe.foreach(fn)`` — side effect, passes items through unchanged.

		Args:
			func: Side-effect function.
			workers: Concurrent workers.
			buffer_size: Backpressure buffer.
			max_buffer_bytes: Memory-based buffer limit in bytes.
				Defaults to 10MB (10_000_000 bytes).
			size_func: Optional function to calculate item size in bytes.
			name: Debug label.

		Returns:
			A pipe operator.
		"""

		def _apply(stream: Stream[T]) -> Stream[T]:
			return stream.foreach(
				func,
				workers=workers,
				buffer_size=buffer_size,
				max_buffer_bytes=max_buffer_bytes,
				size_func=size_func,
				name=name,
			)

		return _apply

	@staticmethod
	def collect() -> Any:
		"""
		``| pipe.collect()`` — terminal: collect all items into a list.

		Returns:
			Sentinel that triggers ``Stream.__or__`` to call ``.collect()``.
		"""
		return _COLLECT_SENTINEL

	@staticmethod
	def count() -> Any:
		"""
		``| pipe.count()`` — terminal: consume all items, return count.

		Returns:
			Sentinel that triggers ``Stream.__or__`` to call ``.count()``.
		"""
		return _COUNT_SENTINEL

	# ------------------------------------------------------------------
	# Result-aware operators
	# ------------------------------------------------------------------

	@staticmethod
	def try_map(
		func: Callable[..., Any],
		*,
		err: Callable[..., Any] | None = None,
		workers: int = 1,
		buffer_size: float = 0,
		name: str | None = None,
	) -> Callable[[Stream[Any]], Stream[Any]]:
		"""``| pipe.try_map(fn, err=handler)`` — map with Ok/Err wrapping."""

		def _apply(stream: Stream[Any]) -> Stream[Any]:
			return stream.try_map(
				func,
				err=err,
				workers=workers,
				buffer_size=buffer_size,
				name=name,
			)

		return _apply

	@staticmethod
	def try_flat_map(
		func: Callable[..., Any],
		*,
		err: Callable[..., Any] | None = None,
		workers: int = 1,
		buffer_size: float = 0,
		name: str | None = None,
	) -> Callable[[Stream[Any]], Stream[Any]]:
		"""``| pipe.try_flat_map(fn, err=handler)`` — flat_map with Ok/Err wrapping."""

		def _apply(stream: Stream[Any]) -> Stream[Any]:
			return stream.try_flat_map(
				func,
				err=err,
				workers=workers,
				buffer_size=buffer_size,
				name=name,
			)

		return _apply

	@staticmethod
	def try_filter(
		predicate: Callable[..., Any],
		*,
		workers: int = 1,
		buffer_size: float = 0,
		name: str | None = None,
	) -> Callable[[Stream[Any]], Stream[Any]]:
		"""``| pipe.try_filter(pred)`` — filter Ok values, Err passes through."""

		def _apply(stream: Stream[Any]) -> Stream[Any]:
			return stream.try_filter(
				predicate,
				workers=workers,
				buffer_size=buffer_size,
				name=name,
			)

		return _apply

	@staticmethod
	def try_foreach(
		func: Callable[..., Any],
		*,
		err: Callable[..., Any] | None = None,
		workers: int = 1,
		buffer_size: float = 0,
		name: str | None = None,
	) -> Callable[[Stream[Any]], Stream[Any]]:
		"""``| pipe.try_foreach(fn, err=handler)`` — side effect on Ok values."""

		def _apply(stream: Stream[Any]) -> Stream[Any]:
			return stream.try_foreach(
				func,
				err=err,
				workers=workers,
				buffer_size=buffer_size,
				name=name,
			)

		return _apply

	@staticmethod
	def recover(
		func: Callable[..., Any],
		*,
		workers: int = 1,
		buffer_size: float = 0,
		name: str | None = None,
	) -> Callable[[Stream[Any]], Stream[Any]]:
		"""``| pipe.recover(fn)`` — convert Err to value, unwrap Ok."""

		def _apply(stream: Stream[Any]) -> Stream[Any]:
			return stream.recover(
				func,
				workers=workers,
				buffer_size=buffer_size,
				name=name,
			)

		return _apply

	@staticmethod
	def ok_only() -> Callable[[Stream[Any]], Stream[Any]]:
		"""``| pipe.ok_only()`` — keep and unwrap Ok, drop Err."""

		def _apply(stream: Stream[Any]) -> Stream[Any]:
			return stream.ok_only()

		return _apply

	@staticmethod
	def errors_only() -> Callable[[Stream[Any]], Stream[Any]]:
		"""``| pipe.errors_only()`` — keep and unwrap Err, drop Ok."""

		def _apply(stream: Stream[Any]) -> Stream[Any]:
			return stream.errors_only()

		return _apply

	@staticmethod
	def collect_split() -> Any:
		"""``| pipe.collect_split()`` — terminal: partition into (oks, errs)."""
		return _COLLECT_SPLIT_SENTINEL


# Module-level singleton
pipe = _Pipe()
