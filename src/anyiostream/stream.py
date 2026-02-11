"""
Stream — the composable pipeline builder.

``Stream`` is the central object.  It holds a lazy *recipe* of processes
that are only materialized (channels + tasks spawned) when a terminal
operation is called (``collect``, ``count``, ``reduce``, or via the
``open()`` context manager).

Two composition styles are supported:

1. **Method chaining** (builder pattern)::

       result = await (
           Stream.from_iterable(items)
           .map(process, workers=4)
           .filter(is_ok)
           .collect()
       )

2. **Pipe operator** (``|``)::

       from anyiostream import pipe

       result = await (
           Stream.from_iterable(items)
           | pipe.map(process, workers=4)
           | pipe.filter(is_ok)
           | pipe.collect()
       )

Design: The pipeline is executed inside a single ``async with
create_task_group()`` block so that structured concurrency is
preserved — every spawned task is guaranteed to complete (or be
cancelled) before the block exits.
"""

from __future__ import annotations

import math
from collections.abc import AsyncIterable, AsyncIterator, Awaitable, Callable, Iterable
from contextlib import asynccontextmanager
from typing import (
	Any,
	TypeVar,
)

import anyio
from anyio.streams.memory import (
	MemoryObjectReceiveStream,
	MemoryObjectSendStream,
)

from anyiostream.process import Process, ProcessConfig, ProcessKind, ResultStages

T = TypeVar("T")
U = TypeVar("U")

# Sentinel for pipe.collect() / pipe.count() — tells __or__ to materialize
_COLLECT_SENTINEL = object()
_COUNT_SENTINEL = object()
_COLLECT_SPLIT_SENTINEL = object()

# ---------------------------------------------------------------------------
# Stream
# ---------------------------------------------------------------------------


class Stream[T](ResultStages):
	"""
	A lazy, composable async pipeline.

	Holds a list of ``Process`` descriptors and a source factory.  Nothing
	runs until a terminal operation (``collect``, ``count``, or ``open()``)
	is invoked.
	"""

	__slots__ = ("_source_factory", "_processes")

	def __init__(
		self,
		source_factory: Callable[[MemoryObjectSendStream[Any]], Awaitable[None]],
		processes: list[Process[Any, Any]] | None = None,
	) -> None:
		self._source_factory = source_factory
		self._processes: list[Process[Any, Any]] = processes or []

	# ------------------------------------------------------------------
	# Constructors
	# ------------------------------------------------------------------

	@classmethod
	def from_iterable(
		cls,
		items: Iterable[T] | AsyncIterable[T],
		*,
		buffer_size: float = 0,
	) -> Stream[T]:
		"""
		Create a stream from a sync or async iterable.

		Args:
		    items: Source data.
		    buffer_size: Buffer between source and first process (not used directly
		        here — the first process's own ``buffer_size`` controls it).
		"""

		async def _produce(send: MemoryObjectSendStream[T]) -> None:
			async with send:
				if isinstance(items, AsyncIterable):
					async for item in items:
						await send.send(item)
				else:
					for item in items:
						await send.send(item)

		return cls(_produce)

	@classmethod
	def from_callable(
		cls,
		factory: Callable[[], Iterable[T] | AsyncIterable[T]],
		*,
		buffer_size: float = 0,
	) -> Stream[T]:
		"""
		Lazily create a stream: ``factory`` is called at execution time.

		Useful when the source is expensive to create or can only be
		iterated once.
		"""

		async def _produce(send: MemoryObjectSendStream[T]) -> None:
			async with send:
				source = factory()
				if isinstance(source, AsyncIterable):
					async for item in source:
						await send.send(item)
				else:
					for item in source:
						await send.send(item)

		return cls(_produce)

	# ------------------------------------------------------------------
	# Process builders (method-chaining API)
	# ------------------------------------------------------------------

	def map(
		self,
		func: Callable[[T], U | Awaitable[U]],
		*,
		workers: int = 1,
		buffer_size: float = 0,
		name: str | None = None,
	) -> Stream[U]:
		"""
		1:1 transformation.  ``func`` may be sync or async.

		Args:
		    func: Transform function ``T -> U``.
		    workers: Concurrent workers for this process.
		    buffer_size: Backpressure buffer to downstream.
		    name: Label for tracing.
		"""
		process: Process[T, U] = Process(
			kind=ProcessKind.MAP,
			func=func,
			config=ProcessConfig(workers=workers, buffer_size=buffer_size, name=name),
		)
		return Stream(self._source_factory, [*self._processes, process])

	def flat_map(
		self,
		func: Callable[[T], AsyncIterable[U] | Iterable[U] | Awaitable[Any]],
		*,
		workers: int = 1,
		buffer_size: float = 0,
		name: str | None = None,
	) -> Stream[U]:
		"""
		1:N transformation.  ``func`` returns an iterable or async iterable.

		Args:
		    func: Transform function ``T -> Iterable[U]`` or ``T -> AsyncIterable[U]``.
		    workers: Concurrent workers for this process.
		    buffer_size: Backpressure buffer to downstream.
		    name: Label for tracing.
		"""
		process: Process[T, U] = Process(
			kind=ProcessKind.FLAT_MAP,
			func=func,
			config=ProcessConfig(workers=workers, buffer_size=buffer_size, name=name),
		)
		return Stream(self._source_factory, [*self._processes, process])

	def filter(
		self,
		predicate: Callable[[T], bool | Awaitable[bool]],
		*,
		workers: int = 1,
		buffer_size: float = 0,
		name: str | None = None,
	) -> Stream[T]:
		"""
		Keep only items where ``predicate`` returns truthy.

		Args:
		    predicate: Filter function ``T -> bool``.
		    workers: Concurrent workers.
		    buffer_size: Backpressure buffer to downstream.
		    name: Label for tracing.
		"""
		process: Process[T, T] = Process(
			kind=ProcessKind.FILTER,
			func=predicate,
			config=ProcessConfig(workers=workers, buffer_size=buffer_size, name=name),
		)
		return Stream(self._source_factory, [*self._processes, process])

	def foreach(
		self,
		func: Callable[[T], Any],
		*,
		workers: int = 1,
		buffer_size: float = 0,
		name: str | None = None,
	) -> Stream[T]:
		"""
		Side-effect process: call ``func`` on each item without changing it.

		Useful for logging, metrics, or caching.

		Args:
		    func: Side-effect function ``T -> None``.
		    workers: Concurrent workers.
		    buffer_size: Backpressure buffer to downstream.
		    name: Label for tracing.
		"""
		process: Process[T, T] = Process(
			kind=ProcessKind.FOREACH,
			func=func,
			config=ProcessConfig(workers=workers, buffer_size=buffer_size, name=name),
		)
		return Stream(self._source_factory, [*self._processes, process])

	# ------------------------------------------------------------------
	# Execution engine (structured concurrency)
	# ------------------------------------------------------------------

	@asynccontextmanager
	async def _execute(self) -> AsyncIterator[MemoryObjectReceiveStream[T]]:
		"""
		Materialize the pipeline and yield the output receive stream.

		Runs inside ``async with create_task_group()`` so every spawned
		task either completes or is cancelled when the block exits.

		Usage::

		    async with stream._execute() as recv:
		        async for item in recv:
		            ...
		"""
		processes = self._processes

		if not processes:
			# No processes — connect source directly to output
			send, recv = anyio.create_memory_object_stream[Any](math.inf)
			async with anyio.create_task_group() as tg:
				tg.start_soon(self._source_factory, send)
				try:
					yield recv
				finally:
					recv.close()
					tg.cancel_scope.cancel()
			return

		# Build channel chain:
		#   source → [ch0] → process0 → [ch1] → process1 → ... → [chN] → output
		#
		# ch0       : between source and first process
		# ch1..chN  : between process[i-1] and process[i], then final output

		channels: list[
			tuple[MemoryObjectSendStream[Any], MemoryObjectReceiveStream[Any]]
		] = []

		# Source → first process channel (unbounded so source never blocks on process)
		channels.append(anyio.create_memory_object_stream[Any](math.inf))

		# Inter-process + final output channels
		for process in processes:
			channels.append(
				anyio.create_memory_object_stream[Any](process.config.buffer_size)
			)

		output_recv = channels[-1][1]

		async with anyio.create_task_group() as tg:
			# 1. Source producer
			tg.start_soon(self._source_factory, channels[0][0])

			# 2. Each process: reads from channels[i] → writes to channels[i+1]
			for i, process in enumerate(processes):
				tg.start_soon(process.run, channels[i][1], channels[i + 1][0])

			# 3. Yield the final output stream to the caller
			try:
				yield output_recv
			finally:
				# On exit (normal or early), close the output stream and
				# cancel in-flight tasks so the task group can exit cleanly.
				output_recv.close()
				tg.cancel_scope.cancel()

	# ------------------------------------------------------------------
	# Terminal operations
	# ------------------------------------------------------------------

	async def collect(self) -> list[T]:
		"""
		Execute the pipeline and collect all outputs into a list.

		Returns:
		    All items produced by the final process.
		"""
		results: list[T] = []
		async with self._execute() as recv:
			async for item in recv:
				results.append(item)
		return results

	async def count(self) -> int:
		"""
		Execute the pipeline, discarding outputs.

		Returns:
		    Number of items processed.
		"""
		count = 0
		async with self._execute() as recv:
			async for _item in recv:
				count += 1
		return count

	async def reduce(
		self,
		func: Callable[[U, T], U | Awaitable[U]],
		initial: U,
	) -> U:
		"""
		Fold all items into a single value.

		Args:
		    func: Reducer ``(acc, item) -> acc``.
		    initial: Starting accumulator value.

		Returns:
		    Final accumulated value.
		"""
		acc = initial
		async with self._execute() as recv:
			async for item in recv:
				result = func(acc, item)
				if isinstance(result, Awaitable):
					acc = await result
				else:
					acc = result
		return acc

	async def first(self) -> T | None:
		"""Return the first item, or ``None`` if the stream is empty."""
		async with self._execute() as recv:
			async for item in recv:
				return item
		return None

	async def take(self, n: int) -> list[T]:
		"""Collect at most *n* items."""
		results: list[T] = []
		async with self._execute() as recv:
			async for item in recv:
				results.append(item)
				if len(results) >= n:
					break
		return results

	@asynccontextmanager
	async def open(self) -> AsyncIterator[MemoryObjectReceiveStream[T]]:
		"""
		Context-manager API for manual iteration with structured concurrency.

		Usage::

		    async with stream.open() as items:
		        async for item in items:
		            process(item)
		"""
		async with self._execute() as recv:
			yield recv

	# ------------------------------------------------------------------
	# Pipe operator
	# ------------------------------------------------------------------

	def __or__(self, other: Any) -> Any:
		"""
		Support ``stream | pipe.map(fn)`` syntax.

		``other`` is either:
		- A callable (``_PipeOp``) that returns a new ``Stream``
		- ``_COLLECT_SENTINEL`` from ``pipe.collect()``
		- ``_COUNT_SENTINEL`` from ``pipe.count()``
		"""
		if other is _COLLECT_SENTINEL:
			return self.collect()
		if other is _COUNT_SENTINEL:
			return self.count()
		if other is _COLLECT_SPLIT_SENTINEL:
			return self.collect_split()
		if callable(other):
			return other(self)
		return NotImplemented
