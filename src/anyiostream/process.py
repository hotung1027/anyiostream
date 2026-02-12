"""
Pipeline process definitions, configuration, and Result-aware mixin.

A Process is a unit of work in the pipeline that transforms items from
an input stream to an output stream with configurable concurrency.

Also provides the ``ResultStages`` mixin (mixed into ``Stream``) for
Result-aware operations: ``try_map``, ``try_flat_map``, ``try_filter``,
``try_foreach``, ``recover``, ``ok_only``, ``errors_only``, and
``collect_split``.
"""

from __future__ import annotations

import sys
from collections.abc import AsyncIterable, Awaitable, Callable
from dataclasses import dataclass, field
from enum import Enum, auto
from typing import (
	Any,
	TypeVar,
)

import anyio
from anyio.streams.memory import MemoryObjectReceiveStream, MemoryObjectSendStream

from anyiostream.result import (
	Err,
	Ok,
	_try_flat_map_wrap,
	_try_map_wrap,
)

T = TypeVar("T")
U = TypeVar("U")
E = TypeVar("E")

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------


class ProcessKind(Enum):
	"""The transformation type of a pipeline process."""

	MAP = auto()  # 1:1 — each input produces exactly one output
	FLAT_MAP = auto()  # 1:N — each input produces zero or more outputs
	FILTER = auto()  # 1:0|1 — each input is kept or dropped
	FOREACH = auto()  # 1:1 — side-effect only, passes through unchanged


@dataclass(frozen=True, slots=True)
class ProcessConfig:
	"""
	Per-process tunables.

	Attributes:
		workers: Number of concurrent workers for this process.
			1 = sequential processing, N > 1 = fan-out via stream cloning.
		buffer_size: Backpressure buffer between this process and the next.
			Controls how many items can be queued waiting for downstream processing.

			Values:
			- 0 = rendezvous (strongest backpressure, synchronous handoff)
			- N > 0 = bounded buffer of size N items
			- math.inf = unbounded (no backpressure, potential memory bloat)

			Choosing buffer_size:
			- For smooth pipeline flow: Set buffer_size to accommodate the expected
			  output volume from fast stages. This prevents fast stages from blocking
			  unnecessarily while still maintaining bounded memory usage.
			  Example: If Stage 1 produces 100 items in 1s and Stage 2 takes 10s to
			  process them, use buffer_size=100 to let Stage 1 complete without blocking.

			- For tight memory constraints: Use buffer_size=0 (rendezvous) or small
			  values (e.g., 1-10) to minimize memory usage. This causes fast stages
			  to block and wait for slow stages, reducing parallelism.

			- For maximum throughput with unbounded input: Use buffer_size equal to
			  the number of workers in the next stage, or slightly larger to ensure
			  workers always have items available.

			Note: If max_buffer_bytes is specified, buffer_size is ignored.

		max_buffer_bytes: Optional memory-based buffer limit in bytes.
			When specified, the effective buffer size is calculated as:
			max_buffer_bytes / estimated_item_size_bytes

			This prevents OOM by capping memory usage rather than item count.
			The estimated item size defaults to 1024 bytes but can be overridden
			with item_size_hint.

			Example: max_buffer_bytes=10_000_000 (10MB) with 1KB items = ~10,000 items

		item_size_hint: Estimated average size of items in bytes.
			Only used when max_buffer_bytes is specified.
			Defaults to 1024 bytes if not provided.

		name: Optional human-readable label for debugging / tracing.
	"""

	workers: int = 1
	buffer_size: float = 0
	max_buffer_bytes: int | None = None
	item_size_hint: int = 1024
	name: str | None = None

	def __post_init__(self) -> None:
		if self.workers < 1:
			raise ValueError(f"workers must be >= 1, got {self.workers}")
		if self.buffer_size < 0:
			raise ValueError(f"buffer_size must be >= 0, got {self.buffer_size}")
		if self.max_buffer_bytes is not None and self.max_buffer_bytes <= 0:
			raise ValueError(f"max_buffer_bytes must be > 0, got {self.max_buffer_bytes}")
		if self.item_size_hint <= 0:
			raise ValueError(f"item_size_hint must be > 0, got {self.item_size_hint}")

	def get_effective_buffer_size(self) -> float:
		"""
		Calculate the effective buffer size based on configuration.

		Returns:
			The number of items that can be buffered.
			If max_buffer_bytes is specified, returns max_buffer_bytes / item_size_hint.
			Otherwise, returns buffer_size.
		"""
		if self.max_buffer_bytes is not None:
			# Convert to int since anyio requires integer buffer sizes
			return int(self.max_buffer_bytes / self.item_size_hint)
		return self.buffer_size


# ---------------------------------------------------------------------------
# Process
# ---------------------------------------------------------------------------


@dataclass(slots=True)
class Process[T, U]:
	"""
	A single concurrent processing unit in the pipeline.

	Each process:
	1. Reads items from an input ``MemoryObjectReceiveStream[T]``
	2. Applies ``func`` to each item
	3. Writes results to an output ``MemoryObjectSendStream[U]``

	Multiple workers can be spawned via ``config.workers``.  Each worker
	receives a *clone* of the input stream so items are load-balanced
	(first-available-wins).
	"""

	kind: ProcessKind
	func: Callable[..., Any]
	config: ProcessConfig = field(default_factory=ProcessConfig)

	# ------------------------------------------------------------------
	# Public API
	# ------------------------------------------------------------------

	async def run(
		self,
		in_recv: MemoryObjectReceiveStream[T],
		out_send: MemoryObjectSendStream[U],
	) -> None:
		"""
		Execute this process: spawn workers, process items, close channels.

		This method takes ownership of both channel ends and closes them
		when all workers are done.

		Args:
			in_recv: The receive end of the upstream channel.
			out_send: The send end of the downstream channel.
		"""
		async with in_recv, out_send:
			if self.config.workers == 1:
				# Fast path — no clone overhead
				await self._worker(in_recv, out_send)
			else:
				async with anyio.create_task_group() as tg:
					for _ in range(self.config.workers):
						tg.start_soon(
							self._worker,
							in_recv.clone(),
							out_send.clone(),
						)

	# ------------------------------------------------------------------
	# Internal
	# ------------------------------------------------------------------

	async def _worker(
		self,
		recv: MemoryObjectReceiveStream[T],
		send: MemoryObjectSendStream[U],
	) -> None:
		"""Single worker loop: read → transform → write."""
		async with recv, send:
			async for item in recv:
				try:
					await self._process_item(item, send)
				except Exception as exc:
					label = self.config.name or self.func.__name__
					print(f"[pipeline/{label}] error processing item: {exc}")

	async def _process_item(
		self,
		item: T,
		send: MemoryObjectSendStream[U],
	) -> None:
		"""Dispatch to the correct handler based on process kind."""
		match self.kind:
			case ProcessKind.MAP:
				result = self.func(item)
				if isinstance(result, Awaitable):
					result = await result
				await send.send(result)

			case ProcessKind.FLAT_MAP:
				result = self.func(item)
				if isinstance(result, AsyncIterable):
					async for sub in result:
						await send.send(sub)
				elif isinstance(result, Awaitable):
					# Awaitable that returns an iterable
					resolved = await result
					if isinstance(resolved, AsyncIterable):
						async for sub in resolved:
							await send.send(sub)
					else:
						for sub in resolved:
							await send.send(sub)
				else:
					# Sync iterable
					for sub in result:
						await send.send(sub)

			case ProcessKind.FILTER:
				result = self.func(item)
				if isinstance(result, Awaitable):
					result = await result
				if result:
					await send.send(item)  # type: ignore[arg-type]

			case ProcessKind.FOREACH:
				result = self.func(item)
				if isinstance(result, Awaitable):
					await result
				await send.send(item)  # type: ignore[arg-type]


# ---------------------------------------------------------------------------
# ResultStages mixin (mixed into Stream)
# ---------------------------------------------------------------------------


class ResultStages:
	"""Result-aware pipeline stages mixed into ``Stream``.

	Uses ``self.__class__`` to construct new instances, avoiding circular
	imports with ``stream.py``.
	"""

	__slots__ = ()

	# -- try_map / try_flat_map / try_filter / try_foreach ------------------

	def try_map(
		self,
		func: Callable[..., Any],
		*,
		err: Callable[..., Any] | None = None,
		workers: int = 1,
		buffer_size: float = 0,
		name: str | None = None,
	) -> Any:
		"""Result-aware 1:1 transform.

		Applies *func* to ``Ok`` values (or raw values).  Exceptions become
		``Err(PipelineError(...))``.  If *err* is provided, ``Err`` items
		are transformed by ``err(error) → Err(result)``.  Otherwise ``Err``
		passes through unchanged.
		"""
		label = name or getattr(func, "__name__", "try_map")
		process = Process(
			kind=ProcessKind.MAP,
			func=_try_map_wrap(func, label, err),
			config=ProcessConfig(workers=workers, buffer_size=buffer_size, name=name),
		)
		return self.__class__(self._source_factory, [*self._processes, process])

	def try_flat_map(
		self,
		func: Callable[..., Any],
		*,
		err: Callable[..., Any] | None = None,
		workers: int = 1,
		buffer_size: float = 0,
		name: str | None = None,
	) -> Any:
		"""Result-aware 1:N transform.

		Each sub-item from *func* is wrapped as ``Ok``.  Exceptions become
		a single ``Err``.  If *err* is provided, ``Err`` items are
		transformed by ``err(error) → Err(result)``.
		"""
		label = name or getattr(func, "__name__", "try_flat_map")
		process = Process(
			kind=ProcessKind.FLAT_MAP,
			func=_try_flat_map_wrap(func, label, err),
			config=ProcessConfig(workers=workers, buffer_size=buffer_size, name=name),
		)
		return self.__class__(self._source_factory, [*self._processes, process])

	def try_filter(
		self,
		predicate: Callable[..., Any],
		*,
		workers: int = 1,
		buffer_size: float = 0,
		name: str | None = None,
	) -> Any:
		"""Result-aware filter.

		Applies *predicate* to ``Ok`` values.  ``Err`` always passes through.
		"""

		async def _wrapped(item: Any) -> bool:
			if isinstance(item, Err):
				return True
			value = item.value if isinstance(item, Ok) else item
			result = predicate(value)
			if isinstance(result, Awaitable):
				result = await result
			return bool(result)

		process = Process(
			kind=ProcessKind.FILTER,
			func=_wrapped,
			config=ProcessConfig(workers=workers, buffer_size=buffer_size, name=name),
		)
		return self.__class__(self._source_factory, [*self._processes, process])

	def try_foreach(
		self,
		func: Callable[..., Any],
		*,
		err: Callable[..., Any] | None = None,
		workers: int = 1,
		buffer_size: float = 0,
		name: str | None = None,
	) -> Any:
		"""Result-aware side-effect.

		Calls *func* on ``Ok`` values for observation (logging, metrics).
		If *err* is provided, also calls ``err(error)`` on ``Err`` items.
		Items pass through unchanged.
		"""

		async def _wrapped(item: Any) -> None:
			if isinstance(item, Ok):
				result = func(item.value)
				if isinstance(result, Awaitable):
					await result
			elif isinstance(item, Err) and err is not None:
				result = err(item.error)
				if isinstance(result, Awaitable):
					await result

		process = Process(
			kind=ProcessKind.FOREACH,
			func=_wrapped,
			config=ProcessConfig(workers=workers, buffer_size=buffer_size, name=name),
		)
		return self.__class__(self._source_factory, [*self._processes, process])

	# -- Exit ramps: leave Result mode ------------------------------------

	def recover(
		self,
		func: Callable[..., Any],
		*,
		workers: int = 1,
		buffer_size: float = 0,
		name: str | None = None,
	) -> Any:
		"""Convert ``Err`` to a value using *func*; ``Ok`` is unwrapped.

		After this stage, items are plain values (no longer ``Result``).
		"""

		async def _wrapped(item: Any) -> Any:
			if isinstance(item, Err):
				result = func(item.error)
				if isinstance(result, Awaitable):
					result = await result
				return result
			if isinstance(item, Ok):
				return item.value
			return item

		process = Process(
			kind=ProcessKind.MAP,
			func=_wrapped,
			config=ProcessConfig(workers=workers, buffer_size=buffer_size, name=name),
		)
		return self.__class__(self._source_factory, [*self._processes, process])

	def ok_only(self) -> Any:
		"""Keep only ``Ok`` values, unwrap them.  Drop all ``Err``."""

		def _extract(item: Any) -> list[Any]:
			if isinstance(item, Ok):
				return [item.value]
			if isinstance(item, Err):
				return []
			return [item]

		process = Process(
			kind=ProcessKind.FLAT_MAP,
			func=_extract,
			config=ProcessConfig(name="ok_only"),
		)
		return self.__class__(self._source_factory, [*self._processes, process])

	def errors_only(self) -> Any:
		"""Keep only ``Err`` values, unwrap to error.  Drop all ``Ok``."""

		def _extract(item: Any) -> list[Any]:
			if isinstance(item, Err):
				return [item.error]
			return []

		process = Process(
			kind=ProcessKind.FLAT_MAP,
			func=_extract,
			config=ProcessConfig(name="errors_only"),
		)
		return self.__class__(self._source_factory, [*self._processes, process])

	# -- Terminal: partition -----------------------------------------------

	async def collect_split(self) -> tuple[list[Any], list[Any]]:
		"""Collect and partition into ``(ok_values, errors)``."""
		oks: list[Any] = []
		errs: list[Any] = []
		async with self._execute() as recv:
			async for item in recv:
				if isinstance(item, Ok):
					oks.append(item.value)
				elif isinstance(item, Err):
					errs.append(item.error)
				else:
					oks.append(item)
		return oks, errs


# ---------------------------------------------------------------------------
# Buffer Allocator
# ---------------------------------------------------------------------------


class BufferAllocator:
	"""
	Memory-aware buffer layer that sits between pipeline stages.

	This intermediate layer tracks actual memory usage of buffered items
	and enforces a memory limit, providing true OOM protection rather than
	relying on item count estimates.

	Architecture:
		Process → MemoryObjectStream(unbounded) → BufferAllocator → MemoryObjectStream(item_queue_size) → Process

	The BufferAllocator maintains an internal buffer of items with memory tracking:
	1. Pulls items from upstream (unbounded queue)
	2. Calculates actual memory size using sys.getsizeof()
	3. Buffers items up to max_buffer_bytes limit
	4. Forwards items to downstream queue when:
	   - Downstream has capacity (item_queue_size limit)
	   - We need to make room for new items
	5. Blocks upstream when memory budget is exhausted
	"""

	def __init__(
		self,
		max_buffer_bytes: int,
		item_queue_size: int = 0,
	) -> None:
		"""
		Initialize buffer allocator.

		Args:
			max_buffer_bytes: Maximum memory to buffer in bytes.
			item_queue_size: Size of downstream item queue (for backpressure).
		"""
		self.max_buffer_bytes = max_buffer_bytes
		self.item_queue_size = item_queue_size

	async def run(
		self,
		recv: MemoryObjectReceiveStream[Any],
		send: MemoryObjectSendStream[Any],
	) -> None:
		"""
		Run the buffer allocator.

		Pulls items from recv, buffers them with memory tracking,
		forwards to send when appropriate.

		Args:
			recv: Upstream receive stream (from previous process).
			send: Downstream send stream (to next process).
		"""
		async with recv, send:
			# Internal buffer: list of (item, size) tuples
			buffer: list[tuple[Any, int]] = []
			current_memory = 0
			upstream_done = False

			async def producer() -> None:
				"""Pull items from upstream and add to buffer."""
				nonlocal current_memory, upstream_done
				try:
					async for item in recv:
						item_size = sys.getsizeof(item)

						# Wait if buffer is full (memory limit reached)
						while current_memory + item_size > self.max_buffer_bytes:
							await anyio.sleep(0.001)

						# Add to buffer
						buffer.append((item, item_size))
						current_memory += item_size
				finally:
					upstream_done = True

			async def consumer() -> None:
				"""Forward items from buffer to downstream."""
				nonlocal current_memory
				while not upstream_done or buffer:
					# Wait for items in buffer
					while not buffer:
						if upstream_done:
							return  # All done
						await anyio.sleep(0.001)

					# Forward oldest item
					item, item_size = buffer.pop(0)
					await send.send(item)
					current_memory -= item_size

			# Run producer and consumer concurrently
			async with anyio.create_task_group() as tg:
				tg.start_soon(producer)
				tg.start_soon(consumer)
