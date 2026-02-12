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

		max_buffer_bytes: Optional memory-based buffer limit in bytes.
			When specified, enables MemoryBuffer integration which tracks actual memory usage.
			The architecture becomes:
			Process → MemoryObjectStream(∞) → MemoryBuffer → MemoryObjectStream(buffer_size) → Process

			This prevents OOM by capping memory usage rather than item count.
			MemoryBuffer receives items without blocking upstream and forwards them to
			downstream when memory budget allows.

			Example: max_buffer_bytes=10_000_000 (10MB limit)

		size_func: Optional function to calculate item size in bytes.
			Only used when max_buffer_bytes is specified.
			If None, MemoryBuffer uses smart default estimation for common types.
			Signature: Callable[[Any], int]

		name: Optional human-readable label for debugging / tracing.
	"""

	workers: int = 1
	buffer_size: float = 0
	max_buffer_bytes: int = 10_000_000  # 10MB default
	size_func: Callable[[Any], int] | None = None
	name: str | None = None

	def __post_init__(self) -> None:
		if self.workers < 1:
			raise ValueError(f"workers must be >= 1, got {self.workers}")
		if self.buffer_size < 0:
			raise ValueError(f"buffer_size must be >= 0, got {self.buffer_size}")
		if self.max_buffer_bytes <= 0:
			raise ValueError(f"max_buffer_bytes must be > 0, got {self.max_buffer_bytes}")


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
# Memory Buffer
# ---------------------------------------------------------------------------


class MemoryBuffer:
	"""
	Memory-aware buffer layer that sits between pipeline stages.

	Architecture:
		Process → MemoryObjectStream(math.inf) → MemoryBuffer → MemoryObjectStream(item_queue_size) → Process

	The MemoryBuffer:
	1. Receives items from upstream (unbounded queue - never blocks upstream)
	2. Tracks cumulative memory usage with pluggable size calculation
	3. Holds items in internal buffer when memory limit would be exceeded
	4. Forwards items to downstream when:
	   - Memory budget allows (current_memory + item_size <= max_buffer_bytes)
	   - Downstream has capacity (send won't block indefinitely)
	5. Provides backpressure by buffering items rather than blocking upstream

	This approach allows fast stages to produce freely while maintaining memory limits.

	Enabled by setting max_buffer_bytes in ProcessConfig.
	"""

	def __init__(
		self,
		max_buffer_bytes: int,
		size_func: Callable[[Any], int] | None = None,
	) -> None:
		"""
		Initialize buffer allocator.

		Args:
			max_buffer_bytes: Maximum memory to buffer in bytes.
			size_func: Optional function to calculate item size.
				If None, uses default approximation strategy.
		"""
		self.max_buffer_bytes = max_buffer_bytes
		self.size_func = size_func or self._default_size_func

	@staticmethod
	def _default_size_func(item: Any) -> int:
		"""
		Default size calculation with fast approximations.

		Strategy:
		1. For simple types (int, str, bytes): use sys.getsizeof()
		2. For lists: estimate as sys.getsizeof(list) + len(list) * average_item_size
		3. For dicts: similar estimation
		4. For other objects: use shallow size (fast but may underestimate)

		Args:
			item: Item to measure.

		Returns:
			Estimated size in bytes.
		"""
		base_size = sys.getsizeof(item)

		# Simple types - return base size
		if isinstance(item, (int, float, bool, type(None), str, bytes)):
			return base_size

		# Lists - estimate content size
		if isinstance(item, list):
			if not item:
				return base_size
			# Sample first few items to estimate average
			sample_size = min(5, len(item))
			sample_total = sum(sys.getsizeof(item[i]) for i in range(sample_size))
			avg_item_size = sample_total // sample_size
			return base_size + len(item) * avg_item_size

		# Dicts - estimate key+value sizes
		if isinstance(item, dict):
			if not item:
				return base_size
			# Sample first few items
			sample_items = list(item.items())[:5]
			sample_total = sum(
				sys.getsizeof(k) + sys.getsizeof(v) for k, v in sample_items
			)
			avg_pair_size = sample_total // len(sample_items) if sample_items else 0
			return base_size + len(item) * avg_pair_size

		# Other types - use shallow size
		return base_size

	async def run(
		self,
		recv: MemoryObjectReceiveStream[Any],
		send: MemoryObjectSendStream[Any],
	) -> None:
		"""
		Run the buffer allocator.

		Receives items from upstream (unbounded), tracks memory,
		forwards to downstream when budget allows.

		Args:
			recv: Upstream receive stream (unbounded - won't block us).
			send: Downstream send stream (bounded - may block).
		"""
		async with recv, send:
			# Internal buffer: queue of (item, size) tuples
			buffer: list[tuple[Any, int]] = []
			current_memory = 0
			upstream_done = False

			async def receive_from_upstream() -> None:
				"""Continuously receive from upstream and add to buffer."""
				nonlocal current_memory, upstream_done
				try:
					async for item in recv:
						item_size = self.size_func(item)
						buffer.append((item, item_size))
						current_memory += item_size
				finally:
					upstream_done = True

			async def send_to_downstream() -> None:
				"""Forward items from buffer to downstream when memory allows."""
				nonlocal current_memory
				while not upstream_done or buffer:
					# Wait for items in buffer
					if not buffer:
						if upstream_done:
							return
						await anyio.sleep(0.001)
						continue

					# Check memory budget
					if current_memory > self.max_buffer_bytes:
						# Over budget - must forward to make room
						pass  # Will forward below

					# Forward oldest item (FIFO)
					item, item_size = buffer.pop(0)

					# Send to downstream (may block if downstream is full)
					await send.send(item)

					# Update memory counter
					current_memory -= item_size

			# Run both tasks concurrently
			async with anyio.create_task_group() as tg:
				tg.start_soon(receive_from_upstream)
				tg.start_soon(send_to_downstream)

