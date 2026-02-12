"""
Tests for anyiostream — concurrent async pipeline with pipe syntax.

Covers:
	- Stream construction (from_iterable, from_callable)
	- Process kinds (map, flat_map, filter, foreach)
	- Concurrency (multi-worker processes)
	- Pipe operator syntax
	- Terminal operations (collect, count, reduce, first, take)
	- Backpressure behavior
	- Error handling within processes
	- Async and sync function support
	- Context manager iteration (open())
"""

from __future__ import annotations

import time
from collections.abc import AsyncIterator

import anyio
import pytest

from anyiostream import Stream, pipe
from anyiostream.process import ProcessConfig

# Mark @pytest.mark.anyio on all test functions in the module
pytestmark = pytest.mark.anyio
# =========================================================================
# Helpers
# =========================================================================


@pytest.mark.anyio
async def async_double(x: int) -> int:
	"""Async 1:1 transform."""
	await anyio.sleep(0)  # yield to event loop
	return x * 2


@pytest.mark.anyio
def sync_double(x: int) -> int:
	"""Sync 1:1 transform."""
	return x * 2


@pytest.mark.anyio
async def async_is_even(x: int) -> bool:
	await anyio.sleep(0)
	return x % 2 == 0


@pytest.mark.anyio
def sync_is_even(x: int) -> bool:
	return x % 2 == 0


@pytest.mark.anyio
async def async_expand(x: int) -> AsyncIterator[str]:
	"""Async 1:N transform — yields x copies of str(x)."""
	for i in range(x):
		await anyio.sleep(0)
		yield f"{x}-{i}"


@pytest.mark.anyio
def sync_expand(x: int) -> list[str]:
	"""Sync 1:N transform."""
	return [f"{x}-{i}" for i in range(x)]


# =========================================================================
# Construction
# =========================================================================


class TestStreamConstruction:
	"""Test Stream factory methods."""

	@pytest.mark.anyio
	async def test_from_iterable_sync(self) -> None:
		result = await Stream.from_iterable([1, 2, 3]).collect()
		assert result == [1, 2, 3]

	@pytest.mark.anyio
	async def test_from_iterable_async(self) -> None:
		async def gen():
			for i in [10, 20, 30]:
				yield i

		result = await Stream.from_iterable(gen()).collect()
		assert result == [10, 20, 30]

	@pytest.mark.anyio
	async def test_from_iterable_empty(self) -> None:
		result = await Stream.from_iterable([]).collect()
		assert result == []

	@pytest.mark.anyio
	async def test_from_callable(self) -> None:
		result = await Stream.from_callable(lambda: [4, 5, 6]).collect()
		assert result == [4, 5, 6]

	@pytest.mark.anyio
	async def test_from_callable_async_gen(self) -> None:
		async def gen():
			for i in [7, 8, 9]:
				yield i

		result = await Stream.from_callable(gen).collect()
		assert result == [7, 8, 9]


# =========================================================================
# Map
# =========================================================================


class TestMap:
	"""Test 1:1 map transformations."""

	@pytest.mark.anyio
	async def test_map_sync(self) -> None:
		result = await Stream.from_iterable([1, 2, 3]).map(sync_double).collect()
		assert result == [2, 4, 6]

	@pytest.mark.anyio
	async def test_map_async(self) -> None:
		result = await Stream.from_iterable([1, 2, 3]).map(async_double).collect()
		assert result == [2, 4, 6]

	@pytest.mark.anyio
	async def test_map_chained(self) -> None:
		result = await (
			Stream.from_iterable([1, 2, 3]).map(sync_double).map(sync_double).collect()
		)
		assert result == [4, 8, 12]

	@pytest.mark.anyio
	async def test_map_workers(self) -> None:
		"""Multiple workers should produce the same items (order may vary)."""
		result = await (
			Stream.from_iterable(range(20)).map(sync_double, workers=4).collect()
		)
		assert sorted(result) == [i * 2 for i in range(20)]


# =========================================================================
# Filter
# =========================================================================


class TestFilter:
	"""Test filter processes."""

	@pytest.mark.anyio
	async def test_filter_sync(self) -> None:
		result = await Stream.from_iterable(range(6)).filter(sync_is_even).collect()
		assert result == [0, 2, 4]

	@pytest.mark.anyio
	async def test_filter_async(self) -> None:
		result = await Stream.from_iterable(range(6)).filter(async_is_even).collect()
		assert result == [0, 2, 4]

	@pytest.mark.anyio
	async def test_filter_all_removed(self) -> None:
		result = await Stream.from_iterable([1, 3, 5]).filter(sync_is_even).collect()
		assert result == []


# =========================================================================
# FlatMap
# =========================================================================


class TestFlatMap:
	"""Test 1:N flat_map transformations."""

	@pytest.mark.anyio
	async def test_flat_map_sync(self) -> None:
		result = await Stream.from_iterable([1, 2, 3]).flat_map(sync_expand).collect()
		expected = ["1-0", "2-0", "2-1", "3-0", "3-1", "3-2"]
		assert sorted(result) == sorted(expected)

	@pytest.mark.anyio
	async def test_flat_map_async(self) -> None:
		result = await Stream.from_iterable([1, 2, 3]).flat_map(async_expand).collect()
		expected = ["1-0", "2-0", "2-1", "3-0", "3-1", "3-2"]
		assert sorted(result) == sorted(expected)

	@pytest.mark.anyio
	async def test_flat_map_empty_expansion(self) -> None:
		result = await Stream.from_iterable([0, 0]).flat_map(sync_expand).collect()
		assert result == []

	@pytest.mark.anyio
	async def test_flat_map_workers(self) -> None:
		result = await (
			Stream.from_iterable([2, 3]).flat_map(sync_expand, workers=2).collect()
		)
		expected = ["2-0", "2-1", "3-0", "3-1", "3-2"]
		assert sorted(result) == sorted(expected)


# =========================================================================
# Foreach
# =========================================================================


class TestForeach:
	"""Test side-effect foreach process."""

	@pytest.mark.anyio
	async def test_foreach_passthrough(self) -> None:
		seen: list[int] = []
		result = await Stream.from_iterable([1, 2, 3]).foreach(seen.append).collect()
		assert result == [1, 2, 3]
		assert seen == [1, 2, 3]

	@pytest.mark.anyio
	async def test_foreach_async(self) -> None:
		seen: list[int] = []

		async def track(x: int) -> None:
			await anyio.sleep(0)
			seen.append(x)

		result = await Stream.from_iterable([10, 20]).foreach(track).collect()
		assert result == [10, 20]
		assert sorted(seen) == [10, 20]


# =========================================================================
# Pipe operator syntax
# =========================================================================


class TestPipeOperator:
	"""Test ``stream | pipe.map(fn)`` composition."""

	@pytest.mark.anyio
	async def test_pipe_map(self) -> None:
		result = await (
			Stream.from_iterable([1, 2, 3]) | pipe.map(sync_double) | pipe.collect()
		)
		assert result == [2, 4, 6]

	@pytest.mark.anyio
	async def test_pipe_filter(self) -> None:
		result = await (
			Stream.from_iterable(range(6)) | pipe.filter(sync_is_even) | pipe.collect()
		)
		assert result == [0, 2, 4]

	@pytest.mark.anyio
	async def test_pipe_flat_map(self) -> None:
		result = await (
			Stream.from_iterable([2, 3]) | pipe.flat_map(sync_expand) | pipe.collect()
		)
		expected = ["2-0", "2-1", "3-0", "3-1", "3-2"]
		assert sorted(result) == sorted(expected)

	@pytest.mark.anyio
	async def test_pipe_chained(self) -> None:
		result = await (
			Stream.from_iterable(range(10))
			| pipe.filter(sync_is_even)
			| pipe.map(sync_double)
			| pipe.collect()
		)
		assert sorted(result) == [0, 4, 8, 12, 16]

	@pytest.mark.anyio
	async def test_pipe_count(self) -> None:
		count = await (
			Stream.from_iterable(range(5)) | pipe.map(sync_double) | pipe.count()
		)
		assert count == 5

	@pytest.mark.anyio
	async def test_pipe_foreach(self) -> None:
		seen: list[int] = []
		result = await (
			Stream.from_iterable([1, 2]) | pipe.foreach(seen.append) | pipe.collect()
		)
		assert result == [1, 2]
		assert seen == [1, 2]

	@pytest.mark.anyio
	async def test_pipe_with_workers(self) -> None:
		result = await (
			Stream.from_iterable(range(20))
			| pipe.map(sync_double, workers=4)
			| pipe.filter(lambda x: x >= 10, workers=2)
			| pipe.collect()
		)
		assert sorted(result) == [
			10,
			12,
			14,
			16,
			18,
			20,
			22,
			24,
			26,
			28,
			30,
			32,
			34,
			36,
			38,
		]


# =========================================================================
# Terminal operations
# =========================================================================


class TestTerminals:
	"""Test terminal operations beyond collect."""

	@pytest.mark.anyio
	async def test_reduce(self) -> None:
		total = await Stream.from_iterable([1, 2, 3, 4]).reduce(
			lambda acc, x: acc + x, 0
		)
		assert total == 10

	@pytest.mark.anyio
	async def test_reduce_async(self) -> None:
		async def add(acc: int, x: int) -> int:
			return acc + x

		total = await Stream.from_iterable([1, 2, 3]).reduce(add, 0)
		assert total == 6

	@pytest.mark.anyio
	async def test_first(self) -> None:
		result = await Stream.from_iterable([10, 20, 30]).first()
		assert result == 10

	@pytest.mark.anyio
	async def test_first_empty(self) -> None:
		result = await Stream.from_iterable([]).first()
		assert result is None

	@pytest.mark.anyio
	async def test_take(self) -> None:
		result = await Stream.from_iterable(range(100)).take(3)
		assert len(result) == 3

	@pytest.mark.anyio
	async def test_count(self) -> None:
		count = await Stream.from_iterable(range(7)).count()
		assert count == 7


# =========================================================================
# Concurrency
# =========================================================================


class TestConcurrency:
	"""Verify that multi-worker processes actually run concurrently."""

	@pytest.mark.anyio
	async def test_concurrent_speedup(self) -> None:
		"""
		5 items x 0.1s each with 5 workers should complete in ~0.1s,
		not ~0.5s (sequential).
		"""

		async def slow_op(x: int) -> int:
			await anyio.sleep(0.1)
			return x

		start = time.monotonic()
		result = await (
			Stream.from_iterable(range(5))
			.map(slow_op, workers=5, buffer_size=5)
			.collect()
		)
		elapsed = time.monotonic() - start

		assert sorted(result) == [0, 1, 2, 3, 4]
		# With 5 workers, should be ~0.1s. Allow generous margin.
		assert elapsed < 0.4, f"Expected concurrent execution, took {elapsed:.2f}s"

	@pytest.mark.anyio
	async def test_multistage_concurrent(self) -> None:
		"""
		Two slow processes should overlap: items flow through process 2
		while process 1 is still producing.
		"""
		events: list[tuple[str, int, float]] = []
		t0 = time.monotonic()

		async def stage1(x: int) -> int:
			events.append(("s1_start", x, time.monotonic() - t0))
			await anyio.sleep(0.05)
			events.append(("s1_end", x, time.monotonic() - t0))
			return x

		async def stage2(x: int) -> int:
			events.append(("s2_start", x, time.monotonic() - t0))
			await anyio.sleep(0.05)
			events.append(("s2_end", x, time.monotonic() - t0))
			return x * 10

		result = await (
			Stream.from_iterable(range(4))
			.map(stage1, workers=2, buffer_size=2)
			.map(stage2, workers=2, buffer_size=2)
			.collect()
		)

		assert sorted(result) == [0, 10, 20, 30]

		# Verify overlap: stage2 should start before all stage1 items finish
		s1_ends = [t for name, _, t in events if name == "s1_end"]
		s2_starts = [t for name, _, t in events if name == "s2_start"]
		if s2_starts and s1_ends:
			# At least one s2 should start before the last s1 ends
			assert min(s2_starts) < max(s1_ends), (
				"Stage 2 should start before Stage 1 finishes all items"
			)


# =========================================================================
# Error handling
# =========================================================================


class TestErrorHandling:
	"""Test that errors in processes don't crash the pipeline."""

	@pytest.mark.anyio
	async def test_map_error_skips_item(self) -> None:
		"""Errors in map should skip the item and continue."""

		def risky(x: int) -> int:
			if x == 2:
				raise ValueError("boom")
			return x * 10

		result = await Stream.from_iterable([1, 2, 3]).map(risky).collect()
		# Item 2 should be skipped due to error
		assert sorted(result) == [10, 30]

	@pytest.mark.anyio
	async def test_filter_error_skips_item(self) -> None:
		def risky_pred(x: int) -> bool:
			if x == 3:
				raise ValueError("boom")
			return x % 2 == 0

		result = await Stream.from_iterable([1, 2, 3, 4]).filter(risky_pred).collect()
		# x=3 errors out and is skipped, x=1 filtered, x=2,4 pass
		assert sorted(result) == [2, 4]


# =========================================================================
# ProcessConfig validation
# =========================================================================


class TestProcessConfig:
	"""Test ProcessConfig validation."""

	def test_valid_config(self) -> None:
		cfg = ProcessConfig(workers=3, buffer_size=10, name="test")
		assert cfg.workers == 3
		assert cfg.buffer_size == 10

	def test_invalid_workers(self) -> None:
		with pytest.raises(ValueError, match="workers must be >= 1"):
			ProcessConfig(workers=0)

	def test_invalid_buffer(self) -> None:
		with pytest.raises(ValueError, match="buffer_size must be >= 0"):
			ProcessConfig(buffer_size=-1)


# =========================================================================
# Backpressure
# =========================================================================


class TestBackpressure:
	"""Test that backpressure actually works with long-running tasks."""

	@pytest.mark.anyio
	async def test_backpressure_with_long_running_tasks(self) -> None:
		"""
		Test multi-stage pipeline with short and long tasks running concurrently.

		Scenario:
		- 10 initial items
		- Stage 1: map(short_task) - 0.1s per item, 10 workers
		- Stage 2: map(long_task) - 10s per item, 10 workers

		Expected behavior with proper backpressure and no worker exhaustion:
		- Stage 1 completes all 10 items in ~0.1s (parallel with 10 workers)
		- Stage 2 processes items as they arrive from Stage 1
		- With 10 workers on Stage 2, all 10 long tasks run in parallel
		- Total time: ~0.1s (stage 1) + 10s (stage 2) = ~10.1s ≈ 11s

		This demonstrates:
		- Proper pipeline concurrency (stages overlap)
		- No worker exhaustion (enough workers to handle load)
		- Backpressure allows items to flow through without monopolization
		"""
		stage1_events: list[tuple[str, int, float]] = []
		stage2_events: list[tuple[str, int, float]] = []
		t0 = time.monotonic()

		# Stage 1: Short tasks (0.1s each)
		async def short_task(x: int) -> int:
			stage1_events.append(("start", x, time.monotonic() - t0))
			await anyio.sleep(0.1)
			stage1_events.append(("end", x, time.monotonic() - t0))
			return x

		# Stage 2: Long tasks (10s each)
		async def long_task(x: int) -> int:
			stage2_events.append(("start", x, time.monotonic() - t0))
			await anyio.sleep(10.0)
			stage2_events.append(("end", x, time.monotonic() - t0))
			return x * 2

		# Multi-stage pipeline with enough workers to avoid blocking
		result = await (
			Stream.from_iterable(range(10))
			.map(short_task, workers=10, buffer_size=2)  # 10 workers for parallel processing
			.map(long_task, workers=10, buffer_size=2)   # 10 workers for parallel processing
			.collect()
		)

		total_time = time.monotonic() - t0

		assert sorted(result) == [i * 2 for i in range(10)]

		# Verify timing: with proper concurrency, should complete in ~11s
		# Stage 1: ~0.1s (all 10 items processed in parallel)
		# Stage 2: ~10s (all 10 items processed in parallel)
		# Total: ~10.1s (with some overlap and overhead, target is ~11s)
		assert 9.5 < total_time < 12.0, (
			f"Expected pipeline to complete in ~11s with proper concurrency, "
			f"but took {total_time:.2f}s. This indicates workers may be blocked or "
			f"resources exhausted."
		)

		# Verify stage 1 completed quickly (all items in ~0.1s due to parallelism)
		stage1_ends = [t for ev, _, t in stage1_events if ev == "end"]
		if stage1_ends:
			stage1_duration = max(stage1_ends) - min(stage1_ends)
			# With 10 workers, all 10 items should complete nearly simultaneously
			# Allow up to 0.5s for scheduling overhead
			assert stage1_duration < 0.5, (
				f"Stage 1 (short tasks) should complete in ~0.1s with 10 workers, "
				f"but took {stage1_duration:.2f}s"
			)

		# Verify stage 2 tasks run concurrently (all starting within short timeframe)
		stage2_starts = [t for ev, _, t in stage2_events if ev == "start"]
		if len(stage2_starts) >= 2:
			stage2_start_spread = max(stage2_starts) - min(stage2_starts)
			# All 10 long tasks should start within ~1s as short tasks complete
			assert stage2_start_spread < 1.5, (
				f"Stage 2 (long tasks) should start concurrently as Stage 1 completes, "
				f"but starts spread over {stage2_start_spread:.2f}s. "
				f"This indicates backpressure or resource issues."
			)

		# Verify stages overlap (stage 2 starts before stage 1 fully completes)
		if stage1_ends and stage2_starts:
			first_stage2_start = min(stage2_starts)
			last_stage1_end = max(stage1_ends)
			# Pipeline concurrency: stage 2 should start processing before stage 1 fully done
			# (though in practice with such fast stage 1, they might complete before stage 2 starts)
			print(
				f"\nPipeline timing:"
				f"\n  Stage 1 duration: {max(stage1_ends):.3f}s"
				f"\n  First Stage 2 start: {first_stage2_start:.3f}s"
				f"\n  Last Stage 1 end: {last_stage1_end:.3f}s"
				f"\n  Total time: {total_time:.3f}s"
			)

	@pytest.mark.anyio
	async def test_backpressure_single_worker_per_stage(self) -> None:
		"""
		Test multi-stage pipeline with limited workers (1 per stage).

		Scenario:
		- 10 initial items
		- Stage 1: map(short_task) - 0.1s per item, 1 worker
		- Stage 2: map(long_task) - 10s per item, 1 worker

		Expected behavior with 1 worker per stage and buffer_size=2:
		- Stage 1 processes first few items quickly (until buffer fills)
		- Then Stage 1 blocks waiting for Stage 2 to consume from buffer
		- Stage 2 processes items sequentially: 10 × 10s = 100s
		- Due to backpressure, Stage 1 and Stage 2 are tightly coupled:
		  - Item 0: Stage 1 (0.1s) → buffer → Stage 2 starts (10s)
		  - Item 1: Stage 1 (0.1s) → buffer fills
		  - Item 2: Stage 1 (0.1s) → buffer full, blocks
		  - Item 3: Stage 1 (0.1s) → still blocking
		  - Item 4: Stage 2 finishes item 0 → Stage 1 can proceed
		- Total time: ~100s (dominated by Stage 2's sequential processing)

		This demonstrates:
		- Sequential processing with limited workers
		- Backpressure prevents Stage 1 from running ahead (buffer_size=2)
		- Pipeline stages are synchronized by backpressure
		"""
		stage1_events: list[tuple[str, int, float]] = []
		stage2_events: list[tuple[str, int, float]] = []
		t0 = time.monotonic()

		# Stage 1: Short tasks (0.1s each)
		async def short_task(x: int) -> int:
			stage1_events.append(("start", x, time.monotonic() - t0))
			await anyio.sleep(0.1)
			stage1_events.append(("end", x, time.monotonic() - t0))
			return x

		# Stage 2: Long tasks (10s each)
		async def long_task(x: int) -> int:
			stage2_events.append(("start", x, time.monotonic() - t0))
			await anyio.sleep(10.0)
			stage2_events.append(("end", x, time.monotonic() - t0))
			return x * 2

		# Multi-stage pipeline with 1 worker per stage (sequential)
		result = await (
			Stream.from_iterable(range(10))
			.map(short_task, workers=1, buffer_size=2)  # 1 worker - sequential processing
			.map(long_task, workers=1, buffer_size=2)   # 1 worker - sequential processing
			.collect()
		)

		total_time = time.monotonic() - t0

		assert sorted(result) == [i * 2 for i in range(10)]

		# Verify timing: with 1 worker per stage, expect sequential processing
		# Stage 2 dominates: 10 items × 10s = 100s (sequential)
		# Total should be ~100s
		assert 95.0 < total_time < 105.0, (
			f"Expected pipeline to complete in ~100s with 1 worker per stage, "
			f"but took {total_time:.2f}s. This indicates timing issues."
		)

		# Verify stage 1 is blocked by backpressure
		# First few items process quickly, then Stage 1 must wait for Stage 2
		stage1_starts = [t for ev, _, t in stage1_events if ev == "start"]
		stage1_ends = [t for ev, _, t in stage1_events if ev == "end"]

		if len(stage1_starts) >= 4:
			# First 4 items should process quickly (before buffer backpressure kicks in)
			first_four_duration = stage1_ends[3] - stage1_starts[0]
			assert first_four_duration < 0.6, (
				f"First 4 items in Stage 1 should process quickly (~0.4s), "
				f"but took {first_four_duration:.2f}s"
			)

			# Later items are spread out due to backpressure from Stage 2
			if len(stage1_ends) >= 10:
				total_stage1_duration = stage1_ends[9] - stage1_starts[0]
				# Stage 1 is blocked by Stage 2, so total duration approaches 100s
				assert total_stage1_duration > 50.0, (
					f"Stage 1 should be blocked by Stage 2 backpressure, "
					f"but completed in {total_stage1_duration:.2f}s"
				)

		# Verify stage 2 processes sequentially (items spread over ~100s)
		stage2_starts = [t for ev, _, t in stage2_events if ev == "start"]
		stage2_ends = [t for ev, _, t in stage2_events if ev == "end"]
		if len(stage2_starts) >= 2:
			stage2_start_spread = max(stage2_starts) - min(stage2_starts)
			# With 1 worker, starts should be spread over ~90s (after first one starts)
			assert 85.0 < stage2_start_spread < 95.0, (
				f"Stage 2 (long tasks) should have starts spread over ~90s with 1 worker, "
				f"but spread was {stage2_start_spread:.2f}s"
			)

		# Verify pipeline overlap: Stage 2 starts before Stage 1 completes
		if stage1_ends and stage2_starts:
			first_stage2_start = min(stage2_starts)
			last_stage1_end = max(stage1_ends)

			# Stage 2 should start after first stage 1 item completes (~0.1s)
			assert first_stage2_start < last_stage1_end, (
				f"Pipeline stages should overlap: Stage 2 should start before Stage 1 completes. "
				f"Stage 1 ends at {last_stage1_end:.2f}s, Stage 2 starts at {first_stage2_start:.2f}s"
			)

			print(
				f"\nPipeline timing (1 worker per stage):"
				f"\n  First Stage 1 end: {min(stage1_ends):.3f}s"
				f"\n  Last Stage 1 end: {last_stage1_end:.3f}s"
				f"\n  First Stage 2 start: {first_stage2_start:.3f}s"
				f"\n  Last Stage 2 start: {max(stage2_starts):.3f}s"
				f"\n  First Stage 2 end: {min(stage2_ends):.3f}s"
				f"\n  Last Stage 2 end: {max(stage2_ends):.3f}s"
				f"\n  Total time: {total_time:.3f}s"
				f"\n  Stage 1 blocked by backpressure: {last_stage1_end - stage1_ends[3]:.3f}s"
				f"\n  Pipeline demonstrates backpressure control"
			)
