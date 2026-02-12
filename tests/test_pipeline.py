"""
Tests for pipeline construction, concurrency, error handling, and config.

Covers:
	- Concurrency (multi-worker processes, stage overlap)
	- Error handling within processes
	- ProcessConfig validation
"""

from __future__ import annotations

import time

import anyio
import pytest

from anyiostream import Stream
from anyiostream.process import ProcessConfig

pytestmark = pytest.mark.anyio


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
		assert sorted(result) == [10, 30]

	@pytest.mark.anyio
	async def test_filter_error_skips_item(self) -> None:
		def risky_pred(x: int) -> bool:
			if x == 3:
				raise ValueError("boom")
			return x % 2 == 0

		result = await Stream.from_iterable([1, 2, 3, 4]).filter(risky_pred).collect()
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
