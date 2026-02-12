"""
Tests for pipeline bottleneck behavior under various conditions.

Covers backpressure with long-running tasks, single-worker pipelines,
buffer sizing, MemoryBuffer preventing blocking, and worker allocation.
"""

from __future__ import annotations

import time

import anyio
import pytest

from anyiostream import Stream

pytestmark = pytest.mark.anyio


class TestBackpressure:
	"""Test that backpressure actually works with long-running tasks."""

	@pytest.mark.anyio
	async def test_backpressure_with_long_running_tasks(self) -> None:
		"""
		Test multi-stage pipeline with short and long tasks running concurrently.

		Scenario:
		- 10 initial items
		- Stage 1: map(short_task) - 0.01s per item, 10 workers
		- Stage 2: map(long_task) - 1s per item, 10 workers

		Expected behavior with proper backpressure and no worker exhaustion:
		- Stage 1 completes all 10 items in ~0.01s (parallel with 10 workers)
		- Stage 2 processes items as they arrive from Stage 1
		- With 10 workers on Stage 2, all 10 long tasks run in parallel
		- Total time: ~0.01s (stage 1) + 1s (stage 2) = ~1.01s
		"""
		stage1_events: list[tuple[str, int, float]] = []
		stage2_events: list[tuple[str, int, float]] = []
		t0 = time.monotonic()

		async def short_task(x: int) -> int:
			stage1_events.append(("start", x, time.monotonic() - t0))
			await anyio.sleep(0.01)
			stage1_events.append(("end", x, time.monotonic() - t0))
			return x

		async def long_task(x: int) -> int:
			stage2_events.append(("start", x, time.monotonic() - t0))
			await anyio.sleep(1.0)
			stage2_events.append(("end", x, time.monotonic() - t0))
			return x * 2

		result = await (
			Stream.from_iterable(range(10))
			.map(short_task, workers=10, buffer_size=2)
			.map(long_task, workers=10, buffer_size=2)
			.collect()
		)

		total_time = time.monotonic() - t0

		assert sorted(result) == [i * 2 for i in range(10)]

		assert 0.9 < total_time < 1.3, (
			f"Expected pipeline to complete in ~1s with proper concurrency, "
			f"but took {total_time:.2f}s. This indicates workers may be blocked or "
			f"resources exhausted."
		)

		stage1_ends = [t for ev, _, t in stage1_events if ev == "end"]
		if stage1_ends:
			stage1_duration = max(stage1_ends) - min(stage1_ends)
			assert stage1_duration < 0.1, (
				f"Stage 1 (short tasks) should complete in ~0.01s with 10 workers, "
				f"but took {stage1_duration:.2f}s"
			)

		stage2_starts = [t for ev, _, t in stage2_events if ev == "start"]
		if len(stage2_starts) >= 2:
			stage2_start_spread = max(stage2_starts) - min(stage2_starts)
			assert stage2_start_spread < 0.3, (
				f"Stage 2 (long tasks) should start concurrently as Stage 1 completes, "
				f"but starts spread over {stage2_start_spread:.2f}s. "
				f"This indicates backpressure or resource issues."
			)

		if stage1_ends and stage2_starts:
			first_stage2_start = min(stage2_starts)
			last_stage1_end = max(stage1_ends)
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
		Test multi-stage pipeline with limited workers (1 per stage) and default MemoryBuffer.

		Scenario:
		- 10 initial items
		- Stage 1: map(short_task) - 0.01s per item, 1 worker
		- Stage 2: map(long_task) - 1s per item, 1 worker

		Expected behavior with 1 worker per stage, buffer_size=2, and default max_buffer_bytes=10MB:
		- With default MemoryBuffer (10MB), upstream channels are unbounded
		- Stage 1 processes all items quickly (~0.1s) without blocking
		- MemoryBuffer absorbs items between Stage 1 and Stage 2
		- Stage 2 processes items sequentially: 10 × 1s = 10s
		- Total time: ~10s (dominated by Stage 2's sequential processing)
		"""
		stage1_events: list[tuple[str, int, float]] = []
		stage2_events: list[tuple[str, int, float]] = []
		t0 = time.monotonic()

		async def short_task(x: int) -> int:
			stage1_events.append(("start", x, time.monotonic() - t0))
			await anyio.sleep(0.01)
			stage1_events.append(("end", x, time.monotonic() - t0))
			return x

		async def long_task(x: int) -> int:
			stage2_events.append(("start", x, time.monotonic() - t0))
			await anyio.sleep(1.0)
			stage2_events.append(("end", x, time.monotonic() - t0))
			return x * 2

		result = await (
			Stream.from_iterable(range(10))
			.map(short_task, workers=1, buffer_size=2)
			.map(long_task, workers=1, buffer_size=2)
			.collect()
		)

		total_time = time.monotonic() - t0

		assert sorted(result) == [i * 2 for i in range(10)]

		assert 9.5 < total_time < 11.0, (
			f"Expected pipeline to complete in ~10s with 1 worker per stage, "
			f"but took {total_time:.2f}s. This indicates timing issues."
		)

		stage1_starts = [t for ev, _, t in stage1_events if ev == "start"]
		stage1_ends = [t for ev, _, t in stage1_events if ev == "end"]

		if len(stage1_starts) >= 4:
			first_four_duration = stage1_ends[3] - stage1_starts[0]
			assert first_four_duration < 0.1, (
				f"First 4 items in Stage 1 should process quickly (~0.04s), "
				f"but took {first_four_duration:.2f}s"
			)

			if len(stage1_ends) >= 10:
				total_stage1_duration = stage1_ends[9] - stage1_starts[0]
				assert total_stage1_duration < 0.2, (
					f"With default MemoryBuffer (10MB), Stage 1 should complete quickly (~0.1s), "
					f"but took {total_stage1_duration:.2f}s"
				)

		stage2_starts = [t for ev, _, t in stage2_events if ev == "start"]
		stage2_ends = [t for ev, _, t in stage2_events if ev == "end"]
		if len(stage2_starts) >= 2:
			stage2_start_spread = max(stage2_starts) - min(stage2_starts)
			assert 8.5 < stage2_start_spread < 9.5, (
				f"Stage 2 (long tasks) should have starts spread over ~9s with 1 worker, "
				f"but spread was {stage2_start_spread:.2f}s"
			)

		if stage1_ends and stage2_starts:
			first_stage2_start = min(stage2_starts)
			last_stage1_end = max(stage1_ends)

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

	@pytest.mark.anyio
	async def test_buffer_size_allows_smooth_pipeline_flow(self) -> None:
		"""
		Test that appropriate buffer_size allows fast stages to complete
		without being blocked by slow stages, while still preventing unbounded growth.

		Scenario:
		- 10 initial items
		- Stage 1: map(short_task) - 0.01s per item, 1 worker (total: 0.1s)
		- Stage 2: map(long_task) - 1s per item, 1 worker (total: 10s)
		- buffer_size=10 (enough to hold all items from fast stage)
		"""
		stage1_events: list[tuple[str, int, float]] = []
		stage2_events: list[tuple[str, int, float]] = []
		t0 = time.monotonic()

		async def short_task(x: int) -> int:
			stage1_events.append(("start", x, time.monotonic() - t0))
			await anyio.sleep(0.01)
			stage1_events.append(("end", x, time.monotonic() - t0))
			return x

		async def long_task(x: int) -> int:
			stage2_events.append(("start", x, time.monotonic() - t0))
			await anyio.sleep(1.0)
			stage2_events.append(("end", x, time.monotonic() - t0))
			return x * 2

		result = await (
			Stream.from_iterable(range(10))
			.map(short_task, workers=1, buffer_size=10)
			.map(long_task, workers=1, buffer_size=10)
			.collect()
		)

		total_time = time.monotonic() - t0

		assert sorted(result) == [i * 2 for i in range(10)]

		assert 9.5 < total_time < 11.0, (
			f"Expected pipeline to complete in ~10s, but took {total_time:.2f}s"
		)

		stage1_starts = [t for ev, _, t in stage1_events if ev == "start"]
		stage1_ends = [t for ev, _, t in stage1_events if ev == "end"]

		if len(stage1_ends) >= 10:
			total_stage1_duration = stage1_ends[9] - stage1_starts[0]
			assert total_stage1_duration < 0.3, (
				f"Stage 1 should complete all items in ~0.1s without blocking, "
				f"but took {total_stage1_duration:.2f}s"
			)

		stage2_starts = [t for ev, _, t in stage2_events if ev == "start"]
		stage2_ends = [t for ev, _, t in stage2_events if ev == "end"]
		if len(stage2_starts) >= 2:
			stage2_start_spread = max(stage2_starts) - min(stage2_starts)
			assert 8.5 < stage2_start_spread < 9.5, (
				f"Stage 2 should have starts spread over ~9s, "
				f"but spread was {stage2_start_spread:.2f}s"
			)

		if stage1_ends and stage2_starts:
			first_stage2_start = min(stage2_starts)
			last_stage1_end = max(stage1_ends)

			assert first_stage2_start < last_stage1_end, (
				"Pipeline stages should overlap: Stage 2 should start before Stage 1 completes."
			)

			print(
				f"\nPipeline timing (buffer_size=10, smooth flow):"
				f"\n  First Stage 1 end: {min(stage1_ends):.3f}s"
				f"\n  Last Stage 1 end: {last_stage1_end:.3f}s"
				f"\n  First Stage 2 start: {first_stage2_start:.3f}s"
				f"\n  Last Stage 2 start: {max(stage2_starts):.3f}s"
				f"\n  First Stage 2 end: {min(stage2_ends):.3f}s"
				f"\n  Last Stage 2 end: {max(stage2_ends):.3f}s"
				f"\n  Total time: {total_time:.3f}s"
				f"\n  Stage 1 NOT blocked - completed in ~0.1s"
				f"\n  Pipeline runs smoothly with appropriate buffer_size"
			)

	@pytest.mark.anyio
	async def test_two_workers_no_blocking_with_default(self) -> None:
		"""
		Test that with default max_buffer_bytes (10MB), Stage 1 is NOT blocked by Stage 2.

		Scenario:
		- 10 initial items
		- Stage 1: map(short_task) - 0.1s per item, 2 workers
		- Stage 2: map(long_task) - 2s per item, 2 workers
		"""
		stage1_events: list[tuple[str, int, float]] = []
		stage2_events: list[tuple[str, int, float]] = []
		t0 = time.monotonic()

		async def short_task(x: int) -> int:
			stage1_events.append(("start", x, time.monotonic() - t0))
			await anyio.sleep(0.1)
			stage1_events.append(("end", x, time.monotonic() - t0))
			return x

		async def long_task(x: int) -> int:
			stage2_events.append(("start", x, time.monotonic() - t0))
			await anyio.sleep(2.0)
			stage2_events.append(("end", x, time.monotonic() - t0))
			return x * 2

		result = await (
			Stream.from_iterable(range(10))
			.map(short_task, workers=2, buffer_size=0)
			.map(long_task, workers=2, buffer_size=0)
			.collect()
		)

		total_time = time.monotonic() - t0

		assert sorted(result) == [i * 2 for i in range(10)]

		stage1_ends = sorted([t for ev, _, t in stage1_events if ev == "end"])
		if stage1_ends:
			stage1_completion = max(stage1_ends)

			print(
				f"\nTwo workers with default max_buffer_bytes (10MB):"
				f"\n  Stage 1 completion: {stage1_completion:.3f}s"
				f"\n  Total time: {total_time:.3f}s"
			)

			assert stage1_completion < 1.0, (
				f"With default MemoryBuffer, Stage 1 should complete in ~0.5s, "
				f"but took {stage1_completion:.2f}s. Stage 1 appears to be blocked!"
			)

		stage1_starts = sorted([t for ev, _, t in stage1_events if ev == "start"])
		if len(stage1_starts) >= 4:
			gaps = []
			for i in range(2, len(stage1_starts), 2):
				if i < len(stage1_starts):
					gap = stage1_starts[i] - stage1_starts[i - 2]
					gaps.append(gap)

			if gaps:
				avg_gap = sum(gaps) / len(gaps)
				print(
					f"\n  Stage 1 batch timing:"
					f"\n    Average gap between batches: {avg_gap:.3f}s"
					f"\n    Expected: ~0.1s (not blocked)"
				)

				assert avg_gap < 0.3, (
					f"Stage 1 batch gap should be ~0.1s (not blocked), "
					f"but was {avg_gap:.2f}s. Stage 1 appears to be blocked!"
				)

		stage2_starts = sorted([t for ev, _, t in stage2_events if ev == "start"])
		if len(stage2_starts) >= 4:
			batch1_spread = stage2_starts[1] - stage2_starts[0]
			assert batch1_spread < 0.3, (
				f"First batch of Stage 2 should start nearly simultaneously, "
				f"but spread was {batch1_spread:.2f}s"
			)

			batch_gap = stage2_starts[2] - stage2_starts[0]
			assert 1.7 < batch_gap < 2.3, (
				f"Second batch of Stage 2 should start ~2s after first batch, "
				f"but gap was {batch_gap:.2f}s"
			)

		print(
			"\n  Conclusion: With default max_buffer_bytes (10MB), Stage 1 is NOT blocked!"
			"\n  MemoryBuffer is always enabled, providing memory protection and preventing blocking."
		)

	@pytest.mark.anyio
	async def test_memory_buffer_prevents_blocking(self) -> None:
		"""
		Test that MemoryBuffer prevents Stage 1 from being blocked by Stage 2.

		Scenario:
			- 10 initial items
			- Stage 1: map(short_task) - 0.1s per item, 2 workers
			- Stage 2: map(long_task) - 2s per item, 2 workers
			- WITH max_buffer_bytes set on Stage 2

		Architecture:
			Stage 1 → MemoryObjectStream(∞) → MemoryBuffer → MemoryObjectStream(buffer_size) → Stage 2
		"""
		stage1_events: list[tuple[str, int, float]] = []
		stage2_events: list[tuple[str, int, float]] = []
		t0 = time.monotonic()

		async def short_task(x: int) -> int:
			stage1_events.append(("start", x, time.monotonic() - t0))
			await anyio.sleep(0.1)
			stage1_events.append(("end", x, time.monotonic() - t0))
			return x

		async def long_task(x: int) -> int:
			stage2_events.append(("start", x, time.monotonic() - t0))
			await anyio.sleep(2.0)
			stage2_events.append(("end", x, time.monotonic() - t0))
			return x * 2

		result = await (
			Stream.from_iterable(range(10))
			.map(short_task, workers=2, buffer_size=0)
			.map(
				long_task,
				workers=2,
				buffer_size=2,
				max_buffer_bytes=10 * 1024,
			)
			.collect()
		)

		total_time = time.monotonic() - t0

		assert sorted(result) == [i * 2 for i in range(10)]

		stage1_ends = sorted([t for ev, _, t in stage1_events if ev == "end"])
		if stage1_ends:
			stage1_completion = max(stage1_ends)

			print(
				f"\nMemoryBuffer test results:"
				f"\n  Stage 1 completion time: {stage1_completion:.3f}s"
				f"\n  Total pipeline time: {total_time:.3f}s"
			)

			assert stage1_completion < 1.0, (
				f"With MemoryBuffer, Stage 1 should complete in ~0.5s, "
				f"but took {stage1_completion:.2f}s. "
				f"This indicates Stage 1 is still being blocked!"
			)

			stage1_starts = sorted([t for ev, _, t in stage1_events if ev == "start"])
			if len(stage1_starts) >= 4:
				gaps = []
				for i in range(2, len(stage1_starts), 2):
					if i < len(stage1_starts):
						gap = stage1_starts[i] - stage1_starts[i - 2]
						gaps.append(gap)

				if gaps:
					avg_gap = sum(gaps) / len(gaps)
					print(
						f"\n  Stage 1 batch timing:"
						f"\n    Average gap between batches: {avg_gap:.3f}s"
						f"\n    Expected: ~0.1s (not blocked)"
					)

					assert avg_gap < 0.3, (
						f"Stage 1 batch gap should be ~0.1s (not blocked), "
						f"but was {avg_gap:.2f}s"
					)

		print(
			f"\n  Conclusion: With MemoryBuffer, Stage 1 is NOT blocked!"
			f"\n  Stage 1 completes in ~{stage1_completion:.3f}s (fast!)"
			f"\n  Pipeline total: ~{total_time:.3f}s (limited by Stage 2)"
		)
