"""
Tests for MemoryBuffer integration with max_buffer_bytes parameter.

Covers basic usage, defaults, custom size functions, multi-stage,
memory limiting, type estimation, and config validation.
"""

from __future__ import annotations

from collections.abc import AsyncIterator

import anyio
import pytest

from anyiostream import Stream, pipe
from anyiostream.process import ProcessConfig

pytestmark = pytest.mark.anyio


class TestMemoryBufferIntegration:
	"""Tests for MemoryBuffer integration with max_buffer_bytes parameter."""

	async def test_max_buffer_bytes_basic(self) -> None:
		"""Test that max_buffer_bytes enables MemoryBuffer integration."""
		items = [b"x" * 1024 for _ in range(10)]

		result = await (
			Stream.from_iterable(items)
			.map(
				lambda x: x + b"y",
				max_buffer_bytes=5 * 1024,
				buffer_size=2,
			)
			.collect()
		)

		assert len(result) == 10
		assert all(len(item) == 1025 for item in result)

	async def test_max_buffer_bytes_default_value(self) -> None:
		"""Test that max_buffer_bytes defaults to 10MB (10_000_000 bytes)."""
		config = ProcessConfig()
		assert config.max_buffer_bytes == 10_000_000, (
			f"Expected default max_buffer_bytes=10_000_000, got {config.max_buffer_bytes}"
		)

		result = await Stream.from_iterable(range(10)).map(lambda x: x * 2).collect()
		assert result == [x * 2 for x in range(10)]

		result = await (
			Stream.from_iterable(["a", "b", "c"])
			.filter(lambda x: x != "b")
			.flat_map(lambda x: [x, x.upper()])
			.collect()
		)
		assert result == ["a", "A", "c", "C"]

	async def test_max_buffer_bytes_with_custom_size_func(self) -> None:
		"""Test max_buffer_bytes with custom size_func."""
		items = list(range(100))

		def custom_size(x: int) -> int:
			return 100

		result = await (
			Stream.from_iterable(items)
			.map(
				lambda x: x * 2,
				max_buffer_bytes=1000,
				size_func=custom_size,
				buffer_size=5,
			)
			.collect()
		)

		assert result == [x * 2 for x in items]

	async def test_max_buffer_bytes_multi_stage(self) -> None:
		"""Test MemoryBuffer in multi-stage pipeline."""
		items = [{"data": b"x" * 512} for _ in range(20)]

		result = await (
			Stream.from_iterable(items)
			.map(
				lambda x: {"data": x["data"] + b"y"},
				max_buffer_bytes=5 * 1024,
				buffer_size=3,
			)
			.map(
				lambda x: len(x["data"]),
				max_buffer_bytes=2 * 1024,
				buffer_size=2,
			)
			.collect()
		)

		assert len(result) == 20
		assert all(size == 513 for size in result)

	async def test_max_buffer_bytes_memory_limiting(self) -> None:
		"""Test that MemoryBuffer limits memory usage."""
		produced = []
		consumed = []

		async def slow_consumer(x: int) -> int:
			consumed.append(x)
			await anyio.sleep(0.01)
			return x

		async def fast_producer() -> AsyncIterator[bytes]:
			for i in range(50):
				item = b"x" * 1024
				produced.append(i)
				yield item

		result = await (
			Stream.from_callable(fast_producer)
			.map(
				slow_consumer,
				max_buffer_bytes=10 * 1024,
				buffer_size=5,
				workers=1,
			)
			.collect()
		)

		assert len(result) == 50

	async def test_max_buffer_bytes_default_size_estimation(self) -> None:
		"""Test MemoryBuffer's default size estimation for different types."""
		items = [
			42,
			"hello",
			b"world",
			[1, 2, 3],
			{"a": 1, "b": 2},
		]

		result = await (
			Stream.from_iterable(items)
			.map(
				lambda x: x,
				max_buffer_bytes=10 * 1024,
				buffer_size=2,
			)
			.collect()
		)

		assert result == items

	async def test_max_buffer_bytes_with_filter(self) -> None:
		"""Test MemoryBuffer with filter operation."""
		items = list(range(100))

		result = await (
			Stream.from_iterable(items)
			.filter(
				lambda x: x % 2 == 0,
				max_buffer_bytes=5 * 1024,
				buffer_size=10,
			)
			.collect()
		)

		assert result == [x for x in items if x % 2 == 0]

	async def test_max_buffer_bytes_with_flat_map(self) -> None:
		"""Test MemoryBuffer with flat_map operation."""
		items = [1, 2, 3]

		result = await (
			Stream.from_iterable(items)
			.flat_map(
				lambda x: [x, x * 10],
				max_buffer_bytes=2 * 1024,
				buffer_size=5,
			)
			.collect()
		)

		assert result == [1, 10, 2, 20, 3, 30]

	async def test_max_buffer_bytes_with_foreach(self) -> None:
		"""Test MemoryBuffer with foreach operation."""
		items = list(range(20))
		side_effects: list[int] = []

		result = await (
			Stream.from_iterable(items)
			.foreach(
				lambda x: side_effects.append(x),
				max_buffer_bytes=3 * 1024,
				buffer_size=4,
			)
			.collect()
		)

		assert result == items
		assert side_effects == items

	async def test_max_buffer_bytes_pipe_operator(self) -> None:
		"""Test MemoryBuffer with pipe operator syntax."""
		items = list(range(30))

		result = await (
			Stream.from_iterable(items)
			| pipe.map(
				lambda x: x * 2,
				max_buffer_bytes=4 * 1024,
				buffer_size=6,
			)
			| pipe.filter(
				lambda x: x < 40,
				max_buffer_bytes=3 * 1024,
				buffer_size=5,
			)
		).collect()

		expected = [x * 2 for x in items if x * 2 < 40]
		assert result == expected

	async def test_config_validation_max_buffer_bytes(self) -> None:
		"""Test that ProcessConfig validates max_buffer_bytes."""
		with pytest.raises(ValueError, match="max_buffer_bytes must be > 0"):
			ProcessConfig(max_buffer_bytes=0)

		with pytest.raises(ValueError, match="max_buffer_bytes must be > 0"):
			ProcessConfig(max_buffer_bytes=-100)
