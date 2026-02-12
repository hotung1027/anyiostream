"""
Tests for individual stage operations — map, flat_map, filter, foreach.

Covers sync/async variants, chaining, multi-worker, and edge cases.
"""

from __future__ import annotations

import anyio
import pytest

from anyiostream import Stream
from tests.conftest import (
	async_double,
	async_expand,
	async_is_even,
	sync_double,
	sync_expand,
	sync_is_even,
)

pytestmark = pytest.mark.anyio


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
