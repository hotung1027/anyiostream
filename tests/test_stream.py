"""
Tests for Stream construction, terminal operations, and pipe operator syntax.

Covers:
	- Stream construction (from_iterable, from_callable)
	- Terminal operations (collect, count, reduce, first, take)
	- Pipe operator syntax
"""

from __future__ import annotations

import pytest

from anyiostream import Stream, pipe
from tests.conftest import sync_double, sync_expand, sync_is_even

pytestmark = pytest.mark.anyio


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
