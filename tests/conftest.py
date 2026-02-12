"""Shared test helpers for anyiostream tests."""

from __future__ import annotations

from collections.abc import AsyncIterator

import anyio


async def async_double(x: int) -> int:
	"""Async 1:1 transform."""
	await anyio.sleep(0)  # yield to event loop
	return x * 2


def sync_double(x: int) -> int:
	"""Sync 1:1 transform."""
	return x * 2


async def async_is_even(x: int) -> bool:
	await anyio.sleep(0)
	return x % 2 == 0


def sync_is_even(x: int) -> bool:
	return x % 2 == 0


async def async_expand(x: int) -> AsyncIterator[str]:
	"""Async 1:N transform — yields x copies of str(x)."""
	for i in range(x):
		await anyio.sleep(0)
		yield f"{x}-{i}"


def sync_expand(x: int) -> list[str]:
	"""Sync 1:N transform."""
	return [f"{x}-{i}" for i in range(x)]
