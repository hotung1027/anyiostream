"""
Tests for Result-aware pipeline stages.

Covers Ok/Err types, try_map (with err= param), try_flat_map, try_filter,
try_foreach, recover, ok_only, errors_only, collect_split, pipe operator
syntax, backward compatibility, and concurrent Result processing.
"""

from __future__ import annotations

import pytest

from anyiostream import Err, Ok, PipelineError, Stream, pipe

# =========================================================================
# Result types
# =========================================================================
# Mark @pytest.mark.anyio on all test functions in the module
pytestmark = pytest.mark.anyio


class TestResultTypes:
	"""Test Ok/Err construction and methods."""

	def test_ok_value(self) -> None:
		ok = Ok(42)
		assert ok.value == 42
		assert ok.is_ok()
		assert not ok.is_err()

	def test_ok_unwrap(self) -> None:
		assert Ok(42).unwrap() == 42

	def test_ok_unwrap_or(self) -> None:
		assert Ok(42).unwrap_or(0) == 42

	def test_ok_unwrap_err_raises(self) -> None:
		with pytest.raises(ValueError, match="Called unwrap_err on Ok"):
			Ok(42).unwrap_err()

	def test_ok_map(self) -> None:
		result = Ok(10).map(lambda x: x * 2)
		assert isinstance(result, Ok)
		assert result.value == 20

	def test_ok_map_err_noop(self) -> None:
		result = Ok(10).map_err(lambda e: "transformed")
		assert isinstance(result, Ok)
		assert result.value == 10

	def test_err_error(self) -> None:
		err = Err("boom")
		assert err.error == "boom"
		assert err.is_err()
		assert not err.is_ok()

	def test_err_unwrap_raises(self) -> None:
		with pytest.raises(ValueError, match="Called unwrap on Err"):
			Err("boom").unwrap()

	def test_err_unwrap_or(self) -> None:
		assert Err("boom").unwrap_or(99) == 99

	def test_err_unwrap_err(self) -> None:
		assert Err("boom").unwrap_err() == "boom"

	def test_err_map_noop(self) -> None:
		result = Err("boom").map(lambda x: x * 2)
		assert isinstance(result, Err)
		assert result.error == "boom"

	def test_err_map_err(self) -> None:
		result = Err("boom").map_err(lambda e: e.upper())
		assert isinstance(result, Err)
		assert result.error == "BOOM"

	def test_ok_frozen(self) -> None:
		ok = Ok(42)
		with pytest.raises(AttributeError):
			ok.value = 99  # type: ignore[misc]

	def test_err_frozen(self) -> None:
		err = Err("boom")
		with pytest.raises(AttributeError):
			err.error = "other"  # type: ignore[misc]

	def test_match_case_ok(self) -> None:
		item: Ok[int] | Err[str] = Ok(42)
		match item:
			case Ok(value=v):
				assert v == 42
			case Err():
				pytest.fail("Should not match Err")

	def test_match_case_err(self) -> None:
		item: Ok[int] | Err[str] = Err("boom")
		match item:
			case Ok():
				pytest.fail("Should not match Ok")
			case Err(error=e):
				assert e == "boom"


class TestPipelineError:
	"""Test PipelineError context."""

	def test_str_with_stage(self) -> None:
		err = PipelineError(exception=ValueError("x"), stage="fetch")
		assert str(err) == "[fetch] x"

	def test_str_without_stage(self) -> None:
		err = PipelineError(exception=ValueError("x"))
		assert str(err) == "x"

	def test_fields(self) -> None:
		exc = ValueError("test")
		err = PipelineError(exception=exc, item="input", stage="s1", traceback="tb")
		assert err.exception is exc
		assert err.item == "input"
		assert err.stage == "s1"
		assert err.traceback == "tb"


# =========================================================================
# try_map
# =========================================================================


class TestTryMap:
	"""Test try_map: raw T -> Result[U, PipelineError], with err= param."""

	@pytest.mark.anyio
	async def test_all_success(self) -> None:
		result = (
			await Stream.from_iterable([1, 2, 3]).try_map(lambda x: x * 10).collect()
		)
		assert len(result) == 3
		assert all(isinstance(r, Ok) for r in result)
		assert sorted(r.unwrap() for r in result) == [10, 20, 30]

	@pytest.mark.anyio
	async def test_one_error(self) -> None:
		def risky(x: int) -> int:
			if x == 2:
				raise ValueError("boom")
			return x * 10

		result = await Stream.from_iterable([1, 2, 3]).try_map(risky).collect()
		assert len(result) == 3

		oks = [r for r in result if isinstance(r, Ok)]
		errs = [r for r in result if isinstance(r, Err)]
		assert len(errs) == 1
		assert sorted(r.unwrap() for r in oks) == [10, 30]

		err = errs[0].error
		assert isinstance(err, PipelineError)
		assert isinstance(err.exception, ValueError)
		assert err.item == 2
		assert err.stage == "risky"

	@pytest.mark.anyio
	async def test_all_error(self) -> None:
		def boom(x: int) -> int:
			raise RuntimeError(f"fail-{x}")

		result = await Stream.from_iterable([1, 2]).try_map(boom).collect()
		assert all(isinstance(r, Err) for r in result)
		assert len(result) == 2

	@pytest.mark.anyio
	async def test_async_func(self) -> None:
		async def double(x: int) -> int:
			return x * 2

		result = await Stream.from_iterable([5]).try_map(double).collect()
		assert len(result) == 1
		assert result[0].unwrap() == 10

	@pytest.mark.anyio
	async def test_async_func_error(self) -> None:
		async def fail(x: int) -> int:
			raise TypeError("async boom")

		result = await Stream.from_iterable([1]).try_map(fail).collect()
		assert len(result) == 1
		assert isinstance(result[0], Err)
		assert isinstance(result[0].error.exception, TypeError)

	@pytest.mark.anyio
	async def test_err_handler(self) -> None:
		"""try_map with err= transforms Err items."""

		def risky(x: int) -> int:
			if x == 2:
				raise ValueError("boom")
			return x * 10

		result = await (
			Stream.from_iterable([1, 2, 3])
			.try_map(risky)
			.try_map(lambda x: x + 1, err=lambda e: f"handled: {e.stage}")
			.collect()
		)
		oks = [r for r in result if isinstance(r, Ok)]
		errs = [r for r in result if isinstance(r, Err)]
		assert sorted(r.unwrap() for r in oks) == [11, 31]
		assert len(errs) == 1
		assert errs[0].error == "handled: risky"

	@pytest.mark.anyio
	async def test_err_passes_through(self) -> None:
		"""Without err=, Err items pass through unchanged."""

		def risky(x: int) -> int:
			if x == 2:
				raise ValueError("boom")
			return x * 10

		result = await (
			Stream.from_iterable([1, 2, 3])
			.try_map(risky)
			.try_map(lambda x: x + 1)
			.collect()
		)
		oks = [r for r in result if isinstance(r, Ok)]
		errs = [r for r in result if isinstance(r, Err)]
		assert sorted(r.unwrap() for r in oks) == [11, 31]
		assert len(errs) == 1
		assert errs[0].error.stage == "risky"  # original error preserved

	@pytest.mark.anyio
	async def test_new_error_in_try_map(self) -> None:
		"""If try_map's func raises, the item becomes a new Err."""

		def second_fail(x: int) -> int:
			if x == 30:
				raise RuntimeError("second stage fail")
			return x + 1

		result = await (
			Stream.from_iterable([1, 2, 3])
			.try_map(lambda x: x * 10)
			.try_map(second_fail)
			.collect()
		)
		oks = [r for r in result if isinstance(r, Ok)]
		errs = [r for r in result if isinstance(r, Err)]
		assert sorted(r.unwrap() for r in oks) == [11, 21]
		assert len(errs) == 1
		assert isinstance(errs[0].error.exception, RuntimeError)
		assert errs[0].error.item == 30  # the unwrapped Ok value


# =========================================================================
# try_flat_map
# =========================================================================


class TestTryFlatMap:
	"""Test try_flat_map: each sub-item wrapped as Ok, exception -> Err."""

	@pytest.mark.anyio
	async def test_success(self) -> None:
		result = await (
			Stream.from_iterable([1, 2]).try_flat_map(lambda x: [x, x * 10]).collect()
		)
		assert all(isinstance(r, Ok) for r in result)
		assert sorted(r.unwrap() for r in result) == [1, 2, 10, 20]

	@pytest.mark.anyio
	async def test_error(self) -> None:
		def explode(x: int) -> list[int]:
			if x == 2:
				raise ValueError("flat boom")
			return [x, x * 10]

		result = await Stream.from_iterable([1, 2]).try_flat_map(explode).collect()
		oks = [r for r in result if isinstance(r, Ok)]
		errs = [r for r in result if isinstance(r, Err)]
		assert sorted(r.unwrap() for r in oks) == [1, 10]
		assert len(errs) == 1
		assert errs[0].error.item == 2

	@pytest.mark.anyio
	async def test_err_handler(self) -> None:
		"""try_flat_map with err= transforms Err items."""

		def explode(x: int) -> list[int]:
			if x == 2:
				raise ValueError("flat boom")
			return [x, x * 10]

		result = await (
			Stream.from_iterable([1, 2])
			.try_flat_map(explode)
			.try_flat_map(lambda x: [x, x + 100], err=lambda e: f"err:{e.item}")
			.collect()
		)
		oks = [r for r in result if isinstance(r, Ok)]
		errs = [r for r in result if isinstance(r, Err)]
		assert sorted(r.unwrap() for r in oks) == [1, 10, 101, 110]
		assert len(errs) == 1
		assert errs[0].error == "err:2"


# =========================================================================
# try_filter
# =========================================================================


class TestTryFilter:
	"""Test try_filter: filter Ok values, Err always passes through."""

	@pytest.mark.anyio
	async def test_filters_ok(self) -> None:
		result = await (
			Stream.from_iterable([1, 2, 3, 4])
			.try_map(lambda x: x)
			.try_filter(lambda x: x % 2 == 0)
			.collect()
		)
		oks = [r for r in result if isinstance(r, Ok)]
		assert sorted(r.unwrap() for r in oks) == [2, 4]

	@pytest.mark.anyio
	async def test_err_always_passes(self) -> None:
		def risky(x: int) -> int:
			if x == 3:
				raise ValueError("boom")
			return x

		result = await (
			Stream.from_iterable([1, 2, 3, 4])
			.try_map(risky)
			.try_filter(lambda x: x % 2 == 0)
			.collect()
		)
		oks = [r for r in result if isinstance(r, Ok)]
		errs = [r for r in result if isinstance(r, Err)]
		assert sorted(r.unwrap() for r in oks) == [2, 4]
		assert len(errs) == 1  # Err for x=3 still passes through


# =========================================================================
# try_foreach
# =========================================================================


class TestTryForeach:
	"""Test try_foreach: side-effect on Ok values, with err= for Err."""

	@pytest.mark.anyio
	async def test_side_effect(self) -> None:
		seen: list[int] = []

		result = await (
			Stream.from_iterable([1, 2, 3])
			.try_map(lambda x: x)
			.try_foreach(lambda x: seen.append(x))
			.collect()
		)
		assert sorted(seen) == [1, 2, 3]
		assert len(result) == 3

	@pytest.mark.anyio
	async def test_err_not_inspected(self) -> None:
		seen: list[int] = []

		def risky(x: int) -> int:
			if x == 2:
				raise ValueError("boom")
			return x

		result = await (
			Stream.from_iterable([1, 2, 3])
			.try_map(risky)
			.try_foreach(lambda x: seen.append(x))
			.collect()
		)
		assert sorted(seen) == [1, 3]
		assert len(result) == 3  # all 3 items still flow through

	@pytest.mark.anyio
	async def test_err_handler(self) -> None:
		"""try_foreach with err= calls handler on Err items."""
		seen_ok: list[int] = []
		seen_err: list[str] = []

		def risky(x: int) -> int:
			if x == 2:
				raise ValueError("boom")
			return x

		result = await (
			Stream.from_iterable([1, 2, 3])
			.try_map(risky)
			.try_foreach(
				lambda x: seen_ok.append(x),
				err=lambda e: seen_err.append(f"err:{e.stage}"),
			)
			.collect()
		)
		assert sorted(seen_ok) == [1, 3]
		assert seen_err == ["err:risky"]
		assert len(result) == 3


# =========================================================================
# recover
# =========================================================================


class TestRecover:
	"""Test recover: Err -> value via func, Ok -> unwrapped."""

	@pytest.mark.anyio
	async def test_recover_all(self) -> None:
		def risky(x: int) -> int:
			if x == 2:
				raise ValueError("boom")
			return x * 10

		result = await (
			Stream.from_iterable([1, 2, 3])
			.try_map(risky)
			.recover(lambda e: -1)
			.collect()
		)
		assert sorted(result) == [-1, 10, 30]

	@pytest.mark.anyio
	async def test_recover_uses_error_context(self) -> None:
		def risky(x: int) -> int:
			if x == 2:
				raise ValueError("boom")
			return x * 10

		result = await (
			Stream.from_iterable([1, 2, 3])
			.try_map(risky)
			.recover(lambda e: e.item * 100)
			.collect()
		)
		assert sorted(result) == [10, 30, 200]


# =========================================================================
# ok_only / errors_only
# =========================================================================


class TestOkOnly:
	"""Test ok_only: keep Ok values unwrapped, drop Err."""

	@pytest.mark.anyio
	async def test_ok_only(self) -> None:
		def risky(x: int) -> int:
			if x == 2:
				raise ValueError("boom")
			return x * 10

		result = await (
			Stream.from_iterable([1, 2, 3]).try_map(risky).ok_only().collect()
		)
		assert sorted(result) == [10, 30]
		assert all(not isinstance(r, (Ok, Err)) for r in result)


class TestErrorsOnly:
	"""Test errors_only: keep Err values unwrapped, drop Ok."""

	@pytest.mark.anyio
	async def test_errors_only(self) -> None:
		def risky(x: int) -> int:
			if x == 2:
				raise ValueError("boom")
			return x * 10

		result = await (
			Stream.from_iterable([1, 2, 3]).try_map(risky).errors_only().collect()
		)
		assert len(result) == 1
		assert isinstance(result[0], PipelineError)
		assert result[0].item == 2


# =========================================================================
# collect_split
# =========================================================================


class TestCollectSplit:
	"""Test collect_split: partition into (oks, errs)."""

	@pytest.mark.anyio
	async def test_split(self) -> None:
		def risky(x: int) -> int:
			if x == 2:
				raise ValueError("boom")
			return x * 10

		oks, errs = await Stream.from_iterable([1, 2, 3]).try_map(risky).collect_split()
		assert sorted(oks) == [10, 30]
		assert len(errs) == 1
		assert isinstance(errs[0], PipelineError)

	@pytest.mark.anyio
	async def test_all_ok(self) -> None:
		oks, errs = await (
			Stream.from_iterable([1, 2]).try_map(lambda x: x).collect_split()
		)
		assert sorted(oks) == [1, 2]
		assert errs == []

	@pytest.mark.anyio
	async def test_all_err(self) -> None:
		def boom(x: int) -> int:
			raise RuntimeError("fail")

		oks, errs = await Stream.from_iterable([1, 2]).try_map(boom).collect_split()
		assert oks == []
		assert len(errs) == 2


# =========================================================================
# Pipe operator with Result
# =========================================================================


class TestPipeOperatorResult:
	"""Test Result operations via pipe | syntax."""

	@pytest.mark.anyio
	async def test_try_map_pipe(self) -> None:
		result = await (
			Stream.from_iterable([1, 2, 3])
			| pipe.try_map(lambda x: x * 10)
			| pipe.collect()
		)
		assert all(isinstance(r, Ok) for r in result)
		assert sorted(r.unwrap() for r in result) == [10, 20, 30]

	@pytest.mark.anyio
	async def test_full_pipe_chain(self) -> None:
		def risky(x: int) -> int:
			if x == 2:
				raise ValueError("boom")
			return x * 10

		result = await (
			Stream.from_iterable([1, 2, 3])
			| pipe.try_map(risky)
			| pipe.try_map(lambda x: x + 1)
			| pipe.try_filter(lambda x: x > 15)
			| pipe.collect()
		)
		oks = [r for r in result if isinstance(r, Ok)]
		errs = [r for r in result if isinstance(r, Err)]
		assert sorted(r.unwrap() for r in oks) == [31]
		assert len(errs) == 1

	@pytest.mark.anyio
	async def test_pipe_collect_split(self) -> None:
		def risky(x: int) -> int:
			if x == 2:
				raise ValueError("boom")
			return x * 10

		oks, errs = await (
			Stream.from_iterable([1, 2, 3]) | pipe.try_map(risky) | pipe.collect_split()
		)
		assert sorted(oks) == [10, 30]
		assert len(errs) == 1

	@pytest.mark.anyio
	async def test_pipe_ok_only(self) -> None:
		def risky(x: int) -> int:
			if x == 2:
				raise ValueError("boom")
			return x * 10

		result = await (
			Stream.from_iterable([1, 2, 3])
			| pipe.try_map(risky)
			| pipe.ok_only()
			| pipe.collect()
		)
		assert sorted(result) == [10, 30]

	@pytest.mark.anyio
	async def test_pipe_errors_only(self) -> None:
		def risky(x: int) -> int:
			if x == 2:
				raise ValueError("boom")
			return x * 10

		result = await (
			Stream.from_iterable([1, 2, 3])
			| pipe.try_map(risky)
			| pipe.errors_only()
			| pipe.collect()
		)
		assert len(result) == 1
		assert isinstance(result[0], PipelineError)

	@pytest.mark.anyio
	async def test_pipe_recover(self) -> None:
		def risky(x: int) -> int:
			if x == 2:
				raise ValueError("boom")
			return x * 10

		result = await (
			Stream.from_iterable([1, 2, 3])
			| pipe.try_map(risky)
			| pipe.recover(lambda e: -1)
			| pipe.collect()
		)
		assert sorted(result) == [-1, 10, 30]

	@pytest.mark.anyio
	async def test_pipe_try_map_with_err(self) -> None:
		def risky(x: int) -> int:
			if x == 2:
				raise ValueError("boom")
			return x * 10

		result = await (
			Stream.from_iterable([1, 2, 3])
			| pipe.try_map(risky)
			| pipe.try_map(lambda x: x + 1, err=lambda e: f"err:{e.stage}")
			| pipe.collect()
		)
		errs = [r for r in result if isinstance(r, Err)]
		assert len(errs) == 1
		assert errs[0].error == "err:risky"

	@pytest.mark.anyio
	async def test_pipe_try_foreach(self) -> None:
		seen: list[int] = []

		result = await (
			Stream.from_iterable([1, 2, 3])
			| pipe.try_map(lambda x: x)
			| pipe.try_foreach(lambda x: seen.append(x))
			| pipe.collect()
		)
		assert sorted(seen) == [1, 2, 3]
		assert len(result) == 3

	@pytest.mark.anyio
	async def test_pipe_try_flat_map(self) -> None:
		result = await (
			Stream.from_iterable([1, 2])
			| pipe.try_flat_map(lambda x: [x, x * 10])
			| pipe.collect()
		)
		assert all(isinstance(r, Ok) for r in result)
		assert sorted(r.unwrap() for r in result) == [1, 2, 10, 20]

	@pytest.mark.anyio
	async def test_pipe_try_filter(self) -> None:
		def risky(x: int) -> int:
			if x == 3:
				raise ValueError("boom")
			return x

		result = await (
			Stream.from_iterable([1, 2, 3, 4])
			| pipe.try_map(risky)
			| pipe.try_filter(lambda x: x % 2 == 0)
			| pipe.collect()
		)
		oks = [r for r in result if isinstance(r, Ok)]
		errs = [r for r in result if isinstance(r, Err)]
		assert sorted(r.unwrap() for r in oks) == [2, 4]
		assert len(errs) == 1


# =========================================================================
# Backward compatibility
# =========================================================================


class TestBackwardCompat:
	"""Verify existing non-Result pipeline behavior is unchanged."""

	@pytest.mark.anyio
	async def test_map_error_still_skips(self) -> None:
		"""Original map() should still skip items on error."""

		def risky(x: int) -> int:
			if x == 2:
				raise ValueError("boom")
			return x * 10

		result = await Stream.from_iterable([1, 2, 3]).map(risky).collect()
		assert sorted(result) == [10, 30]

	@pytest.mark.anyio
	async def test_filter_error_still_skips(self) -> None:
		def risky_pred(x: int) -> bool:
			if x == 3:
				raise ValueError("boom")
			return x % 2 == 0

		result = await Stream.from_iterable([1, 2, 3, 4]).filter(risky_pred).collect()
		assert sorted(result) == [2, 4]

	@pytest.mark.anyio
	async def test_plain_map_no_result_wrapping(self) -> None:
		"""Regular map should return plain values, not wrapped in Ok."""
		result = await Stream.from_iterable([1, 2]).map(lambda x: x * 10).collect()
		assert result == [10, 20]
		assert not isinstance(result[0], Ok)


# =========================================================================
# Concurrent Result processing
# =========================================================================


class TestConcurrentResult:
	"""Test Result-mode with multiple workers."""

	@pytest.mark.anyio
	async def test_try_map_workers(self) -> None:
		def risky(x: int) -> int:
			if x == 5:
				raise ValueError("boom")
			return x * 10

		result = await (
			Stream.from_iterable(range(10)).try_map(risky, workers=3).collect()
		)
		assert len(result) == 10
		oks = [r for r in result if isinstance(r, Ok)]
		errs = [r for r in result if isinstance(r, Err)]
		assert len(oks) == 9
		assert len(errs) == 1
		assert sorted(r.unwrap() for r in oks) == [0, 10, 20, 30, 40, 60, 70, 80, 90]

	@pytest.mark.anyio
	async def test_try_map_with_err_workers(self) -> None:
		def risky(x: int) -> int:
			if x == 3:
				raise ValueError("boom")
			return x

		result = await (
			Stream.from_iterable(range(5))
			.try_map(risky, workers=2)
			.try_map(lambda x: x * 100, workers=2)
			.collect()
		)
		oks = [r for r in result if isinstance(r, Ok)]
		errs = [r for r in result if isinstance(r, Err)]
		assert sorted(r.unwrap() for r in oks) == [0, 100, 200, 400]
		assert len(errs) == 1

	@pytest.mark.anyio
	async def test_chained_result_pipeline_workers(self) -> None:
		"""Full multi-stage Result pipeline with concurrency."""

		def step1(x: int) -> int:
			if x == 5:
				raise ValueError("step1 fail")
			return x * 2

		def step2(x: int) -> int:
			if x == 8:
				raise RuntimeError("step2 fail")
			return x + 1

		result = await (
			Stream.from_iterable(range(10))
			.try_map(step1, workers=3)
			.try_map(step2, workers=2)
			.collect()
		)
		oks = [r for r in result if isinstance(r, Ok)]
		errs = [r for r in result if isinstance(r, Err)]
		# x=5 fails at step1, x=4 (produces 8) fails at step2
		assert len(errs) == 2
		assert len(oks) == 8
