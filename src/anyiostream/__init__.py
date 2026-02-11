"""
anyiostream — Composable async pipelines with structured concurrency.

Built on anyio memory object streams + task groups for true inter-stage
concurrency with backpressure, inspired by Rust Tokio's channel semantics
and aiostream's composable pipe API.

Usage::

    from anyiostream import Stream, pipe

    # Compose a pipeline with pipe syntax
    result = await (
        Stream.from_iterable(urls)
        | pipe.map(fetch, workers=5)
        | pipe.flat_map(parse_html, workers=3)
        | pipe.filter(is_valid)
        | pipe.map(embed, workers=2)
        | pipe.collect()
    )

    # Or use the builder pattern
    pipeline = (
        Stream.from_iterable(urls)
        .map(fetch, workers=5)
        .flat_map(parse_html, workers=3)
        .filter(is_valid)
        .map(embed, workers=2)
    )
    result = await pipeline.collect()

Features:
    - True inter-stage concurrency (each process runs in its own task group)
    - Backpressure via bounded memory object streams
    - Fan-out workers per process via stream cloning
    - Structured concurrency with automatic cleanup
    - Type-safe generic pipeline processes
    - Backend-portable: runs on both asyncio and trio
    - Rust-inspired Ok/Err Result type for error handling
"""

from anyiostream.operators import pipe
from anyiostream.process import Process, ProcessConfig
from anyiostream.result import Err, Ok, PipelineError, Result
from anyiostream.stream import Stream

__all__ = [
    "Stream",
    "pipe",
    "Process",
    "ProcessConfig",
    "Ok",
    "Err",
    "Result",
    "PipelineError",
]
