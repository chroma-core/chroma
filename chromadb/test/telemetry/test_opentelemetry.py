import asyncio
import inspect
import warnings

from chromadb.telemetry.opentelemetry import (
    OpenTelemetryGranularity,
    trace_method,
)


def test_trace_method_uses_inspect_for_coroutine_check() -> None:
    """trace_method should use inspect.iscoroutinefunction, not asyncio.iscoroutinefunction.

    asyncio.iscoroutinefunction is deprecated since Python 3.12 and scheduled for
    removal in Python 3.16.
    """
    with warnings.catch_warnings(record=True) as recorded:
        warnings.simplefilter("always")

        @trace_method("sync_span", OpenTelemetryGranularity.NONE)
        def sync_func() -> int:
            return 1

        @trace_method("async_span", OpenTelemetryGranularity.NONE)
        async def async_func() -> int:
            return 2

        assert sync_func() == 1
        assert inspect.iscoroutinefunction(async_func)
        assert asyncio.run(async_func()) == 2

    deprecation_warnings = [
        warning
        for warning in recorded
        if issubclass(warning.category, DeprecationWarning)
        and "iscoroutinefunction" in str(warning.message)
    ]
    assert not deprecation_warnings
