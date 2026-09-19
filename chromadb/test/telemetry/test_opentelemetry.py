import asyncio
import inspect
import pytest
from chromadb.telemetry.opentelemetry import (
    OpenTelemetryGranularity,
    trace_method,
)


def test_trace_method_sync_function() -> None:
    @trace_method("test_sync", OpenTelemetryGranularity.OPERATION)
    def sample_sync(x: int, y: int) -> int:
        return x + y

    assert not inspect.iscoroutinefunction(sample_sync)
    assert sample_sync(2, 3) == 5


@pytest.mark.asyncio
async def test_trace_method_async_function() -> None:
    @trace_method("test_async", OpenTelemetryGranularity.OPERATION)
    async def sample_async(x: int, y: int) -> int:
        await asyncio.sleep(0.001)
        return x * y

    assert inspect.iscoroutinefunction(sample_async)
    result = await sample_async(3, 4)
    assert result == 12
