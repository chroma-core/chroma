import grpc

from chromadb.proto.utils import RetryOnRpcErrorClientInterceptor


def test_default_retryable_status_codes_are_not_shared() -> None:
    first = RetryOnRpcErrorClientInterceptor()
    second = RetryOnRpcErrorClientInterceptor()

    first.retryable_status_codes.add(grpc.StatusCode.CANCELLED)

    assert second.retryable_status_codes == {
        grpc.StatusCode.UNAVAILABLE,
        grpc.StatusCode.UNKNOWN,
    }
