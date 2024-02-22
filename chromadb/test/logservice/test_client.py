import random
import struct
import unittest
from uuid import UUID

import grpc

from chromadb.logservice.grpc.client import GrpcLogService
from chromadb.proto.chroma_pb2 import (
    SubmitEmbeddingRecord,
    Operation,
    ScalarEncoding,
    Vector,
)
from chromadb.test.conftest import skip_if_not_cluster

test_vector_1 = [random.random() for _ in range(1000)]
test_record_1 = SubmitEmbeddingRecord(
    id="record1",
    vector=None,
    metadata=None,
    operation=None,
    collection_id="00000000-0000-0000-0000-000000000000",
)
test_record_2 = SubmitEmbeddingRecord(
    id="record2",
    vector=Vector(
        dimension=3,
        vector=struct.pack("%sf" % len(test_vector_1), *test_vector_1),
        encoding=ScalarEncoding.FLOAT32,
    ),
    metadata=None,
    operation=Operation.ADD,
    collection_id="00000000-0000-0000-0000-000000000000",
)


class LogServiceClientTest(unittest.TestCase):
    @skip_if_not_cluster()
    def test_push_logs(self) -> None:
        log_service = GrpcLogService()
        collection_id = UUID("00000000-0000-0000-0000-000000000001")
        record_count = log_service.push_logs(
            collection_id=collection_id, records=[test_record_1]
        )
        assert record_count == 1

        log_service = GrpcLogService()
        collection_id = UUID("00000000-0000-0000-0000-000000000001")
        record_count = log_service.push_logs(collection_id=collection_id, records=[])
        assert record_count == 0

        log_service = GrpcLogService()
        collection_id = UUID("00000000-0000-0000-0000-000000000002")
        record_count = log_service.push_logs(
            collection_id=collection_id, records=[test_record_1, test_record_2]
        )
        assert record_count == 2

    @skip_if_not_cluster()
    def test_push_logs_invalid_collection_id_error(self) -> None:
        with self.assertRaises(grpc.RpcError) as context:
            log_service = GrpcLogService()
            collection_id = UUID("00000000-0000-0000-0000-000000000000")
            log_service.push_logs(collection_id=collection_id, records=[])
        self.assertEqual(grpc.StatusCode.INVALID_ARGUMENT, context.exception.code())
        self.assertEqual("invalid collection_id", context.exception.details())
