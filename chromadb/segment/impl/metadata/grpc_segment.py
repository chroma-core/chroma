from typing import Dict, List, Optional, Sequence
from chromadb.proto.utils import RetryOnRpcErrorClientInterceptor
from chromadb.segment import MetadataReader
from chromadb.config import System
from chromadb.errors import InvalidArgumentError
from chromadb.types import Segment, RequestVersionContext
from overrides import override
from chromadb.telemetry.opentelemetry import (
    OpenTelemetryGranularity,
    trace_method,
)
from chromadb.telemetry.opentelemetry.grpc import OtelInterceptor
from chromadb.types import (
    Where,
    WhereDocument,
    MetadataEmbeddingRecord,
)
from chromadb.proto.chroma_pb2_grpc import MetadataReaderStub
import chromadb.proto.chroma_pb2 as pb
import grpc


class GrpcMetadataSegment(MetadataReader):
    """Embedding Metadata segment interface"""

    _request_timeout_seconds: int
    _metadata_reader_stub: MetadataReaderStub
    _segment: Segment

    def __init__(self, system: System, segment: Segment) -> None:
        super().__init__(system, segment)  # type: ignore[safe-super]
        if not segment["metadata"] or not segment["metadata"]["grpc_url"]:
            raise Exception("Missing grpc_url in segment metadata")

        self._segment = segment
        self._request_timeout_seconds = system.settings.require(
            "chroma_query_request_timeout_seconds"
        )

    @override
    def start(self) -> None:
        if not self._segment["metadata"] or not self._segment["metadata"]["grpc_url"]:
            raise Exception("Missing grpc_url in segment metadata")

        channel = grpc.insecure_channel(self._segment["metadata"]["grpc_url"])
        interceptors = [OtelInterceptor(), RetryOnRpcErrorClientInterceptor()]
        channel = grpc.intercept_channel(channel, *interceptors)
        self._metadata_reader_stub = MetadataReaderStub(channel)  # type: ignore

    @override
    def count(self, request_version_context: RequestVersionContext) -> int:
        request: pb.CountRecordsRequest = pb.CountRecordsRequest(
            segment_id=self._segment["id"].hex,
            collection_id=self._segment["collection"].hex,
        )
        response: pb.CountRecordsResponse = self._metadata_reader_stub.CountRecords(
            request, timeout=self._request_timeout_seconds
        )
        return response.count

    @override
    def delete(self, where: Optional[Where] = None) -> None:
        raise NotImplementedError()

    @override
    def max_seqid(self) -> int:
        raise NotImplementedError()

    @trace_method(
        "GrpcMetadataSegment.get_metadata",
        OpenTelemetryGranularity.ALL,
    )
    @override
    def get_metadata(
        self,
        request_version_context: RequestVersionContext,
        where: Optional[Where] = None,
        where_document: Optional[WhereDocument] = None,
        ids: Optional[Sequence[str]] = None,
        limit: Optional[int] = None,
        offset: Optional[int] = None,
        include_metadata: bool = True,
    ) -> Sequence[MetadataEmbeddingRecord]:
        """Query for embedding metadata."""

        if limit is not None and limit < 0:
            raise InvalidArgumentError(f"Limit cannot be negative: {limit}")

        if offset is not None and offset < 0:
            raise InvalidArgumentError(f"Offset cannot be negative: {offset}")

        request: pb.QueryMetadataRequest = pb.QueryMetadataRequest(
            segment_id=self._segment["id"].hex,
            collection_id=self._segment["collection"].hex,
            where=self._where_to_proto(where)
            if where is not None and len(where) > 0
            else None,
            where_document=(
                self._where_document_to_proto(where_document)
                if where_document is not None and len(where_document) > 0
                else None
            ),
            ids=pb.UserIds(ids=ids) if ids is not None else None,
            limit=limit,
            offset=offset,
            include_metadata=include_metadata,
        )

        response: pb.QueryMetadataResponse = self._metadata_reader_stub.QueryMetadata(
            request, timeout=self._request_timeout_seconds
        )
        results: List[MetadataEmbeddingRecord] = []
        for record in response.records:
            result = self._from_proto(record)
            results.append(result)

        return results

    def _where_to_proto(self, where: Optional[Where]) -> pb.Where:
        response = pb.Where()
        if where is None:
            return response
        if len(where) != 1:
            raise ValueError(
                f"Expected where to have exactly one operator, got {where}"
            )

        for key, value in where.items():
            if not isinstance(key, str):
                raise ValueError(f"Expected where key to be a str, got {key}")

            if key == "$and" or key == "$or":
                if not isinstance(value, list):
                    raise ValueError(
                        f"Expected where value for $and or $or to be a list of where expressions, got {value}"
                    )
                children: pb.WhereChildren = pb.WhereChildren(
                    children=[self._where_to_proto(w) for w in value]
                )
                if key == "$and":
                    children.operator = pb.BooleanOperator.AND
                else:
                    children.operator = pb.BooleanOperator.OR

                response.children.CopyFrom(children)
                return response

            # At this point we know we're at a direct comparison. It can either
            # be of the form {"key": "value"} or {"key": {"$operator": "value"}}.

            dc = pb.DirectComparison()
            dc.key = key

            if not isinstance(value, dict):
                # {'key': 'value'} case
                if type(value) is str:
                    ssc = pb.SingleStringComparison()
                    ssc.value = value
                    ssc.comparator = pb.GenericComparator.EQ
                    dc.single_string_operand.CopyFrom(ssc)
                elif type(value) is bool:
                    sbc = pb.SingleBoolComparison()
                    sbc.value = value
                    sbc.comparator = pb.GenericComparator.EQ
                    dc.single_bool_operand.CopyFrom(sbc)
                elif type(value) is int:
                    sic = pb.SingleIntComparison()
                    sic.value = value
                    sic.generic_comparator = pb.GenericComparator.EQ
                    dc.single_int_operand.CopyFrom(sic)
                elif type(value) is float:
                    sdc = pb.SingleDoubleComparison()
                    sdc.value = value
                    sdc.generic_comparator = pb.GenericComparator.EQ
                    dc.single_double_operand.CopyFrom(sdc)
                else:
                    raise ValueError(
                        f"Expected where value to be a string, int, or float, got {value}"
                    )
            else:
                for operator, operand in value.items():
                    if operator in ["$in", "$nin"]:
                        if not isinstance(operand, list):
                            raise ValueError(
                                f"Expected where value for $in or $nin to be a list of values, got {value}"
                            )
                        if len(operand) == 0 or not all(
                            isinstance(x, type(operand[0])) for x in operand
                        ):
                            raise ValueError(
                                f"Expected where operand value to be a non-empty list, and all values to be of the same type "
                                f"got {operand}"
                            )
                        list_operator = None
                        if operator == "$in":
                            list_operator = pb.ListOperator.IN
                        else:
                            list_operator = pb.ListOperator.NIN
                        if type(operand[0]) is str:
                            slo = pb.StringListComparison()
                            for x in operand:
                                slo.values.extend([x])  # type: ignore
                            slo.list_operator = list_operator
                            dc.string_list_operand.CopyFrom(slo)
                        elif type(operand[0]) is bool:
                            blo = pb.BoolListComparison()
                            for x in operand:
                                blo.values.extend([x])  # type: ignore
                            blo.list_operator = list_operator
                            dc.bool_list_operand.CopyFrom(blo)
                        elif type(operand[0]) is int:
                            ilo = pb.IntListComparison()
                            for x in operand:
                                ilo.values.extend([x])  # type: ignore
                            ilo.list_operator = list_operator
                            dc.int_list_operand.CopyFrom(ilo)
                        elif type(operand[0]) is float:
                            dlo = pb.DoubleListComparison()
                            for x in operand:
                                dlo.values.extend([x])  # type: ignore
                            dlo.list_operator = list_operator
                            dc.double_list_operand.CopyFrom(dlo)
                        else:
                            raise ValueError(
                                f"Expected where operand value to be a list of strings, ints, or floats, got {operand}"
                            )
                    elif operator in ["$eq", "$ne", "$gt", "$lt", "$gte", "$lte"]:
                        # Direct comparison to a single value.
                        if type(operand) is str:
                            ssc = pb.SingleStringComparison()
                            ssc.value = operand
                            if operator == "$eq":
                                ssc.comparator = pb.GenericComparator.EQ
                            elif operator == "$ne":
                                ssc.comparator = pb.GenericComparator.NE
                            else:
                                raise ValueError(
                                    f"Expected where operator to be $eq or $ne, got {operator}"
                                )
                            dc.single_string_operand.CopyFrom(ssc)
                        elif type(operand) is bool:
                            sbc = pb.SingleBoolComparison()
                            sbc.value = operand
                            if operator == "$eq":
                                sbc.comparator = pb.GenericComparator.EQ
                            elif operator == "$ne":
                                sbc.comparator = pb.GenericComparator.NE
                            else:
                                raise ValueError(
                                    f"Expected where operator to be $eq or $ne, got {operator}"
                                )
                            dc.single_bool_operand.CopyFrom(sbc)
                        elif type(operand) is int:
                            sic = pb.SingleIntComparison()
                            sic.value = operand
                            if operator == "$eq":
                                sic.generic_comparator = pb.GenericComparator.EQ
                            elif operator == "$ne":
                                sic.generic_comparator = pb.GenericComparator.NE
                            elif operator == "$gt":
                                sic.number_comparator = pb.NumberComparator.GT
                            elif operator == "$lt":
                                sic.number_comparator = pb.NumberComparator.LT
                            elif operator == "$gte":
                                sic.number_comparator = pb.NumberComparator.GTE
                            elif operator == "$lte":
                                sic.number_comparator = pb.NumberComparator.LTE
                            else:
                                raise ValueError(
                                    f"Expected where operator to be one of $eq, $ne, $gt, $lt, $gte, $lte, got {operator}"
                                )
                            dc.single_int_operand.CopyFrom(sic)
                        elif type(operand) is float:
                            sfc = pb.SingleDoubleComparison()
                            sfc.value = operand
                            if operator == "$eq":
                                sfc.generic_comparator = pb.GenericComparator.EQ
                            elif operator == "$ne":
                                sfc.generic_comparator = pb.GenericComparator.NE
                            elif operator == "$gt":
                                sfc.number_comparator = pb.NumberComparator.GT
                            elif operator == "$lt":
                                sfc.number_comparator = pb.NumberComparator.LT
                            elif operator == "$gte":
                                sfc.number_comparator = pb.NumberComparator.GTE
                            elif operator == "$lte":
                                sfc.number_comparator = pb.NumberComparator.LTE
                            else:
                                raise ValueError(
                                    f"Expected where operator to be one of $eq, $ne, $gt, $lt, $gte, $lte, got {operator}"
                                )
                            dc.single_double_operand.CopyFrom(sfc)
                        else:
                            raise ValueError(
                                f"Expected where operand value to be a string, int, or float, got {operand}"
                            )
                    else:
                        # This case should never happen, as we've already
                        # handled the case for direct comparisons.
                        pass

            response.direct_comparison.CopyFrom(dc)
        return response

    def _where_document_to_proto(
        self, where_document: Optional[WhereDocument]
    ) -> pb.WhereDocument:
        response = pb.WhereDocument()
        if where_document is None:
            return response
        if len(where_document) != 1:
            raise ValueError(
                f"Expected where_document to have exactly one operator, got {where_document}"
            )

        for operator, operand in where_document.items():
            if operator == "$and" or operator == "$or":
                # Nested "$and" or "$or" expression.
                if not isinstance(operand, list):
                    raise ValueError(
                        f"Expected where_document value for $and or $or to be a list of where_document expressions, got {operand}"
                    )
                children: pb.WhereDocumentChildren = pb.WhereDocumentChildren(
                    children=[self._where_document_to_proto(w) for w in operand]
                )
                if operator == "$and":
                    children.operator = pb.BooleanOperator.AND
                else:
                    children.operator = pb.BooleanOperator.OR

                response.children.CopyFrom(children)
            else:
                # Direct "$contains" or "$not_contains" comparison to a single
                # value.
                if not isinstance(operand, str):
                    raise ValueError(
                        f"Expected where_document operand to be a string, got {operand}"
                    )
                dwd = pb.DirectWhereDocument()
                dwd.document = operand
                if operator == "$contains":
                    dwd.operator = pb.WhereDocumentOperator.CONTAINS
                elif operator == "$not_contains":
                    dwd.operator = pb.WhereDocumentOperator.NOT_CONTAINS
                else:
                    raise ValueError(
                        f"Expected where_document operator to be one of $contains, $not_contains, got {operator}"
                    )
                response.direct.CopyFrom(dwd)

        return response

    def _from_proto(
        self, record: pb.MetadataEmbeddingRecord
    ) -> MetadataEmbeddingRecord:
        translated_metadata: Dict[str, str | int | float | bool] = {}
        record_metadata_map = record.metadata.metadata
        for key, value in record_metadata_map.items():
            if value.HasField("bool_value"):
                translated_metadata[key] = value.bool_value
            elif value.HasField("string_value"):
                translated_metadata[key] = value.string_value
            elif value.HasField("int_value"):
                translated_metadata[key] = value.int_value
            elif value.HasField("float_value"):
                translated_metadata[key] = value.float_value
            else:
                raise ValueError(f"Unknown metadata value type: {value}")

        mer = MetadataEmbeddingRecord(id=record.id, metadata=translated_metadata)

        return mer
