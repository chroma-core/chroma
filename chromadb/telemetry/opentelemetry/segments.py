def register_segment_manager_metrics(
    telemetry: "chromadb.telemetry.opentelemetry.OpenTelemetryClient",  # noqa: F821 type: ignore
    segment_manager: "chromadb.segment.SegmentManager",  # noqa: F821 type: ignore
) -> None:
    telemetry.add_observable_gauge(
        name="segment_manager_active_segments",
        description="active segments loaded in memory.",
        unit="count",
        callback=lambda: len(segment_manager._instances) / 2
        if hasattr(segment_manager, "_instances")
        else 0,
    )


def register_metadata_segment_metrics(
    telemetry: "chromadb.telemetry.opentelemetry.OpenTelemetryClient",  # noqa: F821 type: ignore
    metadata_segment: "chromadb.segment.impl.metadata.sqlite.SqliteMetadataSegment",  # noqa: F821 type: ignore
) -> None:
    pass
    # telemetry.histogram(
    #     "SqliteMetadataSegment.get_metadata",
    #     unit="ms",
    #     description="Metadata index query times.",
    # )(metadata_segment.get_metadata)
    # telemetry.histogram(
    #     "SqliteMetadataSegment._write_metadata",
    #     unit="ms",
    #     description="Metadata index write times.",
    # )(metadata_segment._write_metadata)
