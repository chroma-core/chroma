import psutil


# We need to differ type resolution here
def register_baseline_metrics(
    telemetry: "chromadb.telemetry.opentelemetry.OpenTelemetryClient",  # noqa: F821 type: ignore
    settings: "chromadb.Settings",  # noqa: F821 type: ignore
) -> None:
    telemetry.add_observable_gauge(
        name="system_cpu_usage",
        description="system cpu usage",
        unit="percent",
        callback=lambda: psutil.cpu_percent(),
    )
    telemetry.add_observable_gauge(
        name="system_memory_usage",
        description="system memory usage",
        unit="percent",
        callback=lambda: psutil.virtual_memory().percent,
    )
    telemetry.add_observable_gauge(
        name="system_memory_usage_bytes",
        description="system memory usage in bytes",
        unit="bytes",
        callback=lambda: psutil.virtual_memory().used,
    )
    telemetry.add_observable_gauge(
        name="system_disk_usage",
        description="system disk usage",
        unit="percent",
        callback=lambda: psutil.disk_usage("/").percent,
    )
    telemetry.add_observable_gauge(
        name="chroma_persistence_disk_used",
        description="Chroma persistence disk used bytes.",
        unit="bytes",
        callback=lambda: psutil.disk_usage(settings.persist_directory).used,
    )
    telemetry.add_observable_gauge(
        name="chroma_persistence_disk_free",
        description="Chroma persistence disk free bytes.",
        unit="bytes",
        callback=lambda: psutil.disk_usage(settings.persist_directory).free,
    )
    telemetry.add_observable_gauge(
        name="process_cpu_usage",
        description="process cpu usage",
        unit="percent",
        callback=lambda: psutil.Process().cpu_percent(),
    )
    telemetry.add_observable_gauge(
        name="process_memory_usage",
        description="process memory usage",
        unit="percent",
        callback=lambda: psutil.Process().memory_percent(),
    )
    telemetry.add_observable_gauge(
        name="process_memory_bytes_rss",
        description="process memory",
        unit="bytes",
        callback=lambda: psutil.Process().memory_info().rss,
    )
    telemetry.add_observable_gauge(
        name="process_memory_bytes_vms",
        description="process memory usage",
        unit="bytes",
        callback=lambda: psutil.Process().memory_full_info().vms,
    )
    telemetry.add_observable_gauge(
        name="process_open_files",
        description="process open files",
        unit="count",  # is this correct metric, can we use standards?
        callback=lambda: len(psutil.Process().open_files()),
        # for Windows (which server should never run on num_handles() can be used)
    )
    telemetry.add_observable_gauge(
        name="process_threads",
        description="process threads",
        unit="count",  # is this correct metric, can we use standards?
        callback=lambda: len(psutil.Process().threads()),
    )
    telemetry.add_observable_gauge(
        name="process_network_connections",
        description="process network connections",
        unit="connections",  # is this correct metric, can we use standards?
        callback=lambda: len(psutil.Process().connections()),
    )
    telemetry.add_observable_gauge(
        name="process_children",
        description="process children",
        unit="children",  # is this correct metric, can we use standards?
        callback=lambda: len(psutil.Process().children()),
    )
    telemetry.add_observable_counter(
        name="network_in",
        callback=lambda: psutil.net_io_counters().bytes_recv,
        unit="bytes",
        description="Bytes in",
    )
    telemetry.add_observable_counter(
        name="network_out",
        callback=lambda: psutil.net_io_counters().bytes_sent,
        unit="bytes",
        description="Bytes out",
    )
