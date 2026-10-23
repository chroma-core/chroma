# Execution notes

The Linux build succeeds in 49.39 seconds. Initial deployment stops because the binary-cache helper permits only epoch31/32 paths. The epoch33 helper extends its exact research-path allowlist to epoch33; deployment reuses the already-built executable. The original failed deployment log remains retained in build.log. No new input transfer, external full-file input rehash or machine restart is required. The benchmark itself still performs its existing per-process vector-identity validation during startup; retained receipts report that setup time separately from construction.
