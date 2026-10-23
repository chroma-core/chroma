# Larger load driver

This isolated sandbox machine removes the eight-CPU request-generator ceiling from the repeated one-host and two-host search comparison. It sends HTTP requests to the existing private endpoints. It has no SSH authorization on the workers.

The machine is `i-05c41c0090a257a06` in account `079819354890`, region `us-east-1`, zone `us-east-1f`. Its public address is `100.48.66.121`; its private address is `172.31.64.169`. The machine type is `m6i.8xlarge`, with 32 logical CPUs and 128 GiB of memory. The official AWS Price List rate is $1.536 per hour. Its absolute stop deadline is 2026-10-15 03:01:58 UTC, with a maximum three-hour compute charge of $4.608 before storage and network charges.

The account, image owner, type specifications, network ownership, restricted artifact role, instance metadata identity, and active stop timer are verified. The bootstrap installs its stop timer before packages. The optional distribution package for the AWS command-line client is unavailable; that setup failure is retained. A Python environment with the AWS SDK and NumPy supplies the required runtime instead.

Automatic approval review rejects adding the new driver's public SSH key to the workers because that would persist additional access. No key is added. The existing driver manages the existing endpoints, and this machine requires only HTTP access through the existing private security-group rule.

The baseline uses the unchanged memory-v4 reader and adapter source, four adapter processes per serving host, and 16 logical CPUs per serving host. The routing tree, immutable snapshots, global beam, candidate budget, and query pool remain fixed. An explicit untimed 100-query warmup precedes three alternating paired repetitions at 1, 2, 4, 8, and 16 clients. Each client has a separate coordinator process. The operating-system page cache is uncontrolled; these runs do not measure cold storage.
