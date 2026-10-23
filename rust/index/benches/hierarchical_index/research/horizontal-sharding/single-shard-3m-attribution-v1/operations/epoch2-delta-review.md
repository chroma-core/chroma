# Epoch2 prerequisite corrections

The storage preflight keeps exact EBS serial, 256 GiB block-device capacity and required free-space checks. It removes the redundant usable-filesystem total threshold and requires at least 80 GiB of actual free space; filesystem metadata reduces the usable size reported by df. The first guarded attempt confirms that resize2fs already sees the expanded filesystem. No data or partition replacement occurs.

The permission helper verifies exact worker identity through IMDS before changing one runtime kernel setting. It records the original perf_event_paranoid value of 4, temporarily sets 2 to permit per-process userspace sampling, and restores 4 before the API stop finalizer. It changes no persistent configuration. The existing live perf probe and phase parser must succeed before input materialization or benchmark builds. Failure triggers restoration and shutdown; counters alone do not satisfy profiling admission.

The first window is closed with API stopped confirmation and $0.20073165729522705 compute upper bound. Its watchdog is retired. Uploaded source archives and extracted files remain intact. A fresh epoch2 window uses a new absolute deadline and new reviewed manifest; it can reuse the exact source archives on the worker without another source transfer.

The input restorer reuses the exact retained first 100K file when present, checking its whole SHA256 against the pinned first-chunk hash while streaming into the new output. This avoids a duplicate 307.2 MB download. All remaining objects use the same pinned S3 versions and whole-chunk checks. The report distinguishes local bytes from network payload bytes.

Restoration binds the receipt to the exact window, sandbox account, instance, original value 4 and temporary value 2. It restores only a current value of 2, accepts 4 idempotently, and rejects every other current value. The receipt records before and after readbacks. The operator invokes restoration before API stop on completion or failure. An unavailable SSH connection triggers API stop and the independent absolute machine shutdown; the setting is not persisted and returns to its configured default after reboot.
