"""Exact admitted protocol method; alarm unwinds through its reader cleanup."""
import signal
import unittest

def deadline(signum,frame):
    raise TimeoutError("90-second protocol work deadline; reserve 30 seconds for six reader closes")

if __name__ == "__main__":
    signal.signal(signal.SIGALRM,deadline)
    signal.alarm(90)
    try:
        suite=unittest.defaultTestLoader.loadTestsFromName("tools.hierarchical_sharding.test_provider_cache_policy.NativePolicy.test_repeated_full_traces_ties_clear_error_and_same_process_retry")
        result=unittest.TextTestRunner(verbosity=2).run(suite)
    finally:
        signal.alarm(0)
    raise SystemExit(0 if result.wasSuccessful() else 1)
