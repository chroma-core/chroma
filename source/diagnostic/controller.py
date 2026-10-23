"""Bounded read-only native memory diagnostic, not a performance benchmark."""
import argparse
import hashlib
import json
import os
from pathlib import Path
import queue
import signal
import subprocess
import threading
import time

LIMIT = 32 * 1024 * 1024
RSS_LIMIT = 1024 * 1024 * 1024

def digest(path):
    h = hashlib.sha256()
    with path.open("rb") as stream:
        for part in iter(lambda: stream.read(1024 * 1024), b""):
            h.update(part)
    return h.hexdigest()

def inventory(root):
    result = {}
    for path in sorted(root.rglob("*")):
        if path.is_symlink():
            raise ValueError("fixture symlink")
        if path.is_file():
            result[str(path.relative_to(root))] = {"bytes": path.stat().st_size, "sha256": digest(path)}
    return result

def run(binary, fixture, output, expected_sha):
    assert digest(binary) == expected_sha
    output.mkdir(exist_ok=False)
    before = inventory(fixture)
    assert sum(v["bytes"] for v in before.values()) <= 64 * 1024 * 1024
    (output / "inventory-before.json").write_text(json.dumps(before, sort_keys=True))
    deadline = time.monotonic() + 180
    summary = {"status": "running", "rss_sample_interval_s": .05, "rss_sampled_abort_bytes": RSS_LIMIT,
               "transient_decode_heap": "unavailable", "rss_instantaneous_peak": "not measured",
               "policies": [], "binary_sha256": expected_sha}
    failure = None
    total = [0]
    lock = threading.Lock()
    try:
        for mode in ("retain", "evict", "stream"):
            start = time.monotonic()
            child = subprocess.Popen([str(binary), str(fixture), mode], stdin=subprocess.PIPE,
                                     stdout=subprocess.PIPE, stderr=subprocess.PIPE, start_new_session=True)
            events = queue.Queue()
            def consume(stream, name):
                try:
                    with (output / f"{mode}.{name}.jsonl").open("wb") as sink:
                        while True:
                            line = stream.readline(LIMIT + 1)
                            if not line:
                                break
                            with lock:
                                total[0] += len(line)
                                if total[0] > LIMIT:
                                    events.put(("error", "output limit"))
                                    return
                            sink.write(line)
                            sink.flush()
                            if name == "stdout":
                                events.put(("phase", json.loads(line)))
                except BaseException as exc:
                    events.put(("error", repr(exc)))
                finally:
                    stream.close()
                    events.put((name + "_closed", None))
            threads = [threading.Thread(target=consume, args=(stream, name), daemon=True)
                       for stream, name in ((child.stdout, "stdout"), (child.stderr, "stderr"))]
            for thread in threads:
                thread.start()
            phases, samples, closed, maximum = [], [], set(), 0
            try:
                while len(closed) < 2 or child.poll() is None:
                    now = time.monotonic()
                    if now > deadline or now - start > 60:
                        raise TimeoutError("diagnostic deadline")
                    if child.poll() is None:
                        measured = subprocess.run(["ps", "-o", "rss=", "-p", str(child.pid)],
                                                  capture_output=True, text=True, timeout=2)
                        if measured.stdout.strip():
                            rss = int(measured.stdout.strip()) * 1024
                            maximum = max(maximum, rss)
                            samples.append({"elapsed_s": now - start, "rss_bytes": rss})
                            if rss > RSS_LIMIT:
                                raise RuntimeError("sampled RSS abort threshold")
                    try:
                        kind, data = events.get(timeout=.05)
                    except queue.Empty:
                        continue
                    if kind == "error":
                        raise RuntimeError(data)
                    if kind.endswith("_closed"):
                        closed.add(kind)
                    if kind == "phase":
                        phases.append(data)
                        if len(phases) > 30:
                            raise RuntimeError("phase limit")
                        child.stdin.write(b"continue\n")
                        child.stdin.flush()
                if child.wait(timeout=2) != 0:
                    raise RuntimeError("native probe failed")
                assert phases[-1]["phase"] == "clear_provider"
            finally:
                # This newly created session belongs only to this child and its descendants.
                try:
                    os.killpg(child.pid, signal.SIGKILL)
                except ProcessLookupError:
                    pass
                child.wait(timeout=5)
                child.stdin.close()
                for thread in threads:
                    thread.join(timeout=3)
                (output / f"{mode}.rss.json").write_text(json.dumps(samples))
            summary["policies"].append({"mode": mode, "phases": len(phases), "max_sampled_rss_bytes": maximum,
                                         "exit_code": child.returncode, "operation_results":
                                         [p["work"] for p in phases if p["work"] and "hits" in p["work"]]})
        oracle = summary["policies"][0]["operation_results"]
        for policy in summary["policies"][1:]:
            assert [(v["hits"], v["unique"]) for v in policy["operation_results"]] == [(v["hits"], v["unique"]) for v in oracle]
        summary["status"] = "passed"
    except BaseException as exc:
        summary["status"] = "failed"
        summary["error"] = repr(exc)
        failure = exc
    finally:
        after = inventory(fixture)
        (output / "inventory-after.json").write_text(json.dumps(after, sort_keys=True))
        summary["fixture_inventory_unchanged"] = before == after
        if before != after:
            summary["status"] = "failed"
            failure = RuntimeError("fixture changed")
        (output / "summary.json").write_text(json.dumps(summary, indent=2, sort_keys=True))
    if failure:
        raise failure

if __name__ == "__main__":
    a = argparse.ArgumentParser()
    a.add_argument("--binary", type=Path, required=True)
    a.add_argument("--sha256", required=True)
    a.add_argument("--fixture", type=Path, required=True)
    a.add_argument("--output", type=Path, required=True)
    args = a.parse_args()
    run(args.binary.resolve(), args.fixture.resolve(), args.output.resolve(), args.sha256)
