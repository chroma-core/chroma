"""Actual-index integration check. Run with the built bench executable as argv[1]."""
import json
import math
import pathlib
import struct
import subprocess
import sys
import tempfile

binary = str(pathlib.Path(sys.argv[1]).resolve())
with tempfile.TemporaryDirectory() as tmp:
    root = pathlib.Path(tmp)
    checkpoint = root / "index"
    def build(rows, start, resume=False, generation=None, metric="l2"):
        source = root / "vectors.f32"
        source.write_bytes(b"".join(struct.pack("<4f", *row) for row in rows))
        cmd = [binary, "build", "--vectors", str(source), "--dimension", "4", "--global-id-start", str(start), "--id-stride", "3", "--row-count", str(len(rows)), "--checkpoint-dir", str(checkpoint), "--shard-id", "1", "--checkpoint-rows", "2", "--generation", str(generation if generation is not None else (2 if resume else 1)), "--source-id", "smoke", "--metric", metric, "--split-threshold", "8", "--merge-threshold", "2", "--branching-factor", "2"]
        if resume:
            cmd.append("--resume")
        return subprocess.run(cmd, capture_output=True, text=True, timeout=20)
    assert build([[1, 0, 0, 0], [0, 1, 0, 0]], 1).returncode == 0
    initial = json.loads((checkpoint / "manifest.json").read_text())
    assert build([[0, 0, 1, 0]], 7, True).returncode == 0
    latest = json.loads((checkpoint / "manifest.json").read_text())
    assert latest["vector_count"] == 3 and latest["id_stride"] == 3
    assert build([[0, 0, 0, 1]], 99, True).returncode != 0
    assert json.loads((checkpoint / "manifest.json").read_text()) == latest
    assert build([[2, 0, 0, 0], [3, 0, 0, 0], [float("nan"), 0, 0, 0]], 10, True, 3).returncode != 0
    assert json.loads((checkpoint / "manifest.json").read_text()) == latest
    assert not (checkpoint / "manifest-3.json").exists()
    def query(generation, requests):
        run = subprocess.run([binary, "read", "--checkpoint-dir", str(checkpoint), "--generation", str(generation)], input="\n".join(json.dumps(q) if isinstance(q, dict) else q for q in requests)+"\n", capture_output=True, text=True, check=True, timeout=20)
        return [json.loads(line) for line in run.stdout.splitlines()]
    request = {"query": [0, 0, 1, 0], "k": 3, "rerank_factor": 2, "generation": latest["generation"], "request_id": "test"}
    responses = query(latest["generation"], ["{", request, dict(request, generation=0)])
    assert "error" in responses[0] and "error" in responses[2]
    assert responses[1]["ids"][0] == 7 and abs(responses[1]["distances"][0]) < 1e-5
    assert set(responses[1]["ids"]) == {1, 4, 7}
    assert responses[1]["rerank_factor"] == 2 and responses[1]["candidate_budget"] == 6
    assert responses[1]["actual_candidate_count"] == 3
    old = query(initial["generation"], [dict(request, k=2, generation=initial["generation"])])[0]
    assert set(old["ids"]) == {1, 4}
    checkpoint = root / "split-index"
    rows = [[float(i), float(i % 3), 1.0, 0.0] for i in range(32)]
    split_build = build(rows, 1)
    assert split_build.returncode == 0, split_build.stderr
    split = query(1, [{"query": rows[17], "k": 32, "rerank": 32, "beam_min": 256, "beam_max": 256, "generation": 1}])[0]
    assert split["ids"][0] == 52 and abs(split["distances"][0]) < 1e-5
    assert set(split["ids"]) == {1 + 3*i for i in range(32)}
    for metric in ("cosine", "ip"):
        checkpoint = root / metric
        rows = [[2, 1, 0, 0], [0, 3, 0, 0], [-1, 0, 1, 0]]
        result = build(rows, 1, metric=metric)
        assert result.returncode == 0, result.stderr
        q = [1, 1, 0, 0]
        response = query(1, [{"query": q, "k": 3, "rerank": 3, "generation": 1}])[0]
        actual = dict(zip(response["ids"], response["distances"]))
        for i, row in enumerate(rows):
            dot = sum(a*b for a, b in zip(row, q))
            expected = 1-dot if metric == "ip" else 1-dot/(math.sqrt(sum(a*a for a in row))*math.sqrt(2))
            assert abs(actual[1+3*i]-expected) < 1e-5, (metric, actual, expected)
    for threads in (1, 2):
        checkpoint = root / f"default-{threads}"
        source = root / "tiny.f32"
        source.write_bytes(struct.pack("<4f", 0, 0, 1, 0))
        run = subprocess.run([binary, "build", "--vectors", str(source), "--dimension", "2", "--global-id-start", "0", "--row-count", "2", "--checkpoint-dir", str(checkpoint), "--shard-id", "0", "--generation", "1", "--source-id", "tiny", "--threads", str(threads)], capture_output=True, text=True, timeout=20)
        assert run.returncode == 0, run.stderr
        response = query(1, [{"query": [0, 0], "k": 2, "rerank": 2, "generation": 1}])[0]
        assert response["ids"] == [0, 1] and response["distances"] == [0, 1]
print("actual-index append, ID mapping, snapshot isolation, and query validation passed")
