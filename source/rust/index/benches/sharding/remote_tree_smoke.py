"""Exercises the same persistent primitives used by local and HTTP coordinators."""
import json
import pathlib
import struct
import subprocess
import sys
import tempfile

binary = str(pathlib.Path(sys.argv[1]).resolve())
with tempfile.TemporaryDirectory() as tmp:
    directory = pathlib.Path(tmp)
    vectors = directory / "vectors.f32"
    mapping = directory / "ids.u64le"
    checkpoint = directory / "index"
    rows = [[float(i), float(i % 3), 1.0, 0.0] for i in range(64)]
    external = [1000 + (63-i)*13 for i in range(64)]
    vectors.write_bytes(b"".join(struct.pack("<4f", *row) for row in rows))
    mapping.write_bytes(b"".join(struct.pack("<Q", i) for i in external))
    build = [binary, "build", "--vectors", str(vectors), "--dimension", "4", "--row-count", "64", "--checkpoint-dir", str(checkpoint), "--generation", "1", "--source-id", "tree-smoke", "--shard-id", "subtree-0", "--id-map", str(mapping), "--boundary-hash", "fixed-boundary", "--branching-factor", "2", "--split-threshold", "8", "--merge-threshold", "2", "--threads", "2"]
    run = subprocess.run(build, capture_output=True, text=True, timeout=30)
    assert run.returncode == 0, run.stderr
    manifest = json.loads((checkpoint / "manifest.json").read_text())
    process = subprocess.Popen([binary, "read", "--checkpoint-dir", str(checkpoint), "--generation", "1"], stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
    def call(op, **fields):
        process.stdin.write(json.dumps(dict(op=op, generation=1, **fields))+"\n")
        process.stdin.flush()
        response = json.loads(process.stdout.readline())
        assert "error" not in response, response
        return response
    query = [23.234, 1.1, 1.25, 0.37]
    description = call("describe_root", profile_memory=True)
    assert description["retained_postings"]["entries"] == 0
    assert description["retained_postings"]["materialized_leaves"] == 0
    assert description["root"]["kind"] == manifest["root_ref"]["kind"]
    assert description["root"]["node_id"] == manifest["root_ref"]["id"]
    assert not description["root"]["is_leaf"]
    frontier = [description["root"]["node_id"]]
    leaves = []
    while frontier:
        children = call("expand", nodes=frontier, query=query)["children"]
        leaves.extend(child["node_id"] for child in children if child["is_leaf"])
        frontier = [child["node_id"] for child in children if not child["is_leaf"]]
    scored = call("score_selected_leaves", nodes=leaves, query=query, candidate_budget=64, profile_memory=True)
    retained = scored["retained_postings"]
    assert retained["entries"] >= 64 and 0 < retained["materialized_leaves"] <= len(leaves)
    assert retained["payload_bytes"] > 0
    assert retained["leaf_owned_bytes_including_centroids"] >= retained["payload_bytes"]
    assert retained["posting_block_pins"] == retained["vector_block_pins"] == 0
    unprofiled = call("score_selected_leaves", nodes=leaves, query=query, candidate_budget=64)
    assert "retained_postings" not in unprofiled
    assert unprofiled["candidates"] == scored["candidates"]
    assert scored["unique_candidates_scanned"] == 64
    assert {h["id"] for h in scored["candidates"]} == set(external)
    exact = call("rerank_selected", local_ids=[h["local_id"] for h in scored["candidates"]], query=query)["results"]
    exact.sort(key=lambda h:(h["distance"], h["id"]))
    expected = sorted(((sum((a-b)**2 for a,b in zip(row,query)),external[i]) for i,row in enumerate(rows)))
    assert [h["id"] for h in exact[:10]] == [i for _,i in expected[:10]]
    assert all(abs(h["distance"]-d)<0.002 for h,(d,_) in zip(exact,expected))
    process.stdin.close()
    assert process.wait(timeout=10) == 0
    # Moving a frozen routing boundary cannot publish a replacement snapshot.
    appended = directory / "append.f32"
    appended.write_bytes(struct.pack("<4f", 70, 1, 0, 0))
    mapping.write_bytes(mapping.read_bytes()+struct.pack("<Q", 999999))
    resume = build[:]
    for flag, value in (("--vectors", str(appended)),("--row-count", "1"),("--generation", "2"),("--boundary-hash", "changed-boundary")):
        resume[resume.index(flag)+1] = value
    run = subprocess.run(resume+["--resume"], capture_output=True, text=True, timeout=30)
    assert run.returncode != 0
    assert json.loads((checkpoint / "manifest.json").read_text()) == manifest
    # Equal approximate distances use external IDs when applying the local budget.
    tie_checkpoint = directory / "ties"
    vectors.write_bytes(struct.pack("<4f", 1, 1, 0, 0)*8)
    mapping.write_bytes(b"".join(struct.pack("<Q", i) for i in [80,70,60,50,40,30,20,10]))
    tie_build = build[:]
    tie_build[tie_build.index("--checkpoint-dir")+1] = str(tie_checkpoint)
    tie_build[tie_build.index("--row-count")+1] = "8"
    run = subprocess.run(tie_build, capture_output=True, text=True, timeout=30)
    assert run.returncode == 0, run.stderr
    process = subprocess.Popen([binary,"read","--checkpoint-dir",str(tie_checkpoint),"--generation","1"],stdin=subprocess.PIPE,stdout=subprocess.PIPE,stderr=subprocess.PIPE,text=True)
    root = call("describe_root")["root"]
    tied = call("score_selected_leaves",nodes=[root["node_id"]],query=[1,0,0,0],candidate_budget=3)
    assert [h["id"] for h in tied["candidates"]] == [10,20,30], tied
    assert tied["unique_candidates_scanned"] == 8
    process.stdin.close()
    assert process.wait(timeout=10) == 0
print("remote traversal, selected-leaf scoring, exact rerank, explicit IDs, and frozen boundary checks passed")
