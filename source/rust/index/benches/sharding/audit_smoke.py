"""Next-revision publication gate above 100k rows; never run against timed v2."""
import hashlib
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
    with vectors.open("wb") as data, mapping.open("wb") as ids:
        for i in range(100001):
            data.write(struct.pack("<4f", 1, i % 17, 0, 0))
            ids.write(struct.pack("<Q", 10+i))
    command = [binary,"build","--vectors",str(vectors),"--dimension","4","--row-count","100001","--checkpoint-dir",str(checkpoint),"--shard-id","audit","--generation","1","--source-id","audit-smoke","--id-map",str(mapping),"--boundary-hash","frozen","--split-threshold","100010","--merge-threshold","0","--threads","1","--checkpoint-rows","100001"]
    def run(args):
        result = subprocess.run(args,capture_output=True,text=True,timeout=180)
        return result
    initial = run(command)
    assert initial.returncode == 0, initial.stderr
    manifest = json.loads((checkpoint/"manifest.json").read_text())
    binding = manifest["integrity_audit"]
    assert binding["generation"] == 1 and binding["block_ids"] == manifest["block_ids"]
    assert binding["id_map_sha256"] == hashlib.sha256(mapping.read_bytes()).hexdigest()
    coverage = binding["coverage"]
    assert coverage["unique_live_rows"] == 100001 and coverage["bitmap_bytes"] == 12501
    assert coverage["raw_embedding_reads"] == 0 and coverage["peak_leaf_entries"] == 100001
    assert coverage["reachable_nodes"] == 1
    # A failed post-flush audit preserves the published generation and its map.
    append = directory/"append.f32"
    append.write_bytes(struct.pack("<4f",1,18,0,0))
    with mapping.open("ab") as stream:
        stream.write(struct.pack("<Q",100011))
    resume = command[:]
    for flag,value in (("--vectors",str(append)),("--row-count","1"),("--generation","2")):
        resume[resume.index(flag)+1] = value
    resume.append("--resume")
    failed = run(resume+["--audit-max-leaf-entries","100001"])
    assert failed.returncode != 0 and "full integrity audit" in failed.stderr, failed.stderr
    assert json.loads((checkpoint/"manifest.json").read_text()) == manifest
    assert not (checkpoint/"manifest-2.json").exists()
    assert not (checkpoint/"id-map-2.u64le").exists()
    assert hashlib.sha256((checkpoint/manifest["id_mapping"]["path"]).read_bytes()).hexdigest() == manifest["id_mapping"]["sha256"]
    succeeded = run(resume)
    assert succeeded.returncode == 0, succeeded.stderr
    latest = json.loads((checkpoint/"manifest.json").read_text())
    assert latest["integrity_audit"]["coverage"]["unique_live_rows"] == 100002
    assert json.loads((checkpoint/"manifest-1.json").read_text()) == manifest
print("full audit above 100k and post-flush publication failure checks passed")
