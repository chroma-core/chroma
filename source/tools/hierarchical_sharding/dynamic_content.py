"""Verify canonical values against sealed evidence of an actual writer build.

The local authority trusts only its admitted producer source and immutable Rust
binaries. This is execution provenance under that trusted-worker contract, not
remote attestation against a compromised worker or a malicious authority.
"""
import hashlib
import json
import math
from pathlib import Path
import struct
from .tree_manifest import canonical_bytes,file_hash

PROOF_NAME='build-provenance.json'
VERIFIED_WRITERS={
    'd159552ebcc1ce9e3aabef22f679cf5b1d74e9b09c3bbca790bd7a9aef667bf3',
    '7e53d0ff0936546aa5d55ca653a2a524cfe23fd53c6134499ff130bbb0a0d158',
    '55ab58878efdbb4ddf217df386535e3dcf2ab9c0ef0f23b80689c4293c4cb830',
}


def normalized_row(raw,dimension,metric):
    values=struct.unpack('<'+'f'*dimension,raw)
    if any(not math.isfinite(v) for v in values):raise ValueError('nonfinite canonical FP32 value')
    if metric=='cosine':
        # This matches the admitted worker's ordered f64 norm, division, then
        # round-to-nearest f32 cast. Canonical bytes themselves remain unchanged.
        norm=math.sqrt(sum(float(v)*float(v) for v in values))
        if norm==0:raise ValueError('cosine canonical vector must be nonzero')
        return struct.pack('<'+'f'*dimension,*(v/norm for v in values))
    return raw


def mapped_digest(vectors,mapping,dimension,metric,canonical=False):
    """Hash stable IDs and normalized rows in the snapshot's local-ID order."""
    digest=hashlib.sha256(canonical_bytes(dict(version=1,dimension=dimension,metric=metric)))
    count=0;row_bytes=dimension*4
    with Path(vectors).open('rb') as source,Path(mapping).open('rb') as ids:
        while ident:=ids.read(8):
            if len(ident)!=8:raise ValueError('truncated proof map')
            if canonical:source.seek(struct.unpack('<Q',ident)[0]*row_bytes)
            raw=source.read(row_bytes)
            if len(raw)!=row_bytes:raise ValueError('proof vector row is missing')
            digest.update(ident);digest.update(normalized_row(raw,dimension,metric));count+=1
        if not canonical and source.read(1):raise ValueError('proof input has extra rows')
    return digest.hexdigest(),count


def verify_build_content(directory,subtree,canonical,dimension,metric,accepted_producers):
    """Reject legacy/unverified output and mismatched canonical source values."""
    from .dynamic_runtime import inventory
    directory=Path(directory);path=directory/PROOF_NAME
    if path.is_symlink() or not path.is_file():raise ValueError('verified writer build provenance is required; legacy snapshots need an independent content audit or rebuild')
    proof=json.loads(path.read_text())
    if proof.get('format_version')!=1 or proof.get('producer_source_sha256') not in accepted_producers or proof.get('binary_sha256') not in VERIFIED_WRITERS or (type(proof.get('returncode')) is not int or proof['returncode']!=0):raise ValueError('unverified build producer or Rust writer')
    if proof.get('snapshot_sha256')!=subtree['snapshot']['sha256'] or proof.get('map_sha256')!=subtree['local_to_global']['sha256'] or proof.get('dimension')!=dimension or proof.get('metric')!=metric:raise ValueError('build execution evidence belongs to another snapshot')
    files=inventory(directory);files.pop(PROOF_NAME,None)
    if hashlib.sha256(canonical_bytes(files)).hexdigest()!=proof.get('output_inventory_sha256'):raise ValueError('writer output differs from sealed execution evidence')
    inputs=proof.get('inputs',{})
    for key,filename in (('vectors','construction-input.f32'),('map','construction-input.u64le')):
        expected=inputs.get(key,{})
        if expected.get('file')!=filename or files.get(filename)!=dict(bytes=expected.get('bytes'),sha256=expected.get('sha256')):raise ValueError('actual immutable writer input is missing or changed')
    if inputs['map']['sha256']!=subtree['local_to_global']['sha256']:raise ValueError('actual executed input map differs from persisted IDs')
    if type(proof.get('pid')) is not int or proof['pid']<=0 or not all(type(proof.get(k)) in (int,float) and math.isfinite(proof[k]) for k in ('started_unix','finished_unix')) or proof['finished_unix']<proof['started_unix']:raise ValueError('writer execution identity is incomplete')
    argv=proof.get('argv',[])
    if not isinstance(argv,list) or len(argv)<2 or argv[1]!='build':raise ValueError('actual writer command is missing')
    for flag,filename in (('--vectors','construction-input.f32'),('--id-map','construction-input.u64le')):
        if argv.count(flag)!=1 or argv.index(flag)+1>=len(argv) or Path(argv[argv.index(flag)+1]).name!=filename:raise ValueError('writer did not execute the sealed input')
    actual,count=mapped_digest(directory/'construction-input.f32',directory/'construction-input.u64le',dimension,metric)
    expected,canonical_count=mapped_digest(canonical,directory/'construction-input.u64le',dimension,metric,canonical=True)
    if actual!=proof.get('mapped_normalized_sha256') or count!=subtree['vector_count'] or canonical_count!=count or actual!=expected:raise ValueError('stored build input does not correspond to canonical vector values')
    return proof
