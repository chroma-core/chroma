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
    # Reviewed representative split-target epoch; normal defaults unchanged.
    'ff3dbe73449f96618fb6cebcbf33dac9617ad4e2562c9d3e5ea980830d713666',
    # Native shared-parser correctness epoch; remote-selected-write-v23 reviewed build.
    '9ed5ffbdc806372972e5ef9357e3612f8abe886afe9baf12bf99bfd340f268fe',
    # Native owner-private selected-write epoch; remote-selected-write-v9 reviewed source/build.
    'cb26d15c8449d8d3d05996b65b46dbfa2ad0613aae6756b71e8515cc667c44a0',
    # Native strict single-copy compactor; global-write-v5 independent admission.
    'ff46b44e63a37e67a90c10d17975263813471803176a6c47d14f83ad80edabfa',
    # Native direct-fork compactor epoch; global-write-v4 independent admission.
    'd26ae940c9e0d918fde681e0fc7b9e2a6b16bcda0c2355bcdd534ccbd1a1cc12',
    # Native Darwin correctness-only epoch; independently admitted build/source receipt.
    '745c00160558cda786a2f19ec3ebf2e465a839e9acb209a0162a63cd1c22a449',
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
    if not isinstance(argv,list) or len(argv)<2:raise ValueError('actual writer command is missing')
    if argv[1]=='build':
        for flag,filename in (('--vectors','construction-input.f32'),('--id-map','construction-input.u64le')):
            if argv.count(flag)!=1 or argv.index(flag)+1>=len(argv) or Path(argv[argv.index(flag)+1]).name!=filename:raise ValueError('writer did not execute the sealed input')
    elif argv[1]=='compact-global-writes' and proof.get('execution_kind')=='global-write-compaction':
        spec=proof.get('compactor_spec',{})
        if (spec.get('file')!='write-plan-spec.json' or files.get('write-plan-spec.json')!=dict(bytes=spec.get('bytes'),sha256=spec.get('sha256'))
                or argv.count('--spec')!=1 or argv.index('--spec')+1>=len(argv) or Path(argv[argv.index('--spec')+1]).name!='write-plan-spec.json'):
            raise ValueError('compactor did not execute the sealed parent/input specification')
        receipt=json.loads((directory/'compactor-execution.json').read_text())
        if (receipt.get('snapshot_sha256')!=subtree['snapshot']['sha256'] or receipt.get('exact_vector_rows_verified')!=subtree['vector_count']
                or receipt.get('all_selected_leaf_memberships_verified') is not True):raise ValueError('saved selected-leaf content proof differs')
    elif argv[1]=='selected-write-session' and proof.get('execution_kind')=='selected-write-session':
        spec=proof.get('compactor_spec',{})
        if (spec.get('file')!='selected-write-spec.json' or files.get('selected-write-spec.json')!=dict(bytes=spec.get('bytes'),sha256=spec.get('sha256'))
                or argv.count('--spec')!=1 or argv.index('--spec')+1>=len(argv) or Path(argv[argv.index('--spec')+1]).name!='selected-write-spec.json'):
            raise ValueError('owner session did not execute its sealed specification')
        if proof.get('consumed_spec_sha256')!=spec.get('sha256'):raise ValueError('owner consumed-spec digest differs')
        saved_result=json.loads((directory/'selected-write-result.json').read_text())
        if saved_result.get('op')!='saved' or saved_result.get('spec_sha256')!=spec.get('sha256'):raise ValueError('native saved-spec proof differs')
        receipt=json.loads((directory/'compactor-execution.json').read_text())
        if (receipt.get('snapshot_sha256')!=subtree['snapshot']['sha256'] or receipt.get('exact_vector_rows_verified')!=subtree['vector_count']
                or receipt.get('all_selected_leaf_memberships_verified') is not True):raise ValueError('owner saved selected-leaf proof differs')
        if saved_result.get('receipt')!=receipt:raise ValueError('native saved result differs from persisted execution receipt')
    else:raise ValueError('actual writer command is missing or unsupported')
    actual,count=mapped_digest(directory/'construction-input.f32',directory/'construction-input.u64le',dimension,metric)
    expected,canonical_count=mapped_digest(canonical,directory/'construction-input.u64le',dimension,metric,canonical=True)
    if actual!=proof.get('mapped_normalized_sha256') or count!=subtree['vector_count'] or canonical_count!=count or actual!=expected:raise ValueError('stored build input does not correspond to canonical vector values')
    return proof
