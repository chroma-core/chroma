"""Bounded regular-file checkpoint transfers for supervised fixture agents."""
import hashlib
from pathlib import Path,PurePosixPath
import tarfile
from .dynamic_runtime import inventory,durable_directory,durable_file
from .tree_manifest import file_hash

DEFAULT_LIMITS=dict(wire_bytes=32*1024**2,expanded_bytes=64*1024**2,file_bytes=16*1024**2,members=512)


def checked_inventory(rows,limits):
    if not isinstance(rows,dict) or not rows or len(rows)>limits['members']:raise ValueError('transfer member count exceeds admission')
    for name,row in rows.items():
        path=PurePosixPath(name)
        if not isinstance(name,str) or not name or len(name)>200 or path.is_absolute() or any(p in ('..','.') for p in path.parts) or str(path)!=name or '\\' in name:raise ValueError('unsafe transfer member')
        if not isinstance(row,dict) or type(row.get('bytes')) is not int or not 0<=row['bytes']<=limits['file_bytes']:raise ValueError('transfer file exceeds admission')
        digest=row.get('sha256')
        if not isinstance(digest,str) or len(digest)!=64 or any(c not in '0123456789abcdef' for c in digest):raise ValueError('invalid transfer digest')
    if sum(row['bytes'] for row in rows.values())>limits['expanded_bytes']:raise ValueError('expanded transfer exceeds admission')
    return rows


def export_checkpoint(directory,target,limits=None):
    limits=limits or DEFAULT_LIMITS;directory=Path(directory);target=Path(target)
    rows=checked_inventory(inventory(directory),limits)
    # USTAR adds at least one header per file plus end padding. This conservative
    # check rejects before writing, and the actual final wire length is checked.
    projected=sum(512+((row['bytes']+511)//512)*512 for row in rows.values())+10240
    if projected>limits['wire_bytes']:raise ValueError('wire transfer exceeds admission')
    with target.open('xb') as stream:
        with tarfile.open(fileobj=stream,mode='w',format=tarfile.USTAR_FORMAT) as archive:
            for name,row in sorted(rows.items()):
                info=tarfile.TarInfo(name);info.size=row['bytes'];info.mode=0o600;info.mtime=0
                with (directory/name).open('rb') as source:archive.addfile(info,source)
        stream.flush();__import__('os').fsync(stream.fileno())
    if inventory(directory)!=rows:raise ValueError('source changed during export')
    if target.stat().st_size>limits['wire_bytes']:raise ValueError('wire transfer exceeded admission')
    durable_file(target)
    return dict(bytes=target.stat().st_size,sha256=file_hash(target),files=rows)


def import_checkpoint(archive_path,destination,receipt,limits=None):
    limits=limits or DEFAULT_LIMITS;archive_path=Path(archive_path);destination=Path(destination)
    rows=checked_inventory(receipt['files'],limits)
    if type(receipt.get('bytes')) is not int or not 0<receipt['bytes']<=limits['wire_bytes'] or archive_path.stat().st_size!=receipt['bytes'] or file_hash(archive_path)!=receipt.get('sha256'):raise ValueError('wire archive differs from admitted source')
    destination.mkdir(parents=True,exist_ok=False);seen=set();expanded=0
    with archive_path.open('rb') as source,tarfile.open(fileobj=source,mode='r|') as archive:
        for member in archive:
            if member.type!=tarfile.REGTYPE or member.pax_headers or member.sparse is not None or member.name not in rows or member.name in seen:raise ValueError('unexpected, linked, duplicated or nonregular transfer member')
            expected=rows[member.name]
            if member.size!=expected['bytes'] or member.size>limits['file_bytes']:raise ValueError('expanded member differs from admission')
            expanded+=member.size
            if expanded>limits['expanded_bytes'] or len(seen)>=limits['members']:raise ValueError('expanded transfer exceeds admission')
            target=destination/member.name;target.parent.mkdir(parents=True,exist_ok=True);digest=hashlib.sha256();remaining=member.size
            with archive.extractfile(member) as payload,target.open('xb') as sink:
                while remaining:
                    chunk=payload.read(min(65536,remaining))
                    if not chunk:raise ValueError('truncated transfer member')
                    sink.write(chunk);digest.update(chunk);remaining-=len(chunk)
                sink.flush();__import__('os').fsync(sink.fileno())
            if digest.hexdigest()!=expected['sha256']:raise ValueError('transferred file digest mismatch')
            seen.add(member.name)
    if seen!=set(rows) or inventory(destination)!=rows:raise ValueError('incomplete checkpoint transfer')
    durable_directory(destination);return rows
