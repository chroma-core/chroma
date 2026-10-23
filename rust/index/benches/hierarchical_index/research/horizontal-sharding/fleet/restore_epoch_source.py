"""Restore one immutable binary's complete source into a new isolated directory."""
import argparse
import hashlib
import json
import os
from pathlib import Path
import tarfile
import uuid


def digest(path):
    with Path(path).open('rb') as stream:
        return hashlib.file_digest(stream,'sha256').hexdigest()


def restore(epoch,registry,archive,binary,destination):
    record=json.loads(Path(registry).read_text())['epochs'][epoch]
    if digest(archive)!=record['source_archive_sha256'] or digest(binary)!=record['binary_sha256']:
        raise ValueError('source archive or binary does not match the selected immutable epoch')
    destination=Path(destination)
    if destination.exists():raise FileExistsError('source epoch destination already exists')
    temporary=destination.with_name(destination.name+'.partial-'+uuid.uuid4().hex)
    temporary.mkdir(parents=True);expected={}
    with tarfile.open(archive) as source:
        for member in source.getmembers():
            relative=Path(member.name)
            if relative.is_absolute() or '..' in relative.parts or not relative.parts or relative.parts[0]!='source':
                raise ValueError('source member escapes the epoch directory')
            if member.isdir():continue
            if member.name in expected:raise ValueError('duplicate source archive member')
            if member.isfile():
                stream=source.extractfile(member);sha=hashlib.sha256()
                while True:
                    block=stream.read(8*1024*1024)
                    if not block:break
                    sha.update(block)
                expected[member.name]=dict(kind='file',bytes=member.size,sha256=sha.hexdigest())
            elif member.issym():expected[member.name]=dict(kind='symlink',target=member.linkname)
            else:raise ValueError('unsupported source archive member')
        source.extractall(temporary,filter='data')
    for name,entry in expected.items():
        path=temporary/name
        if entry['kind']=='symlink':
            if not path.is_symlink() or os.readlink(path)!=entry['target'] or not path.resolve().is_relative_to(temporary.resolve()):raise ValueError('source symlink differs or escapes the epoch')
        elif path.is_symlink() or path.stat().st_size!=entry['bytes'] or digest(path)!=entry['sha256']:
            raise ValueError('restored source file differs: '+name)
    manifest=dict(epoch=epoch,binary_sha256=record['binary_sha256'],source_archive_sha256=record['source_archive_sha256'],verified_files=len(expected),files=expected)
    (temporary/'source-manifest.json').write_text(json.dumps(manifest,indent=2));os.rename(temporary,destination)
    return manifest


if __name__=='__main__':
    parser=argparse.ArgumentParser(description=__doc__)
    for key in ('epoch','registry','archive','binary','destination'):parser.add_argument('--'+key,required=True)
    args=parser.parse_args();result=restore(args.epoch,args.registry,args.archive,args.binary,args.destination)
    print(json.dumps({key:value for key,value in result.items() if key!='files'}))
