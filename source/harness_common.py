import hashlib,json
from pathlib import Path

def require(ok, message):
    if not ok: raise ValueError(message)

def descriptor(path):
    path=Path(path).resolve(strict=True);h=hashlib.sha256()
    with path.open('rb') as stream:
        for chunk in iter(lambda:stream.read(1024*1024),b''):h.update(chunk)
    return dict(path=str(path),bytes=path.stat().st_size,sha256=h.hexdigest())

def verify(item):
    require(descriptor(item['path'])==item,'descriptor mismatch: '+item['path'])
    return Path(item['path'])

def write(path, value):
    with path.open('x') as stream:json.dump(value,stream,indent=2);stream.write('\n')
