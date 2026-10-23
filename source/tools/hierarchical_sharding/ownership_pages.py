"""Immutable append-only ownership pages with a bounded changed-page verifier.

This component does not replace the compactor's flat-map oracle. A trusted root
receipt comes only from a complete audit of retained immutable page files. The
caller must preserve those files against mutation for the receipt's lifetime.
"""
import copy
import fcntl
import hashlib
import json
import os
from pathlib import Path
import re
import struct
from dataclasses import dataclass
from .tree_manifest import canonical_bytes, content_hash
from .dynamic_runtime import durable_namespace

MAX_ROWS = 100000
MAX_U64 = 2**64-1


def _integer(value, low=0, high=MAX_ROWS):
    if type(value) is not int or not low <= value <= high:
        raise ValueError('invalid ownership integer')
    return value


def _path(value):
    if not isinstance(value,str) or not value or '..' in Path(value).parts:
        raise ValueError('unsafe ownership path')
    path=Path(value)
    if not path.is_absolute() or path.resolve()!=path:
        raise ValueError('ownership paths must be absolute without symlinks')
    return path


def _read(page):
    path=_path(page['path']);data=path.read_bytes()
    if len(data)!=page['bytes'] or hashlib.sha256(data).hexdigest()!=page['sha256']:
        raise ValueError('ownership page digest or length differs')
    return data


def _layout(root):
    if not isinstance(root,dict) or set(root)!={'format','page_rows','global_count','parent_sha256','owners'} or root['format']!='ownership-pages-v1':
        raise ValueError('invalid ownership root')
    width=_integer(root['page_rows'],1,MAX_ROWS);total=_integer(root['global_count'])
    if root['parent_sha256'] is not None and not re.fullmatch('[0-9a-f]{64}',str(root['parent_sha256'])):
        raise ValueError('invalid parent root hash')
    owners=root['owners']
    if not isinstance(owners,list) or not 1<=len(owners)<=128:raise ValueError('invalid ownership owners')
    previous='';seen=set();count=0
    for owner in owners:
        if not isinstance(owner,dict) or set(owner)!={'owner','count','pages'}:raise ValueError('invalid owner descriptor')
        name=owner['owner']
        if not isinstance(name,str) or not re.fullmatch('[A-Za-z0-9][A-Za-z0-9_.-]{0,95}',name) or name<=previous:raise ValueError('owners must have unique sorted identities')
        previous=name;n=_integer(owner['count']);count+=n
        if count>MAX_ROWS:raise ValueError('ownership count exceeds bounded prototype')
        pages=owner['pages']
        if not isinstance(pages,list) or len(pages)!=(n+width-1)//width:raise ValueError('missing ownership page')
        offset=0
        for page in pages:
            if not isinstance(page,dict) or set(page)!={'path','sha256','bytes','start','rows'}:raise ValueError('invalid page descriptor')
            rows=_integer(page['rows'],1,width)
            if type(page['start']) is not int or page['start']!=offset or rows!=min(width,n-offset) or type(page['bytes']) is not int or page['bytes']!=rows*8:
                raise ValueError('page offsets or lengths overlap or omit rows')
            if not isinstance(page['sha256'],str) or not re.fullmatch('[0-9a-f]{64}',page['sha256']):raise ValueError('invalid page digest')
            path=str(_path(page['path']))
            if path in seen:raise ValueError('duplicate page reference')
            seen.add(path);offset+=rows
    if count!=total:raise ValueError('global count differs from owner counts')


@dataclass(frozen=True)
class TrustedRoot:
    """Exact audited root bytes; validity requires immutable retained parent files."""
    encoded: bytes
    sha256: str

    def root(self):
        if hashlib.sha256(self.encoded).hexdigest()!=self.sha256:raise ValueError('trusted root receipt hash differs')
        root=json.loads(self.encoded);_layout(root);return root


def audit(root):
    """Validate every page and dense global coverage before issuing a receipt."""
    _layout(root);seen=set()
    for owner in root['owners']:
        for page in owner['pages']:
            for (ident,) in struct.iter_unpack('<Q',_read(page)):
                if ident>=root['global_count'] or ident in seen:raise ValueError('duplicate or out-of-range global ownership')
                seen.add(ident)
    if len(seen)!=root['global_count']:raise ValueError('missing global ownership')
    encoded=canonical_bytes(root)
    return TrustedRoot(encoded,hashlib.sha256(encoded).hexdigest())


def verify_append(parent, candidate):
    """Read changed pages only, including an old tail needed for prefix proof."""
    if not isinstance(parent,TrustedRoot):raise ValueError('fully audited parent receipt required')
    before=parent.root();_layout(candidate)
    if candidate['parent_sha256']!=parent.sha256 or candidate['page_rows']!=before['page_rows'] or candidate['global_count']<=before['global_count']:
        raise ValueError('append root does not extend pinned parent')
    if [o['owner'] for o in before['owners']]!=[o['owner'] for o in candidate['owners']]:raise ValueError('append cannot change owners')
    seen=set();read_bytes=0;changed=0;reused=0
    for old,new in zip(before['owners'],candidate['owners']):
        if new['count']<old['count']:raise ValueError('append removes local ownership')
        for index,page in enumerate(new['pages']):
            previous=old['pages'][index] if index<len(old['pages']) else None
            if previous==page:
                reused+=1;continue
            if previous is not None and previous['rows']==before['page_rows']:raise ValueError('append changes a complete old page')
            data=_read(page);read_bytes+=len(data);changed+=1
            prefix=b''
            if previous is not None:
                prefix=_read(previous);read_bytes+=len(prefix)
                if data[:len(prefix)]!=prefix:raise ValueError('append changes old local prefix')
            for (ident,) in struct.iter_unpack('<Q',data[len(prefix):]):
                if not before['global_count']<=ident<candidate['global_count'] or ident in seen:raise ValueError('new ownership is duplicate or outside reserved interval')
                seen.add(ident)
        # A partial old tail may be reused unchanged only when this owner does not append.
        if new['count']>old['count'] and old['pages'] and old['pages'][-1]['rows']<before['page_rows'] and new['pages'][len(old['pages'])-1]==old['pages'][-1]:
            raise ValueError('new rows skip partial old tail')
    if len(seen)!=candidate['global_count']-before['global_count']:raise ValueError('new global interval has missing rows')
    return dict(changed_pages=changed,reused_pages=reused,verified_bytes=read_bytes,new_rows=len(seen))


def audit_append(parent,candidate):
    """Extend an audited immutable receipt by verifying only changed page bytes."""
    stats=verify_append(parent,candidate)
    encoded=canonical_bytes(candidate)
    return TrustedRoot(encoded,hashlib.sha256(encoded).hexdigest()),stats


def lookup(receipt,owner,local_id):
    """Resolve one local ID against the caller's pinned immutable root."""
    root=receipt.root();_integer(local_id)
    record=next((o for o in root['owners'] if o['owner']==owner),None)
    if record is None or local_id>=record['count']:raise ValueError('unknown owner or local ID')
    page=record['pages'][local_id//root['page_rows']]
    return struct.unpack_from('<Q',_read(page),(local_id-page['start'])*8)[0]


def _page(directory, owner, start, values):
    data=b''.join(struct.pack('<Q',value) for value in values)
    path=directory/f'{owner}-{start}.u64le'
    with path.open('xb') as out:out.write(data);out.flush();os.fsync(out.fileno())
    path.chmod(0o400)
    return dict(path=str(path),sha256=hashlib.sha256(data).hexdigest(),bytes=len(data),start=start,rows=len(values))


def create(maps,directory,page_rows=128):
    _integer(page_rows,1,MAX_ROWS)
    root=dict(format='ownership-pages-v1',page_rows=page_rows,global_count=sum(len(v) for v in maps.values()),parent_sha256=None,owners=[])
    # Admit names, count and values before creating output files.
    if not maps or len(maps)>128 or root['global_count']>MAX_ROWS:raise ValueError('bounded ownership maps required')
    values=[value for rows in maps.values() for value in rows]
    if any(type(v) is not int or not 0<=v<root['global_count'] for v in values) or len(set(values))!=root['global_count']:raise ValueError('dense disjoint global IDs required')
    if any(not isinstance(n,str) or not re.fullmatch('[A-Za-z0-9][A-Za-z0-9_.-]{0,95}',n) for n in maps):raise ValueError('unsafe owner identity')
    directory=_path(str(directory));directory.mkdir(parents=True,exist_ok=False)
    for owner,rows in sorted(maps.items()):
        root['owners'].append(dict(owner=owner,count=len(rows),pages=[_page(directory,owner,i,rows[i:i+page_rows]) for i in range(0,len(rows),page_rows)]))
    audit(root);return root


def append(parent, additions, directory):
    if not isinstance(parent,TrustedRoot):raise ValueError('fully audited parent receipt required')
    old=parent.root();root=copy.deepcopy(old);root['parent_sha256']=parent.sha256
    if set(additions)-{o['owner'] for o in old['owners']}:raise ValueError('unknown append owner')
    values=[v for rows in additions.values() for v in rows];total=old['global_count']+len(values)
    if not values or total>MAX_ROWS or any(type(v) is not int or not old['global_count']<=v<total for v in values) or len(set(values))!=len(values):raise ValueError('complete disjoint new global interval required')
    root['global_count']=total;directory=_path(str(directory));directory.mkdir(parents=True,exist_ok=False)
    for owner in root['owners']:
        new=list(additions.get(owner['owner'],[]))
        if not new:continue
        start=owner['count'];prefix=[]
        if owner['pages'] and owner['pages'][-1]['rows']<root['page_rows']:
            tail=owner['pages'].pop();start=tail['start'];prefix=[v for (v,) in struct.iter_unpack('<Q',_read(tail))]
        rows=prefix+new;owner['count']+=len(new)
        owner['pages'].extend(_page(directory,owner['owner'],start+i,rows[i:i+root['page_rows']]) for i in range(0,len(rows),root['page_rows']))
    verify_append(parent,root);return root


def publish(pointer,candidate,expected_parent):
    """CAS the complete root after changed pages and their directories are durable."""
    pointer=_path(str(pointer));lock=Path(str(pointer)+'.lock')
    with lock.open('a+b') as held:
        fcntl.flock(held,fcntl.LOCK_EX)
        current=json.loads(pointer.read_bytes())
        if content_hash(current)!=expected_parent.sha256:raise ValueError('ownership root compare-and-swap failed')
        stats=verify_append(expected_parent,candidate)
        for directory in {Path(p['path']).parent for o in candidate['owners'] for p in o['pages']}:
            durable_namespace(directory)
        temp=Path(str(pointer)+'.tmp')
        with temp.open('xb') as out:out.write(canonical_bytes(candidate));out.flush();os.fsync(out.fileno())
        os.replace(temp,pointer)
        durable_namespace(pointer.parent)
        return stats
