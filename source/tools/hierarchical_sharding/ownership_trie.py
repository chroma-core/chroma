"""Bounded immutable descriptor paths; separate from deployment admission.

The fixed key spaces deliberately retain the 100k correctness-prototype limit.
Initial trust requires a full audit of immutable files. Incremental proof checks
every changed path against that exact parent; the deployment integration is a
separate milestone. Payload and cache-owned byte counters are distinct.
"""
import copy
import hashlib
import json
import os
from pathlib import Path
import struct
import sys
from collections import OrderedDict
from dataclasses import dataclass
from . import ownership_pages as flat
from .tree_manifest import canonical_bytes,content_hash

MAX_NODE_BYTES=65536


def _depth(cap,fanout):
    height=0
    while fanout**(height+1)<cap:height+=1
    return height


def _owned(value):
    if isinstance(value,dict):return sys.getsizeof(value)+sum(_owned(k)+_owned(v) for k,v in value.items())
    if isinstance(value,list):return sys.getsizeof(value)+sum(_owned(v) for v in value)
    return sys.getsizeof(value)


class Store:
    def __init__(self,fanout,cache_nodes=32,cache_bytes=262144):
        flat._integer(fanout,2,16);flat._integer(cache_nodes,0,1024);flat._integer(cache_bytes,0,16*1024**2)
        self.fanout=fanout;self.cache_nodes=cache_nodes;self.cache_bytes=cache_bytes
        self.cache=OrderedDict();self.owned=0
        self.metrics=dict(metadata_disk_reads=0,metadata_read_bytes=0,metadata_parses=0,cache_hits=0,peak_cache_owned_bytes=0,peak_cache_nodes=0,page_read_bytes=0,node_writes=0)

    def descriptor(self,desc,start,height):
        if not isinstance(desc,dict) or set(desc)!={'path','sha256','bytes','start','height','count'}:raise ValueError('invalid descriptor')
        for key,value in [('start',start),('height',height)]:
            if type(desc[key]) is not int or desc[key]!=value:raise ValueError('descriptor range or height differs')
        flat._integer(desc['count'],1,flat.MAX_ROWS);flat._integer(desc['bytes'],1,MAX_NODE_BYTES)
        if not isinstance(desc['sha256'],str) or len(desc['sha256'])!=64 or any(c not in '0123456789abcdef' for c in desc['sha256']):raise ValueError('invalid descriptor hash')
        flat._path(desc['path'])

    def get(self,desc,start,height):
        self.descriptor(desc,start,height);key=content_hash(desc)
        if key in self.cache:
            self.metrics['cache_hits']+=1;node,size=self.cache.pop(key);self.cache[key]=(node,size);return copy.deepcopy(node)
        path=flat._path(desc['path'])
        if path.stat().st_size!=desc['bytes']:raise ValueError('descriptor file length differs')
        with path.open('rb') as stream:data=stream.read(desc['bytes']+1)
        self.metrics['metadata_disk_reads']+=1;self.metrics['metadata_read_bytes']+=len(data)
        if hashlib.sha256(data).hexdigest()!=desc['sha256']:raise ValueError('descriptor digest differs')
        node=json.loads(data);self.metrics['metadata_parses']+=1
        if canonical_bytes(node)!=data:raise ValueError('descriptor encoding is not canonical')
        if not isinstance(node,dict) or set(node)!={'start','height','slots'} or type(node['start']) is not int or node['start']!=start or type(node['height']) is not int or node['height']!=height or not isinstance(node['slots'],dict) or not 1<=len(node['slots'])<=self.fanout:raise ValueError('invalid descriptor node')
        count=0
        for digit,value in node['slots'].items():
            if digit not in {str(i) for i in range(self.fanout)}:raise ValueError('invalid descriptor slot')
            if height:
                self.descriptor(value,start+int(digit)*self.fanout**height,height-1);count+=value['count']
            else:count+=1
        if count!=desc['count']:raise ValueError('descriptor aggregate count differs')
        # Charge decoded values, keys, entry tuples and the actual OrderedDict
        # container. This is Python-owned memory, not allocator/RSS accounting.
        size=_owned(node)+_owned(key)+sys.getsizeof((node,0))+sys.getsizeof(0)
        if self.cache_nodes and size+sys.getsizeof(OrderedDict())<=self.cache_bytes:
            self.cache[key]=(node,size);self.owned+=size
            while self.cache and (len(self.cache)>self.cache_nodes or self.owned+sys.getsizeof(self.cache)>self.cache_bytes):
                _,(_,oldsize)=self.cache.popitem(last=False);self.owned-=oldsize
            charged=self.owned+sys.getsizeof(self.cache)
            self.metrics['peak_cache_owned_bytes']=max(self.metrics['peak_cache_owned_bytes'],charged)
            self.metrics['peak_cache_nodes']=max(self.metrics['peak_cache_nodes'],len(self.cache))
        return copy.deepcopy(node)

    def put(self,node,directory):
        data=canonical_bytes(node)
        if len(data)>MAX_NODE_BYTES:raise ValueError('descriptor exceeds byte bound')
        digest=hashlib.sha256(data).hexdigest();path=directory/(digest+'.json')
        if path.exists():
            with path.open('rb') as stream:existing=stream.read(MAX_NODE_BYTES+1)
            if existing!=data:raise ValueError('immutable node collision')
        else:
            with path.open('xb') as out:out.write(data);out.flush();os.fsync(out.fileno())
            path.chmod(0o400);self.metrics['node_writes']+=1
        count=sum(v['count'] for v in node['slots'].values()) if node['height'] else len(node['slots'])
        return dict(path=str(path),sha256=digest,bytes=len(data),start=node['start'],height=node['height'],count=count)

    def set(self,desc,key,value,start,height,directory):
        if type(key) is not int or not start<=key<start+self.fanout**(height+1):raise ValueError('key outside authenticated path')
        node=self.get(desc,start,height) if desc else dict(start=start,height=height,slots={})
        digit=(key-start)//self.fanout**height
        if height:
            node['slots'][str(digit)]=self.set(node['slots'].get(str(digit)),key,value,start+digit*self.fanout**height,height-1,directory)
        else:node['slots'][str(digit)]=value
        return self.put(node,directory)

    def lookup(self,desc,key,start,height):
        if desc is None:return None
        node=self.get(desc,start,height);digit=(key-start)//self.fanout**height
        if not 0<=digit<self.fanout:return None
        value=node['slots'].get(str(digit))
        return self.lookup(value,key,start+digit*self.fanout**height,height-1) if height and value is not None else value

    def entries(self,desc,start,height,seen=None):
        if desc is None:return
        seen=set() if seen is None else seen
        if desc['path'] in seen:raise ValueError('duplicate or cyclic descriptor reference')
        seen.add(desc['path']);node=self.get(desc,start,height)
        for digit,value in sorted(node['slots'].items(),key=lambda row:int(row[0])):
            key=start+int(digit)*self.fanout**height
            if height:yield from self.entries(value,key,height-1,seen)
            else:yield key,value

    def changes(self,before,after,start,height):
        if canonical_bytes(before)==canonical_bytes(after):return
        if after is None:raise ValueError('append removes a descriptor path')
        a=self.get(before,start,height)['slots'] if before else {}
        b=self.get(after,start,height)['slots']
        if set(a)-set(b):raise ValueError('append removes a descriptor slot')
        for digit in sorted(set(a)|set(b),key=int):
            if canonical_bytes(a.get(digit))==canonical_bytes(b.get(digit)):continue
            key=start+int(digit)*self.fanout**height
            if height:yield from self.changes(a.get(digit),b.get(digit),key,height-1)
            else:yield key,a.get(digit),b.get(digit)


def _root(root):
    if not isinstance(root,dict) or set(root)!={'format','fanout','page_rows','global_count','owners','directory','parent_sha256'} or root['format']!='ownership-trie-v1':raise ValueError('invalid trie root')
    flat._integer(root['fanout'],2,16);flat._integer(root['page_rows'],1,flat.MAX_ROWS);flat._integer(root['global_count'])
    names=root['owners']
    if not isinstance(names,list) or not 1<=len(names)<=128 or any(not isinstance(n,str) or not flat.re.fullmatch('[A-Za-z0-9][A-Za-z0-9_.-]{0,95}',n) for n in names) or names!=sorted(set(names)):raise ValueError('invalid pinned owner directory')
    if root['parent_sha256'] is not None and (not isinstance(root['parent_sha256'],str) or not flat.re.fullmatch('[0-9a-f]{64}',root['parent_sha256'])):raise ValueError('invalid trie parent hash')


def _owner(value,name,root):
    if not isinstance(value,dict) or set(value)!={'owner','count','pages'} or value['owner']!=name:raise ValueError('owner path binding differs')
    flat._integer(value['count'])
    if (value['count']==0)!=(value['pages'] is None):raise ValueError('empty owner page root differs')


def _page(value,name,ordinal,count,root):
    if not isinstance(value,dict) or set(value)!={'owner','ordinal','path','sha256','bytes','start','rows'} or value['owner']!=name or type(value['ordinal']) is not int or value['ordinal']!=ordinal:raise ValueError('page owner or ordinal binding differs')
    expected=min(root['page_rows'],count-ordinal*root['page_rows'])
    if expected<=0 or type(value['start']) is not int or value['start']!=ordinal*root['page_rows'] or type(value['rows']) is not int or value['rows']!=expected or type(value['bytes']) is not int or value['bytes']!=expected*8:raise ValueError('page local range differs')
    data=flat._read(value)
    return data


@dataclass(frozen=True)
class AuditedRoot:
    encoded:bytes
    sha256:str
    def root(self):
        if hashlib.sha256(self.encoded).hexdigest()!=self.sha256:raise ValueError('audited trie root hash differs')
        root=json.loads(self.encoded);_root(root);return root


def _audit(root,collect_maps=False):
    _root(root);store=Store(root['fanout'],cache_nodes=0);seen=set();total=0;maps={}
    owners=list(store.entries(root['directory'],0,_depth(128,root['fanout'])))
    if [k for k,_ in owners]!=list(range(len(root['owners']))):raise ValueError('missing or additional owner path')
    for index,value in owners:
        name=root['owners'][index];_owner(value,name,root);count=0
        if collect_maps:maps[name]=[]
        for ordinal,page in store.entries(value['pages'],0,_depth(flat.MAX_ROWS,root['fanout'])):
            if ordinal!=count:raise ValueError('missing or additional page path')
            data=_page(page,name,ordinal,value['count'],root);store.metrics['page_read_bytes']+=len(data)
            for (ident,) in struct.iter_unpack('<Q',data):
                if ident>=root['global_count'] or ident in seen:raise ValueError('duplicate or out-of-range global ID')
                seen.add(ident)
                if collect_maps:maps[name].append(ident)
            count+=1
        if count!=(value['count']+root['page_rows']-1)//root['page_rows']:raise ValueError('owner page coverage differs')
        total+=value['count']
    if total!=root['global_count'] or len(seen)!=total:raise ValueError('global live coverage differs')
    data=canonical_bytes(root);return AuditedRoot(data,hashlib.sha256(data).hexdigest()),store.metrics,maps


def audit(root):
    receipt,metrics,_=_audit(root)
    return receipt,metrics


def audit_with_maps(root):
    """Complete bounded oracle; this deliberately materializes all local maps."""
    return _audit(root,collect_maps=True)


def durable(root):
    """Persist all reachable files before the existing deployment CAS.

    This full traversal remains an explicit integration cost until retention
    receipts support an independently reviewed changed-object durability proof.
    """
    from .dynamic_runtime import durable_file
    _root(root);store=Store(root['fanout'],cache_nodes=0);seen=set()
    def nodes(desc,start,height):
        if desc is None:return
        if desc['path'] in seen:raise ValueError('duplicate descriptor path')
        seen.add(desc['path']);node=store.get(desc,start,height);durable_file(Path(desc['path']))
        for digit,value in node['slots'].items():
            key=start+int(digit)*store.fanout**height
            if height:yield from nodes(value,key,height-1)
            else:yield key,value
    for index,owner in nodes(root['directory'],0,_depth(128,root['fanout'])):
        _owner(owner,root['owners'][index],root)
        for _,page in nodes(owner['pages'],0,_depth(flat.MAX_ROWS,root['fanout'])):
            durable_file(Path(page['path']))


class PinnedReader:
    def __init__(self,receipt,cache_nodes=32,cache_bytes=262144):
        if not isinstance(receipt,AuditedRoot):raise ValueError('full audited immutable root required')
        self.root=receipt.root();self.owners={n:i for i,n in enumerate(self.root['owners'])};self.store=Store(self.root['fanout'],cache_nodes,cache_bytes)
        self.root_owned_bytes=_owned(self.root)+_owned(self.owners)

    def lookup(self,owner,local):
        flat._integer(local)
        if owner not in self.owners:raise ValueError('unknown owner')
        record=self.store.lookup(self.root['directory'],self.owners[owner],0,_depth(128,self.root['fanout']));_owner(record,owner,self.root)
        if local>=record['count']:raise ValueError('unknown local ID')
        ordinal=local//self.root['page_rows'];value=self.store.lookup(record['pages'],ordinal,0,_depth(flat.MAX_ROWS,self.root['fanout']))
        data=_page(value,owner,ordinal,record['count'],self.root);self.store.metrics['page_read_bytes']+=len(data)
        return struct.unpack_from('<Q',data,(local%self.root['page_rows'])*8)[0]


def create(maps,directory,page_rows=128,fanout=4):
    flat._integer(fanout,2,16);directory=flat._path(str(directory));directory.mkdir(parents=True,exist_ok=False)
    old=flat.create(maps,directory/'pages',page_rows);store=Store(fanout);owners=None
    for index,owner in enumerate(old['owners']):
        pages=None
        for ordinal,page in enumerate(owner['pages']):
            pages=store.set(pages,ordinal,dict(page,owner=owner['owner'],ordinal=ordinal),0,_depth(flat.MAX_ROWS,fanout),directory)
        owners=store.set(owners,index,dict(owner=owner['owner'],count=owner['count'],pages=pages),0,_depth(128,fanout),directory)
    root=dict(format='ownership-trie-v1',fanout=fanout,page_rows=page_rows,global_count=old['global_count'],owners=[o['owner'] for o in old['owners']],directory=owners,parent_sha256=None)
    audit(root);return root


def verify_append(parent,candidate):
    if not isinstance(parent,AuditedRoot):raise ValueError('audited immutable parent required')
    old=parent.root();_root(candidate)
    if any(old[k]!=candidate[k] for k in ('owners','fanout','page_rows')) or candidate['parent_sha256']!=parent.sha256 or candidate['global_count']<=old['global_count']:raise ValueError('append changes pinned trie interpretation')
    store=Store(old['fanout'],cache_nodes=0);seen=set();growth=0;changed_pages=0
    for index,before,after in store.changes(old['directory'],candidate['directory'],0,_depth(128,old['fanout'])):
        if index>=len(old['owners']) or before is None:raise ValueError('append changes owner set')
        name=old['owners'][index];_owner(before,name,old);_owner(after,name,candidate)
        if after['count']<=before['count']:raise ValueError('changed owner does not grow')
        growth+=after['count']-before['count'];new_page_count=0
        for ordinal,a,b in store.changes(before['pages'],after['pages'],0,_depth(flat.MAX_ROWS,old['fanout'])):
            if a is not None and a['rows']==old['page_rows']:raise ValueError('append changes complete old page')
            data=_page(b,name,ordinal,after['count'],candidate);store.metrics['page_read_bytes']+=len(data);prefix=b''
            if a is not None:
                prefix=_page(a,name,ordinal,before['count'],old);store.metrics['page_read_bytes']+=len(prefix)
                if data[:len(prefix)]!=prefix:raise ValueError('append changes existing local prefix')
            else:
                if ordinal<(before['count']+old['page_rows']-1)//old['page_rows']:raise ValueError('append inserts page in old local range')
                new_page_count+=1
            for (ident,) in struct.iter_unpack('<Q',data[len(prefix):]):
                if not old['global_count']<=ident<candidate['global_count'] or ident in seen:raise ValueError('reserved global range is duplicate or out-of-range')
                seen.add(ident)
            changed_pages+=1
        expected_delta=(after['count']+old['page_rows']-1)//old['page_rows']-(before['count']+old['page_rows']-1)//old['page_rows']
        if new_page_count!=expected_delta:raise ValueError('append page coverage differs')
    if growth!=candidate['global_count']-old['global_count'] or len(seen)!=growth:raise ValueError('append loses reserved global IDs')
    data=canonical_bytes(candidate)
    return AuditedRoot(data,hashlib.sha256(data).hexdigest()),dict(store.metrics,changed_pages=changed_pages,new_rows=growth)


def append(parent,additions,directory):
    if not isinstance(parent,AuditedRoot):raise ValueError('audited immutable parent required')
    old=parent.root();names=old['owners'];values=[v for rows in additions.values() for v in rows];total=old['global_count']+len(values)
    if set(additions)-set(names) or not values or total>flat.MAX_ROWS or any(type(v) is not int or not old['global_count']<=v<total for v in values) or len(set(values))!=len(values):raise ValueError('complete new global interval required')
    directory=flat._path(str(directory));directory.mkdir(parents=True,exist_ok=False);store=Store(old['fanout']);candidate=copy.deepcopy(old);candidate.update(global_count=total,parent_sha256=parent.sha256)
    for name,new in sorted(additions.items()):
        if not new:continue
        index=names.index(name);owner=store.lookup(candidate['directory'],index,0,_depth(128,old['fanout']));_owner(owner,name,old)
        start=owner['count'];prefix=[];ordinal=start//old['page_rows']
        if start%old['page_rows']:
            last=store.lookup(owner['pages'],ordinal,0,_depth(flat.MAX_ROWS,old['fanout']))
            prefix=[v for (v,) in struct.iter_unpack('<Q',_page(last,name,ordinal,owner['count'],old))];start-=len(prefix)
        rows=prefix+list(new);root=owner['pages']
        for offset in range(0,len(rows),old['page_rows']):
            page=flat._page(directory,name,start+offset,rows[offset:offset+old['page_rows']]);ordinal=(start+offset)//old['page_rows']
            root=store.set(root,ordinal,dict(page,owner=name,ordinal=ordinal),0,_depth(flat.MAX_ROWS,old['fanout']),directory)
        updated=dict(owner=name,count=owner['count']+len(new),pages=root)
        candidate['directory']=store.set(candidate['directory'],index,updated,0,_depth(128,old['fanout']),directory)
    trusted,metrics=verify_append(parent,candidate)
    return candidate,trusted,dict(metrics,construction=store.metrics)
