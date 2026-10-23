import json,subprocess,time,resource,sys
from pathlib import Path
from harness_common import require,descriptor,write
ROWS=16384
# The measurement platform is fixed at import, independent of test runtime gates.
# Darwin reports bytes; Linux reports KiB. The runtime package runs on Linux.
_RSS_BYTES = sys.platform == 'darwin'
def child_peak_kib(usage):
    return usage.ru_maxrss / 1024 if _RSS_BYTES else usage.ru_maxrss


def checkpoint(x, native):
    pub=x['checkpoint_publication'];require(pub is not None and pub['generation']==1,'original checkpoint absent')
    path=Path(pub['manifest_path']);require(path==Path(native['output'])/'store/manifest.json','manifest output path')
    pinned=descriptor(path);require(pinned['sha256']==pub['manifest_sha256'] and pinned['bytes']==pub['manifest_bytes'],'original manifest hash')
    value=json.loads(path.read_text());require(value['generation']==1 and value['vector_count']==ROWS and value['source_id']==native['source_id'] and value['metric']=='ip' and value['dimension']==768,'checkpoint metadata')
    require(value['block_ids']==x['roots'] and value['construction']['config']==native['config'],'saved roots/config')
    require(value['construction_input']['prefix_sha256']==native['prefix_sha256'] and value['construction_input']['vectors']==native['vectors'] and value['construction_input']['spec_sha256']==x['spec_sha256'],'checkpoint input binding')
    mapping=value['id_mapping'];require(mapping==pub['id_mapping'] and mapping['path']=='id-map-1.u64le' and mapping['count']==ROWS and mapping['bytes']==ROWS*8,'dense map descriptor')
    map_path=path.parent/mapping['path'];mp=descriptor(map_path);require(mp['bytes']==mapping['bytes'] and mp['sha256']==mapping['sha256'],'persisted map binding')
    require(pub['map_write_bytes']==pub['map_verify_bytes']==ROWS*8,'map charge')
    return pinned,mp

def child(command, stem, out, remaining, env):
    require(remaining>0,'fixed deadline exhausted');start=time.monotonic();before=resource.getrusage(resource.RUSAGE_CHILDREN)
    try:
        with (out/(stem+'.stdout')).open('xb') as stdout,(out/(stem+'.stderr')).open('xb') as stderr:p=subprocess.run(command,stdout=stdout,stderr=stderr,env=env,timeout=remaining)
    except subprocess.TimeoutExpired:
        write(out/(stem+'-process.json'),dict(command=command,status='timeout',elapsed_seconds=time.monotonic()-start));raise
    after=resource.getrusage(resource.RUSAGE_CHILDREN);write(out/(stem+'-process.json'),dict(command=command,returncode=p.returncode,elapsed_seconds=time.monotonic()-start,user_cpu_seconds=after.ru_utime-before.ru_utime,system_cpu_seconds=after.ru_stime-before.ru_stime,observed_cumulative_child_maxrss_kib=child_peak_kib(after),voluntary_switches=after.ru_nvcsw-before.ru_nvcsw,involuntary_switches=after.ru_nivcsw-before.ru_nivcsw,scope='Whole child delta; no phase CPU attribution. Concurrent spans overlap.'))
    require(p.returncode==0,stem+' failed');require(child_peak_kib(after)<=8*1024**2,'observed8GiB cumulative child RSS ceiling');return json.loads((out/(stem+'.stdout')).read_text())
