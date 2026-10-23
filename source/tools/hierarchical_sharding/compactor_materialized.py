"""Validate a trusted internal compactor's exact materializer export.

File/source receipts establish local fixture provenance, not remote authentication.
The delegate retains the existing collection registration authority.
"""
import copy
import json
import math
from pathlib import Path
import struct
from .compactor_registration import HIERARCHY_BUNDLE
from .dynamic_deployment import validate_record
from .tree_manifest import checked_artifact,canonical_bytes,content_hash,file_hash

PRODUCERS=(
    'rust/segment/src/types.rs',
    'rust/worker/src/execution/orchestration/hierarchical_materialized_input.rs',
    'rust/index/tests/hierarchical_materialized_input.rs',
)
TOP_FIELDS={'collection_id','collection_version','parent_log_position','pulled_log_offset','parent_deployment_sha256','ownership_generation','rows'}
ROW_FIELDS={'log_offset','global_id','record_offset_id','user_id','embedding_bits'}
def integer(v,maximum=2**63-1,minimum=0):return type(v) is int and minimum<=v<=maximum

def load_json(path):
    def pairs(items):
        result={}
        for key,value in items:
            if key in result:raise ValueError('duplicate materializer JSON key')
            result[key]=value
        return result
    return json.loads(Path(path).read_text(),object_pairs_hook=pairs,parse_constant=lambda value:(_ for _ in ()).throw(ValueError('nonfinite JSON constant')))

def checked(desc,maximum):
    if not isinstance(desc,dict) or set(desc)!={'path','bytes','sha256'} or not isinstance(desc['path'],str) or not Path(desc['path']).is_absolute() or not integer(desc['bytes'],maximum) or not isinstance(desc['sha256'],str) or len(desc['sha256'])!=64 or any(c not in '0123456789abcdef' for c in desc['sha256']):raise ValueError('strict materializer artifact descriptor required')
    return checked_artifact(desc,'.')

class MaterializedAuthorityAdapter:
    def __init__(self,delegate,export,receipt):
        self.delegate=delegate;self.export=copy.deepcopy(export);self.receipt=copy.deepcopy(receipt)
        self.context=None;self.binding=None
    def __getattr__(self,name):return getattr(self.delegate,name)
    def capture(self,collection_id):
        captured=copy.deepcopy(self.delegate.capture(collection_id))
        if self.context is not None and canonical_bytes(captured)!=canonical_bytes(self.context):raise ValueError('captured compaction context changed')
        self.context=captured
        return copy.deepcopy(captured)
    def _load(self):
        export_path=checked(self.export,512*1024*1024);receipt_path=checked(self.receipt,64*1024)
        proof=load_json(receipt_path)
        if set(proof)!={'format_version','export','fixture_input','producer_sources','runtime_binary'} or proof['format_version']!=1 or type(proof['format_version']) is not int or proof['export']!=self.export:raise ValueError('materializer receipt differs')
        checked(proof['fixture_input'],64*1024);checked(proof['runtime_binary'],256*1024*1024)
        if set(proof['producer_sources'])!=set(PRODUCERS):raise ValueError('complete materializer producer closure required')
        root=Path(__file__).resolve().parents[2]
        if any(file_hash(root/name)!=proof['producer_sources'][name] for name in PRODUCERS):raise ValueError('materializer producer source changed')
        value=load_json(export_path)
        if not isinstance(value,dict) or set(value)!=TOP_FIELDS:raise ValueError('exact materialized source schema required')
        return value
    def source_logs(self,context):
        if self.context is None or canonical_bytes(context)!=canonical_bytes(self.context):raise ValueError('source context differs from captured compaction')
        value=self._load();c=context['parent']['collection'];job=context['compaction']
        segment=next(s for s in context['parent']['segments'] if s['segment_id']==context['vector_segment_id'])
        references=segment['file_paths'][HIERARCHY_BUNDLE]
        if len(references)!=1:raise ValueError('one registered hierarchy descriptor required')
        descriptor=json.loads(references[0]);loaded=json.loads(checked_artifact(descriptor,'.').read_text());parent=loaded.get('deployment',loaded);validate_record(parent)
        expected=dict(collection_id=c['collection_id'],collection_version=c['version'],parent_log_position=c['log_position'],pulled_log_offset=job['pulled_log_offset'],parent_deployment_sha256=content_hash(parent),ownership_generation=parent['fence'])
        if any(value[k]!=v or type(value[k]) is not type(v) for k,v in expected.items()):raise ValueError('materialized source collection/log/ownership fence differs')
        rows=value['rows'];count=job['pulled_log_offset']-c['log_position'];first=parent['tree']['source']['row_count'];dimension=parent['tree']['dimension']
        if not isinstance(rows,list) or len(rows)!=count or not 0<count<=100000 or first+count>100000 or count*dimension*4>512*1024*1024:raise ValueError('materialized source row/payload budget differs')
        users=set();result=[]
        for i,row in enumerate(rows):
            if not isinstance(row,dict) or set(row)!=ROW_FIELDS or not integer(row['log_offset']) or row['log_offset']!=c['log_position']+i+1 or not integer(row['global_id'],2**32-1) or not integer(row['record_offset_id'],2**32-1) or row['record_offset_id']!=row['global_id'] or row['global_id']!=first+i or not isinstance(row['user_id'],str) or not row['user_id'] or row['user_id'] in users:raise ValueError('materialized source ID/offset/user differs')
            bits=row['embedding_bits']
            if not isinstance(bits,list) or len(bits)!=dimension or any(not integer(v,2**32-1) for v in bits):raise ValueError('materialized float32 bits differ')
            vector=list(struct.unpack('<'+'f'*dimension,struct.pack('<'+'I'*dimension,*bits)))
            if any(not math.isfinite(v) for v in vector):raise ValueError('materialized vector is nonfinite')
            users.add(row['user_id']);result.append(dict(operation='add',materialized_operation='add_new',log_offset=row['log_offset'],global_id=row['global_id'],user_id=row['user_id'],embedding=vector))
        self.binding=dict(export=self.export,receipt=self.receipt,registered_parent=descriptor,parent_deployment_sha256=content_hash(parent))
        return result
    def source_binding(self,context):
        # Recheck actual files and exact capture on every preparation health check.
        self.source_logs(context)
        return copy.deepcopy(self.binding)
