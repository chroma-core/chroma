import importlib.util,copy,hashlib
from pathlib import Path
P=Path(__file__).resolve().parent
s=importlib.util.spec_from_file_location('base',P/'make-3m-spec-epoch29.py');g=importlib.util.module_from_spec(s);s.loader.exec_module(g)
s=importlib.util.spec_from_file_location('job',P/'remote-job-epoch29-validation.py');remote=importlib.util.module_from_spec(s);s.loader.exec_module(remote)
WORKER=g.WORKER;PEER=g.PEER;BASE=g.BASE
def generate(a,now):
 assert a.arm in ('storage100k','3m-final')
 base=copy.copy(a);base.arm='3m-distributed';v=g.generate(base,now)
 owner=1 if a.mode=='run' else 2;name=a.arm+'-owner'+str(owner)
 v['job_id']=name;v['args']['--output']=BASE+'/'+name
 kind='smoke' if a.arm=='storage100k' else 'final'
 config=P/('storage-'+kind+'-config-epoch29.json')
 v['args']['--storage-config']=BASE+'/storage-'+kind+'-config.json';v['storage_config_sha256']=hashlib.sha256(config.read_bytes()).hexdigest()
 if kind=='smoke':v['args']['--rows']=100000;v['args']['--owner-budget']=72000
 v['provenance']['arm']=a.arm;v['provenance']['rows']=v['args']['--rows'];v['provenance']['owner_capacity']=v['args']['--owner-budget'];v['provenance']['question']='Physical owner-direct and unchanged-save correctness' if kind=='smoke' else 'Repeat same3Msource after preserved construction outlier; no pre-save algorithm difference from corrected28'
 remote.validate(v,now);return v
