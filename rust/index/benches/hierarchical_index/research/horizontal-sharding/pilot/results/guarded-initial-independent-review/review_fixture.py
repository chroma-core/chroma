import tarfile,json,hashlib,struct,re
from pathlib import Path
p=Path(__file__).resolve().parents[1];a=p/'guarded-initial-fixture-small-v1.tar.gz';base='results/guarded-initial-fixture23001-v1/'
with tarfile.open(a) as t:
 read=lambda n:t.extractfile(n).read()
 load=lambda n:json.loads(read(n))
 terminal=load(base+'build/host-batch/terminal-0.json');assert terminal['status']=='passed' and terminal['reconciled'] and all(x['status']=='ok' and not x['unreconciled_children'] for x in terminal['guard_report']['outcomes'])
 config=load(base+'build/host-0-config.json');assert config['cpu_affinity']=='0-7,32-39' and config['lease_ttl_s']==60 and config['guard']['concurrency']==16
 maps=[];flushes=0
 inspections=load(base+'remote-reopen.json')
 for i,count in [(0,13095),(1,9906)]:
  m=load(base+f'copies/{i}/manifest-1.json');raw=read(base+f'copies/{i}/id-map-1.u64le');assert m['generation']==1 and m['vector_count']==count
  assert hashlib.sha256(raw).hexdigest()==m['id_mapping']['sha256'];maps.extend(struct.unpack('<'+str(count)+'Q',raw))
  proof=m['integrity_audit'];assert proof['generation']==1 and proof['block_ids']==m['block_ids'] and proof['coverage']['unique_live_rows']==count and proof['id_map_sha256']==m['id_mapping']['sha256']
  row=next(x for x in inspections if x['subtree_id']==str(i));assert row['snapshot_sha256']==hashlib.sha256(read(base+f'copies/{i}/manifest-1.json')).hexdigest() and row['root_response']['generation']==1
  cmd=config['guard']['commands'][i];assert cmd[cmd.index('--checkpoint-rows')+1]=='1000' and cmd[cmd.index('--threads')+1]=='1'
  batches=list(map(int,re.findall(r'committed build batch for generation 1: (\d+) vectors',read(base+f'build/{i}.stderr').decode())));assert batches==list(range(1000,count,1000))+[count];flushes+=len(batches)
 assert sorted(maps)==list(range(23001));tree=read(base+'tree-1.json');pointer=load(base+'current.json');assert hashlib.sha256(tree).hexdigest()==pointer['tree_sha256']
 final=terminal['guard_report']['final_inventory'];assert final['aggregate_writer_rss_bytes']==0 and not final['writer_processes']
 report=dict(status='passed',rows=23001,owners=2,batch_commits=flushes,final_generation=1,full_dense_global_maps=True,terminal_reconciled=True,resource_limits=config['guard']['limits'],final_inventory=final,archive_sha256=hashlib.sha256(a.read_bytes()).hexdigest(),limits=['Real fixture uses1000row checkpoints to force24commits. Future10M config must explicitly use10000.','Physical object reachability inventory remains separate; guard inventories every regular ownedfile includingprivate leftovers.','Concrete10M placement/scopes/headroom admission remains pending.'])
(p/'guarded-initial-independent-review/fixture-review.json').write_text(json.dumps(report,indent=2)+'\n');print(report['status'],flushes)
