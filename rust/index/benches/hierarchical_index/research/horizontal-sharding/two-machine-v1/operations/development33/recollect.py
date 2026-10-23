from run import P,ssh,B,worker,peer
import sys,json,shlex,concurrent.futures
name=sys.argv[1];local=P/name
assert local.is_dir()
def collect(pair):
 iid,mode,owner=pair;spec=json.loads((local/(mode+'.json')).read_text());output=spec['argv'][spec['argv'].index('--output')+1]
 code="import pathlib,json; roots=[pathlib.Path("+repr(output)+"),pathlib.Path("+repr(spec['evidence'])+")]; print(json.dumps({str(p):p.read_text() for root in roots if root.exists() for p in root.iterdir() if p.is_file() and p.name!='perf.data' and p.stat().st_size<200000000}))"
 target=local/(f'owner{owner}-receipts.json');target.write_text(ssh(iid,'python3 -c '+shlex.quote(code),timeout=180));print('COLLECTED',name,owner,flush=True)
with concurrent.futures.ThreadPoolExecutor(2) as pool:list(pool.map(collect,[(worker,'run',1),(peer,'serve',2)]))
