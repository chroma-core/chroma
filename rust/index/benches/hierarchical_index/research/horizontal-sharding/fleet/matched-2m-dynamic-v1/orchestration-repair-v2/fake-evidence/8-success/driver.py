import json,sys
from pathlib import Path
s=json.loads(Path(sys.argv[2]).read_text());out=Path(s['integration_output']);out.joinpath('driver-entered.json').write_text('{}');original=json.loads(Path(s['fake_base_template']).read_text());assert {k:v for k,v in s.items() if k!='bound_inputs'}=={k:v for k,v in original.items() if k!='bound_inputs'};assert s['bound_inputs']['sha256'];
if s.get('fake_exit'):raise SystemExit(s['fake_exit'])
for name in s['fake_results']:
 p=Path(s['fake_experiment'])/name;p.parent.mkdir(parents=True,exist_ok=True);p.write_text('{}')
