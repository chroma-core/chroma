"""Count each closed, API-stopped task window once; retain the prior ledger scope."""
import datetime
import hashlib
import json
from decimal import Decimal
from pathlib import Path
base=Path(__file__).resolve().parent
root=base.parent
ledger_path=root/'SPEND-LEDGER.json'
ledger=json.loads(ledger_path.read_text())
summary=ledger['covered_compute_summary']
changed=False
for window in sorted((base/'operations').glob('window-epoch*')):
    closed=window/'closed.json'
    if not closed.exists():continue
    receipt=json.loads(closed.read_text(),parse_float=Decimal)
    if receipt['api_state']!='stopped':raise SystemExit(f'Unclosed compute: {window}')
    relative=str(closed.relative_to(root))
    if any(c.get('source')==relative for c in ledger['commitments']):continue
    amount=Decimal(str(receipt['compute_usd_upper_bound']))
    ledger['commitments'].append({'experiment':'single-shard-redesign-v1-'+window.name,'status':'api-stopped-see-window-receipts','compute_upper_bound_usd':float(amount),'exact_compute_bound_usd':str(amount),'additional_storage_monthly_usd':0,'source':relative})
    for key in ['compute_upper_bound_usd','first_month_compute_plus_reserved_storage_usd']:
        summary[key]=str(Decimal(summary[key])+amount)
    summary['approval_minus_covered_first_month_bound_usd']=str(Decimal(str(ledger['approval_usd']))-Decimal(summary['first_month_compute_plus_reserved_storage_usd']))
    data=closed.read_bytes()
    summary['sources'].append({'path':relative,'bytes':len(data),'sha256':hashlib.sha256(data).hexdigest()})
    changed=True
if changed:
    ledger['reconciled_at']=datetime.datetime.now(datetime.timezone.utc).isoformat()
    ledger['note']='Covered task compute receipts are counted once and are not a whole-account bill. Retained recurring storage components remain separately identified in covered_compute_summary.'
    ledger_path.write_text(json.dumps(ledger,indent=2)+'\n')
print(json.dumps({k:v for k,v in summary.items() if k!='sources'},indent=2))
