"""Classify discarded private effects, without asserting that a mutation did not run."""
import re
from .tree_manifest import canonical_bytes,content_hash

CLASSIFICATION='discarded-private-with-unknown-effects'
TERMINAL_FIELDS=dict(classification=CLASSIFICATION,commit_started=False,failed=True,threads_stopped=True,
                     quarantined=True,eligible_replica=False,eligible_generation=False)

def validate_terminal_fields(receipt):
    if (not isinstance(receipt,dict) or canonical_bytes({k:receipt.get(k) for k in TERMINAL_FIELDS})!=canonical_bytes(TERMINAL_FIELDS)
            or type(receipt.get('returncode')) is not int):
        raise ValueError('private discard receipt lacks exact terminal classification')

def command_identity(payload):
    ident=payload.get('operation_id')
    if type(ident) is not str or re.fullmatch('[0-9a-f]{32}',ident) is None:
        raise ValueError('exact reserved command operation required')
    if payload.get('op')!='selected-write-command' or not isinstance(payload.get('command'),dict):
        raise ValueError('selected writer command required')
    if len(canonical_bytes(payload))>256*1024:raise ValueError('command payload exceeds admission')
    return dict(operation_id=ident,payload_sha256=content_hash(payload))

def validate_discarded(receipt,operation,session,job_id,session_id):
    expected=dict(job_id=job_id,session_id=session_id,handle=session['handle'],replica_id=session['replica_id'],spec_sha256=session['spec_sha256'])
    validate_terminal_fields(receipt)
    if (canonical_bytes({key:operation.get(key) for key in expected})!=canonical_bytes(expected)
            or operation.get('worker')!=session.get('worker')):
        raise ValueError('uncertain command scope differs from the owned session')
    if (not isinstance(receipt,dict) or receipt.get('classification')!=CLASSIFICATION
            or canonical_bytes(receipt.get('scope'))!=canonical_bytes(expected)
            or receipt.get('commit_started') is not False or receipt.get('failed') is not True
            or receipt.get('threads_stopped') is not True or type(receipt.get('returncode')) is not int
            or receipt.get('quarantined') is not True or receipt.get('eligible_replica') is not False
            or receipt.get('eligible_generation') is not False):
        raise ValueError('private discard receipt is not exact terminal precommit evidence')
    wanted={key:operation[key] for key in ('operation_id','payload_sha256')}
    matches=[row for row in receipt.get('commands',[]) if row.get('operation_id')==wanted['operation_id']]
    if len(matches)!=1 or canonical_bytes(matches[0])!=canonical_bytes(wanted):
        raise ValueError('private discard receipt does not bind exact uncertain command')
    return CLASSIFICATION
