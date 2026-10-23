from harness_common import require
ROWS=16384
DIM=768
def shape(x,arm):
    c=x['census'];require(c['unique_live_rows']==ROWS and c['duplicate_current_entries']==c['unreachable_nodes']==0,'full saved census')
    require([v['rows'] for v in x['phases']]==[4096,8192,12288,16384],'common balance cuts')
    require(x['source_read_chunk_rows']==8192 and x['source_read_calls']==2 and x['source_prefix_read_bytes']==ROWS*DIM*4,'input read chunks')
    timing=x['timing_accounting'];parts=x['input_seconds']+timing['setup_seconds']+timing['phase_elapsed_seconds']+x['save_seconds']+x['audit_seconds']+timing['publication_seconds']+timing['other_before_serialize_seconds']
    require(abs(parts-timing['native_before_serialize_seconds'])<1e-6,'native phase accounting')
    if arm=='reference':
        s=x['reference_shape'];require(s['nodes']==s['reachable_nodes'] and s['pending_balance']==0 and s['depth']>=2,'reference shape')
    else:
        s=x['owner_report'];require(s['schedule_sample_overflow'] is False and sum(bool(v) for v in s['last_reference_subtree_assignments'])>=2 and s['peak_distinct_subtree_workers']>=2,'multiple scheduler regions absent')
        require(s['pending_balance']==s['central_current_versions']==s['central_mutable_posting_rows']==0,'unfinished balance/central mirror')
        require(s['predicate_batching'] is True and s['batched_scrub_validation'] is True and s['direct_current_policy'] is False and s['direct_current_reads']==0,'combined owner policy')
        require(s['predicate_batch_computed_rows']>0 and s['scrub_validation_rows']>0,'owner work absent')
        require(x['construction_policy']=='owner-batched-v1','owner echo')
        if arm=='dynamic1':require(s['placement_moves']==0 and sum(v['current']>0 for v in s['owners'])==1,'oneowner migration')
        else:require(s['placement_moves']>0 and sum(v['current']>0 for v in s['owners'])==4,'fourowner placement absent')
