"""Bounded GT set joins outside native query timing; never changes decisions."""
import struct,hashlib,time
from harness_common import require

def mapping(path,expected):
 raw=path.read_bytes();require(len(raw)==expected['bytes']==65536 and hashlib.sha256(raw).hexdigest()==expected['sha256'],'consumed identity map binding');values=struct.unpack('<8192Q',raw);require(values==tuple(range(8192)),'dense original mapping differs');return values

def join_trace(response,gt_ids,leaves,id_map):
 started=time.monotonic_ns();trace=response.get('query_stage_trace');require(response.get('query_stage_trace_policy')=='bounded-query-stages-v1' and trace is not None and trace['complete'] is True and trace['id_cap']==8192,'stage trace missing/incomplete')
 counts=response['query_work']['counts'];selected=trace['selected_leaf_ids'];pre=trace['pretrim_candidate_ids'];retained=trace['retained_candidate_ids'];require(len(selected)==counts['selected_leaves'] and len(pre)==counts['distinct_candidates_before_trim'] and len(retained)==counts['candidates_after_trim'],'stage cardinality counters')
 require(all(len(x)<=8192 and len(x)==len(set(x)) for x in [selected,pre,retained]),'stage cap/duplicate IDs')
 require(all(type(i) is int and 0<=i<=2**32-1 and i in leaves for i in selected),'unknown selected leaf')
 require(all(type(i) is int and 0<=i<len(id_map) for i in pre+retained),'candidate local-ID range');require(set(retained)<=set(pre),'retained outside pretrim')
 gt=set(gt_ids);selected_gt=set().union(*(set(leaves[i])&gt for i in selected));pre_ids={id_map[i] for i in pre};retained_ids={id_map[i] for i in retained};returned=set(response['ids']);require(returned<=retained_ids,'returned outside exact candidates')
 pre_gt=pre_ids & gt;retained_gt=retained_ids & gt;returned_gt=returned & gt;require(pre_gt==selected_gt,'saved leaf GT and actual pretrim GT disagree');require(returned_gt<=retained_gt<=pre_gt,'GT stage order')
 value=dict(expected=len(gt),selected_leaf_gt_hits=len(selected_gt),pretrim_gt_hits=len(pre_gt),retained_candidate_gt_hits=len(retained_gt),returned_gt_hits=len(returned_gt),lost_at_navigation=len(gt)-len(selected_gt),lost_at_candidate_trim=len(pre_gt)-len(retained_gt),lost_at_exact_return=len(retained_gt)-len(returned_gt),native_trace_copy_ns=trace['copy_elapsed_ns'],join_elapsed_ns=time.monotonic_ns()-started,scope='ActualchosenlocalIDs mapped with originalidentitymap; savedleafGTjoin outsidequery. Selected==pretrimGT is a consistencycheck, not an assumed loss stage. Exact-return losses can include FP32vsFP64 order/ties. Native elapsed includes trace work and is not comparable to prior latency.')
 require(sum(value[k] for k in ['lost_at_navigation','lost_at_candidate_trim','lost_at_exact_return'])==len(gt)-len(returned_gt),'loss partition');return value
