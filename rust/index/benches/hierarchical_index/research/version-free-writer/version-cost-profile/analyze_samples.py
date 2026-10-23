import re,json,bisect,collections
from pathlib import Path
p=Path(__file__).parent/'artifacts'
addr={};key=None
for line in (p/'address-lines.txt').read_text().splitlines():
 if re.fullmatch('0x[0-9a-f]+',line):key=int(line,16);addr[key]=[]
 elif key is not None:addr[key].append(line)
bias=int(json.loads((p/'address-relocation.json').read_text())['load_bias'],16)
log=(p/'wikipedia-instrumented-1m-2.log').read_text()
marks=[(float(t),v) for t,v in re.findall(r'VERSION_PROFILE_MARK unix=([\d.]+) label=(.+)',log)]
phases=[(t,v) for t,v in marks if not v.endswith(('_BEGIN','_END'))]
ts=[v[0] for v in phases]
include={'Add vectors','Balance','Commit: fork','Commit: flush','Reopen writer','Drop old writer'}
counts=collections.Counter();byphase=collections.defaultdict(collections.Counter);lines=collections.Counter();quality=collections.Counter();total=0
for block in (p/'samples-symbol-offsets.txt').read_text().split('\n\n'):
 m=re.match(r'([\d.]+):\s+(\d+)',block)
 if not m:continue
 t,period=float(m[1]),int(m[2])/1e9
 phase=phases[bisect.bisect_right(ts,t)-1][1] if ts and t>=ts[0] else 'before_markers'
 byphase[phase]['all']+=period
 if phase not in include:continue
 total+=1
 frames=[]
 for ln in block.splitlines()[1:]:
  ip=re.match(r'\s*([0-9a-f]+) ',ln)
  frames.append(addr.get(int(ip[1],16)-bias,[]) if ip and 'instrumented-binary)' in ln else [])
 text='\n'.join(sum(frames,[]))+'\n'+block
 version=any(s in text for s in ['current_version_sync','VersionCache3get','VersionCache6insert','prefetch_versions'])
 scrub='5scrub' in text or '::scrub+' in text
 # Exact inline source locations for operations on version maps; exclude embedding insertion and quantization.
 source_lines=[]
 for frame in frames:
  for ln in frame:
   z=re.search(r'hierarchical_index/writer/writer.rs:(\d+)',ln)
   if z:source_lines.append(int(z[1]))
 update=any(n in range(489,500) or n in range(747,752) or n in [180,181] for n in source_lines)
 dirty=any(n in [180,181] for n in source_lines)
 if version:counts['version_lookup_cache_prefetch_inclusive']+=period
 if 'current_version_sync' in text:counts['current_version_sync_inclusive_subset']+=period
 if 'VersionCache3get' in text or 'VersionCache::get' in text:counts['cache_get_inclusive_subset']+=period
 if 'prefetch_versions' in text:counts['prefetch_inclusive_subset']+=period
 if update:counts['version_update_or_dirty_source_evidence']+=period
 if dirty:counts['dirty_tracking_subset']+=period
 if scrub:counts['scrub_inclusive']+=period
 if scrub and version:counts['scrub_lookup_subset']+=period
 if scrub and not version:counts['scrub_without_resolved_lookup']+=period
 if version or update:counts['version_lookup_update_union']+=period
 if not frames or not frames[0]:quality['unresolved_leaf_samples']+=1
 if '[unknown]' in block:quality['samples_with_unknown_frame']+=1
 if len(frames)<=1:quality['single_frame_samples']+=1
 for ln in (frames[0] if frames else []):
  if 'hierarchical_index/writer/' in ln:lines[ln]+=period
 counts['all']+=period
 if version:byphase[phase]['version_lookup_cache_prefetch_inclusive']+=period
out={'eligible_samples':total,'period_seconds':dict(counts),'phase_period_seconds':dict(byphase),'quality':dict(quality),'leaf_source_period_seconds':dict(lines.most_common()),'method':'49Hz user CPU-clock periods; inclusive callchain union, exact source inline attribution; phases exclude benchmark diagnostics and input load. CPU periods are not elapsed time. Unknown frames unassigned.'}
(p/'sample-attribution.json').write_text(json.dumps(out,indent=2)+'\n')
print(json.dumps({k:v for k,v in out.items() if k!='leaf_source_period_seconds'},indent=2))
print('TOP SOURCE',list(lines.most_common(25)))
