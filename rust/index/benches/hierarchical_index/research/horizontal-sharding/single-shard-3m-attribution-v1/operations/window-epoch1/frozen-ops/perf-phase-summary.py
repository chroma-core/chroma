#!/usr/bin/env python3
"""Stream userspace perf stacks into bounded per-phase symbol summaries."""
import argparse,bisect,collections,json,pathlib,re,subprocess,sys
HEADER=re.compile(r'^\s*(\d+\.\d+):\s*cpu-clock(?::u)?:\s*(.*)$')
FRAME=re.compile(r'^\s*(?:0x)?[0-9a-fA-F]+\s+(.+?)\s*$')
def intervals(events):
 cuts=[];labels=[];cycle=0
 for e in events:
  kind=e['event']
  if kind=='add_start':cycle+=1;label=('add',cycle)
  elif kind=='add_complete':label=('balance',cycle)
  elif kind in ['commit_start','flush_start']:label=('save',None)
  else:label=('other',None)
  cuts.append(e['unix_seconds']);labels.append(label)
 assert cuts==sorted(cuts)
 return cuts,labels
def summarize(lines,events):
 cuts,labels=intervals(events);groups={};current=None;frames=[];total=0;unknown=0;unparsed=0;lost=0
 def finish():
  nonlocal total,unknown
  if current is None:return
  total+=1
  if total>2500000:raise RuntimeError('More than2.5Msamples; preserve rawprofile and stop summary')
  ix=bisect.bisect_right(cuts,current)-1;phase,cycle=labels[ix] if ix>=0 else ('other',None)
  leaf=frames[0] if frames else '[unknown]'
  if leaf=='[unknown]':unknown+=1
  names=[phase]+([f'{phase}:cycle{cycle:02d}'] if cycle is not None else [])
  for name in names:
   g=groups.setdefault(name,{'samples':0,'unknown_leaf_samples':0,'leaf':collections.Counter(),'inclusive':collections.Counter()});g['samples']+=1;g['unknown_leaf_samples']+=leaf=='[unknown]';g['leaf'][leaf]+=1
   for frame in set(frames):g['inclusive'][frame]+=1
 for line in lines:
  if 'PERF_RECORD_LOST' in line or 'lost' in line.lower():
   match=re.search(r'(?:lost[: ]+)(\d+)',line,re.I)
   if match:lost+=int(match.group(1))
   continue
  match=HEADER.match(line)
  if match:
   finish();current=float(match.group(1));frames=[];tail=match.group(2)
   if tail:
    f=FRAME.match(tail)
    if f:frames.append(re.sub(r'\+0x[0-9a-f]+.*$','',f.group(1)))
   continue
  if not line.strip():continue
  f=FRAME.match(line)
  if current is not None and f:
   if len(frames)>256:raise RuntimeError('Unexpectedstackdepth; rawprofile preserved')
   frames.append(re.sub(r'\+0x[0-9a-f]+.*$','',f.group(1)))
  else:unparsed+=1
 finish()
 return {'samples':total,'unknown_leaf_samples':unknown,'lost_samples_reported':lost,'unparsed_nonempty_lines':unparsed,'clock':'CLOCK_REALTIME','leaf_definition':'First recorded stack symbol; inclusivecounts one occurrence per symbol per sample.','groups':{k:{'samples':v['samples'],'unknown_leaf_samples':v['unknown_leaf_samples'],'top_leaf_symbols':v['leaf'].most_common(80),'top_inclusive_symbols':v['inclusive'].most_common(80)} for k,v in groups.items()}}
def main():
 p=argparse.ArgumentParser();p.add_argument('--perf',required=True);p.add_argument('--profile',required=True,type=pathlib.Path);p.add_argument('--events',type=pathlib.Path);p.add_argument('--output',required=True,type=pathlib.Path);a=p.parse_args()
 events=[json.loads(x) for x in a.events.read_text().splitlines()] if a.events else []
 stderr=a.output.with_suffix('.stderr')
 with stderr.open('w') as err:
  child=subprocess.Popen(['/usr/bin/timeout','--kill-after=5','120',a.perf,'script','--ns','--show-lost-events','--fields','time,event,ip,sym','--input',str(a.profile)],stdout=subprocess.PIPE,stderr=err,text=True)
  try:report=summarize(child.stdout,events)
  except BaseException:child.kill();child.wait();raise
  code=child.wait()
 if code:raise RuntimeError(f'perf script failed:{code}; see{stderr}')
 if report['samples']==0:raise RuntimeError('No recognizedCPU samples; rawprofilepreserved')
 a.output.write_text(json.dumps(report,indent=2)+'\n');print(json.dumps({k:v for k,v in report.items() if k!='groups'}))
if __name__=='__main__':main()
