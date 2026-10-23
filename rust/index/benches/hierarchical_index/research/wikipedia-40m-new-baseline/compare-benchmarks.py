import pathlib,re,json,csv
import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt
import numpy as np
root=pathlib.Path(__file__).parent
sec=lambda x:float(x[:-2])/1000 if x.endswith('ms') else float(x[:-1])*(60 if x.endswith('m') else 1)
old=(root/'historical-hierarchical-wikipedia.md').read_text()
pattern=r'--- Checkpoint (\d+) .*?\n\s*Indexed 1.00M vec in ([\d.]+[sm]).*?\| balance ([\d.]+[sm]) \| load ([\d.]+[sm])'
h=[{'checkpoint':int(c),'add_s':sec(a),'balance_s':sec(b),'load_s':sec(l),'commit_s':None} for c,a,b,l in re.findall(pattern,old)]
assert len(h)==40
current=(root/'full-cli-output.txt').read_text()
pattern=r'--- Checkpoint (\d+) .*?\n\s*Indexed 1.00M vec in ([\d.]+[sm]).*?\| balance ([\d.]+[sm]) \((\d+) iterations\) \| load ([\d.]+[sm]) \| commit ([\d.]+[sm]) \| reopen ([\d.]+(?:ms|s))'
n=[{'checkpoint':int(c),'add_s':sec(a),'balance_s':sec(b),'iterations':int(i),'load_s':sec(l),'commit_s':sec(t),'reopen_s':sec(o)} for c,a,b,i,l,t,o in re.findall(pattern,current)]
assert len(n)==40
q=json.loads((root/'historical-quantized-wikipedia-40m.json').read_text())['checkpoints']
for row in h+n:row['compute_s']=row['add_s']+row['balance_s']
for row in q:row['compute_s']=row['index_s']
(root/'comparison-data.json').write_text(json.dumps({'new_baseline':n,'historical_hierarchical':h,'historical_quantized':q},indent=2))
with (root/'new-baseline-checkpoints.csv').open('w') as f:
 w=csv.DictWriter(f,fieldnames=n[0].keys());w.writeheader();w.writerows(n)
plt.rcParams.update({'font.family':'DejaVu Sans','font.size':10,'axes.spines.top':False,'axes.spines.right':False,'axes.titleweight':'bold'})
fig,axs=plt.subplots(1,3,figsize=(16,5.4));colors=['#087f8c','#6f59a8']
for rows,label,col in [(n,'New 40M baseline',colors[0]),(q,'Historical Quantized SPANN',colors[1])]:
 x=[r['checkpoint'] for r in rows];vals=[r['compute_s'] for r in rows]
 axs[0].plot(x,np.cumsum(vals)/60,label=label,color=col,lw=2.4)
 axs[1].plot(x,vals,color=col,lw=2.2)
 axs[2].plot(x,[r['commit_s'] for r in rows],color=col,lw=2.2)
axs[0].set_title('Cumulative index computation');axs[0].set_ylabel('Minutes; excludes input loading and commits');axs[0].legend(loc='upper left',fontsize=9)
axs[1].set_title('Computation for each 1M increment');axs[1].set_ylabel('Seconds; add + balance, or integrated index')
axs[2].set_title('Commit for each 1M increment');axs[2].set_ylabel('Seconds')
for ax in axs:ax.set_xlabel('Index size (millions of vectors)');ax.set_xlim(1,40);ax.grid(axis='y',alpha=.2)
fig.suptitle('Wikipedia indexing: new baseline vs historical Quantized SPANN',fontsize=17,fontweight='bold',y=.97)
fig.text(.05,.025,'Historical observations, not a matched experiment. The quantized reference does not document hardware or thread count.\nNew run: 32 threads, one replica, split/merge 2048/512, RNG factor 0. Quantized run: two replicas, split/merge 512/128, RNG factor 4.\nComputation includes balancing in both implementations; the quantized run does not time balancing separately. Minute timings are rounded.',fontsize=9,color='#444444')
fig.tight_layout(rect=(0,.15,1,.91));fig.savefig(root/'wikipedia-build-comparison.png',dpi=180);fig.savefig(root/'wikipedia-build-comparison.svg')
print(json.dumps({name:{'compute_minutes':sum(r['compute_s'] for r in rows)/60,'commit_minutes':sum(r['commit_s'] for r in rows)/60 if rows[0]['commit_s'] is not None else None} for name,rows in [('new',n),('historical_hierarchical',h),('historical_quantized',q)]},indent=2))
