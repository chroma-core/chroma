from pathlib import Path
import difflib
r=Path('/mnt/msmarco/version-free-followup-20261020');p=r/'replay/rust/index/benches/hierarchical_index/writer/writer.rs';before=p.read_text()
a=before.index('            let worker_start = Instant::now();',before.index('    fn balance_parallel_inner'))
b=before.index('            self.stats.balance_iterations.lock().push(iteration);',a)
new='''            let (over,under) = self.nodes.iter().fold((0usize,0usize), |(o,u),e| {
                if let TreeNode::Leaf(l)=e.value() { (o+usize::from(l.length>self.config.split_threshold),u+usize::from(l.length>0 && l.length<self.config.merge_threshold)) } else {(o,u)}
            });
            let split_before=self.stats.splits.load(Ordering::Relaxed);
            let merge_before=self.stats.merges.load(Ordering::Relaxed);
            println!("REPLAY_ROUND_START round={} unix={:.6} over={} under={} partition_work={:?} assigned_work={:?}", round, std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap().as_secs_f64(), over,under,partitions.iter().map(|p|p.1).collect::<Vec<_>>(),thread_work);
            let worker_start = Instant::now();
            let worker_details = std::thread::scope(|scope| {
                let handles: Vec<_> = thread_subtrees.iter().enumerate().filter(|(_,s)|!s.is_empty()).map(|(worker,subtrees)| {
                    scope.spawn(move || {
                        let start=Instant::now(); let mut visited=0usize;let mut eligible=0usize;
                        for &subtree_root in subtrees {
                            for leaf_id in self.collect_leaves_under(subtree_root) {
                                visited+=1;
                                eligible+=usize::from(self.nodes.get(&leaf_id).is_some_and(|e|match e.value(){TreeNode::Leaf(l)=> l.length>self.config.split_threshold || (l.length>0 && l.length<self.config.merge_threshold),_=>false}));
                                self.balance(leaf_id,0);
                            }
                        }
                        (worker,start.elapsed().as_nanos() as u64,visited,eligible)
                    })
                }).collect();
                handles.into_iter().map(|h|h.join().unwrap()).collect::<Vec<_>>()
            });
            iteration.worker_wall_nanos=worker_start.elapsed().as_nanos() as u64;
            iteration.worker_sum_nanos=worker_details.iter().map(|x|x.1).sum();
            iteration.worker_max_nanos=worker_details.iter().map(|x|x.1).max().unwrap_or(0);
            println!("REPLAY_ROUND_END round={} unix={:.6} splits={} merges={} worker_details_id_nanos_visited_eligible={:?}",round,std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap().as_secs_f64(),self.stats.splits.load(Ordering::Relaxed)-split_before,self.stats.merges.load(Ordering::Relaxed)-merge_before,worker_details);
'''
after=before[:a]+new+before[b:];p.write_text(after);(r/'artifacts/replay-writer-only.patch').write_text(''.join(difflib.unified_diff(before.splitlines(True),after.splitlines(True),fromfile='a/rust/index/benches/hierarchical_index/writer/writer.rs',tofile='b/rust/index/benches/hierarchical_index/writer/writer.rs')))
