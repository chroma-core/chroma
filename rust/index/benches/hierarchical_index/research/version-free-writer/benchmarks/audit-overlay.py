from pathlib import Path
import difflib,sys
r=Path('/mnt/msmarco/version-free-writer-7927');variant=sys.argv[1]; root=r/variant
p=root/'rust/index/benches/hierarchical_index/writer/diagnostics.rs';before=p.read_text()
method='''
    pub fn benchmark_audit_samples(&self, samples: &[(u32, Vec<f32>)], total: usize) -> Result<(), String> {
        let ids = self.root_reachable_valid_ids()?;
        if ids.len() != total || (0..total as u32).any(|id| !ids.contains(&id)) { return Err("audit ID coverage mismatch".into()); }
        let sample_map: HashMap<u32, &Vec<f32>> = samples.iter().map(|(id,v)| (*id,v)).collect();
        let mut seen = HashSet::new();
        let mut physical = 0usize;
        let mut live = 0usize;
        for entry in self.nodes.iter() {
            if let TreeNode::Leaf(leaf) = entry.value() {
                physical += leaf.ids.len();
                for (slot,&id) in leaf.ids.iter().enumerate() {
                    VALIDITY
                    live += 1;
                    if let Some(expected) = sample_map.get(&id) {
                        if !seen.insert(id) { return Err(format!("duplicate live sample {id}")); }
                        let embedding = self.embeddings.get(&id).ok_or_else(||format!("missing embedding {id}"))?;
                        if embedding.len() != expected.len() || embedding.iter().zip(expected.iter()).any(|(a,b)|a.to_bits()!=b.to_bits()) {return Err(format!("embedding bits mismatch {id}"));}
                        let code = Code::<1>::quantize(&embedding, &leaf.centroid);
                        let actual = super::super::common::code_slice(&leaf.codes, slot, self.code_size());
                        if code.as_ref() != actual {return Err(format!("posting code mismatch {id}"));}
                    }
                }
            }
        }
        if seen.len()!=samples.len() || live!=total {return Err(format!("audit counts: samples {} expected {}, live {} expected {}",seen.len(),samples.len(),live,total));}
        println!("BENCHMARK_AUDIT valid_ids={} physical_postings={} live_postings={} sampled_embeddings_and_codes={} all_sample_bits_match=true", ids.len(),physical,live,seen.len());
        Ok(())
    }
'''
validity='' if variant=='candidate' else 'if !self.current_version_sync(id).is_some_and(|v| v==leaf.versions[slot] && v & super::DELETED_BIT == 0) {continue;}'
method=method.replace('VALIDITY',validity)
after=before.replace('impl HierarchicalSpannWriter {','impl HierarchicalSpannWriter {'+method,1);p.write_text(after)
patch=list(difflib.unified_diff(before.splitlines(True),after.splitlines(True),fromfile=str(p.relative_to(root)),tofile=str(p.relative_to(root))))
p=root/'rust/index/benches/hierarchical_spann_profile_quantized.rs';before=p.read_text()
anchor='    let total_start = Instant::now();'
inject='''    if let Ok(path) = std::env::var("VF_BENCHMARK_AUDIT_SAMPLES") {
        let samples: Vec<(u32, Vec<f32>)> = serde_json::from_slice(&std::fs::read(path)?)?;
        writer.load_all_postings().await?;
        let sample_ids: Vec<u32> = samples.iter().map(|s|s.0).collect();
        writer.load_raw(&sample_ids).await?;
        writer.benchmark_audit_samples(&samples,total_vectors)?;
        return Ok(());
    }
'''
assert before.count(anchor)==1;after=before.replace(anchor,inject+anchor);p.write_text(after)
patch+=list(difflib.unified_diff(before.splitlines(True),after.splitlines(True),fromfile=str(p.relative_to(root)),tofile=str(p.relative_to(root))))
(r/'artifacts'/f'{variant}-audit-only.patch').write_text(''.join(patch))
