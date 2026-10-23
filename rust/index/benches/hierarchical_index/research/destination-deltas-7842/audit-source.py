from pathlib import Path
p=Path('/home/ubuntu/chroma-reassignment-destination-deltas/rust/index/benches/hierarchical_spann_profile_quantized.rs')
s=p.read_text();Path('/home/ubuntu/benchmark-data/destination-deltas-7842/audit-source-original.rs').write_text(s)
needle='    let total_start = Instant::now();'
audit='''    if std::env::var_os("AUDIT_SOURCE_ONLY").is_some() {
        let (valid, missing_embeddings, missing_nodes) = writer.reachable_valid_posting_counts();
        println!("Source reachable valid IDs: {valid} | indexed embeddings without one: {missing_embeddings} | missing child nodes: {missing_nodes}");
        return Ok(());
    }

'''
assert s.count(needle)==1
p.write_text(s.replace(needle,audit+needle))
