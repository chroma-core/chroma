from pathlib import Path
root=Path('/home/ubuntu/chroma-large-index-balancing')
p=root/'rust/index/benches/hierarchical_spann_profile_quantized.rs'
s=p.read_text(); marker='    let args = Args::parse_from(std::env::args().filter(|a| a != "--bench"));'
s=s.replace(marker,marker+'''
    if let Ok(dir) = std::env::var("CHROMA_BALANCE_COMPONENT_FIXTURE") {
        let dir = PathBuf::from(dir);
        let metadata = load_checkpoint_meta(&dir)?;
        let cache_bytes: u64 = std::env::var("CHROMA_COMPONENT_CACHE_BYTES")?.parse()?;
        let count: usize = std::env::var("CHROMA_COMPONENT_LEAF_COUNT")?.parse()?;
        let provider = make_blockfile_provider(&dir.join("data"), cache_bytes).await;
        let writer = HierarchicalSpannWriter::open(&provider, metadata.to_ids(), DistanceFunction::Euclidean, HierarchicalSpannConfig::default()).await.map_err(|e| e.to_string())?;
        writer.profile_component_loads(count).await.map_err(|e| e.to_string())?;
        return Ok(());
    }
''')
p.write_text(s)
p=root/'rust/index/benches/hierarchical_index/writer/persistence.rs'; s=p.read_text()
needle='impl HierarchicalSpannWriter {'
s=s.replace(needle,needle+'''
    /// This investigation-only replay reads independent fixture copies and performs no persistence.
    pub async fn profile_component_loads(&self, count: usize) -> Result<(), Box<dyn ChromaError>> {
        let mut ids: Vec<_> = self.nodes.iter().filter(|entry| matches!(entry.value(), TreeNode::Leaf(_))).map(|entry| *entry.key()).collect();
        // Distribute leaves through the tree in a fixed order, independent of hash-map iteration.
        ids.sort_by_key(|id| ((*id as u64).wrapping_mul(11400714819323198485u64)).rotate_left(17));
        let started = std::time::Instant::now();
        for (i, id) in ids.into_iter().take(count).enumerate() {
            let start = std::time::Instant::now();
            self.load(id).await?;
            println!("[component-probe] leaf={} id={} total_ms={:.3} elapsed_s={:.3}", i, id, start.elapsed().as_secs_f64()*1000.0, started.elapsed().as_secs_f64());
        }
        println!("[component-probe] finished elapsed_s={:.6}", started.elapsed().as_secs_f64());
        Ok(())
    }
''',1)
s=s.replace('        self.prefetch_versions(&loaded_ids).await?;','''        let probe_started = std::time::Instant::now();
        self.prefetch_versions(&loaded_ids).await?;
        if std::env::var_os("CHROMA_BALANCE_COMPONENT_FIXTURE").is_some() {
            println!("[component-probe] prefetch ids={} ms={:.3}", loaded_ids.len(), probe_started.elapsed().as_secs_f64()*1000.0);
        }''',1)
p.write_text(s)
