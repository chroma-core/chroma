//! Bounded private compactor planning; published stores are opened, never committed.
use super::*;
use hierarchical_index::writer::distributed::{append_owned_global, WriteOwnership};
use sha2::{Digest, Sha256};

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Owner {
    subtree_id: String,
    checkpoint_dir: PathBuf,
    generation: u64,
    manifest_sha256: String,
    centroid: Vec<f32>,
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Spec {
    parent_sha256: String,
    source_id: String,
    dimension: usize,
    metric: String,
    old_count: u64,
    canonical_vectors: PathBuf,
    canonical_sha256: String,
    append_vectors: PathBuf,
    append_sha256: String,
    owners: Vec<Owner>,
    max_rows: usize,
}
fn digest(path: &Path) -> Result<String> {
    let mut hash = Sha256::new();
    let mut file = File::open(path)?;
    let mut buffer = [0; 65536];
    loop {
        let n = file.read(&mut buffer)?;
        if n == 0 {
            break;
        }
        hash.update(&buffer[..n]);
    }
    Ok(format!("{:x}", hash.finalize()))
}
pub async fn run(path: &Path) -> Result<()> {
    let spec: Spec = serde_json::from_reader(File::open(path)?)?;
    ensure!(
        spec.parent_sha256.len() == 64 && spec.parent_sha256.bytes().all(|b| b.is_ascii_hexdigit()),
        "pinned parent hash required"
    );
    ensure!(
        spec.dimension > 0
            && spec.dimension <= 1024
            && !spec.owners.is_empty()
            && spec.owners.len() <= 128,
        "bounded write-plan dimensions/owners required"
    );
    ensure!(
        digest(&spec.append_vectors)? == spec.append_sha256,
        "append input hash differs"
    );
    let size = spec.append_vectors.metadata()?.len();
    let row_bytes = (spec.dimension * 4) as u64;
    ensure!(
        size % row_bytes == 0
            && spec.max_rows > 0
            && spec.max_rows <= 100_000
            && spec.old_count <= spec.max_rows as u64
            && size / row_bytes <= spec.max_rows as u64 - spec.old_count,
        "bounded complete append rows required"
    );
    ensure!(
        digest(&spec.canonical_vectors)? == spec.canonical_sha256,
        "canonical input hash differs"
    );
    let mut canonical = File::open(&spec.canonical_vectors)?;
    ensure!(
        canonical.metadata()?.len() == (spec.old_count + size / row_bytes) * row_bytes,
        "full canonical coverage differs"
    );
    let mut writers = Vec::new();
    let mut manifests = Vec::new();
    let mut maps = Vec::new();
    let mut boundaries = Vec::new();
    let mut previous = None;
    for owner in &spec.owners {
        ensure!(
            previous
                .as_ref()
                .is_none_or(|last: &String| last < &owner.subtree_id),
            "owners must have unique stable logical order"
        );
        previous = Some(owner.subtree_id.clone());
        let manifest = owner
            .checkpoint_dir
            .join(format!("manifest-{}.json", owner.generation));
        ensure!(
            digest(&manifest)? == owner.manifest_sha256
                && owner.checkpoint_dir.join("data").is_dir(),
            "parent manifest/store identity differs"
        );
        let m: Manifest = serde_json::from_reader(File::open(&manifest)?)?;
        let marker: serde_json::Value = serde_json::from_reader(File::open(
            owner.checkpoint_dir.join("global-write-private.json"),
        )?)?;
        ensure!(
            marker["parent_sha256"] == spec.parent_sha256
                && marker["subtree_id"] == owner.subtree_id
                && marker["manifest_sha256"] == owner.manifest_sha256,
            "private clone admission differs"
        );
        ensure!(
            m.shard_id == owner.subtree_id
                && m.generation == owner.generation
                && m.source_id == spec.source_id
                && m.dimension == spec.dimension
                && m.metric == spec.metric,
            "owner semantic identity differs"
        );
        let mapping = m
            .id_mapping
            .as_ref()
            .context("explicit parent ID map required")?;
        let mut lookup = id_map::IdLookup::open(&owner.checkpoint_dir, mapping)?;
        let mut ids = Vec::new();
        for id in 0..m.vector_count {
            ids.push(lookup.get(u32::try_from(id)?)?);
        }
        let provider = provider(&owner.checkpoint_dir).await?;
        let writer = HierarchicalSpannWriter::open(
            &provider,
            m.block_ids.ids()?,
            metric(&spec.metric)?,
            read_config(&m.construction)?,
        )
        .await
        .map_err(|e| anyhow::anyhow!("write-plan open: {e}"))?;
        manifests.push(m);
        writers.push(writer);
        maps.push(ids);
        boundaries.push(owner.centroid.clone());
    }
    let mut ownership = WriteOwnership::new(maps, spec.max_rows).map_err(anyhow::Error::msg)?;
    ensure!(
        ownership.maps().iter().map(Vec::len).sum::<usize>() as u64 == spec.old_count,
        "parent coverage differs from expected prefix"
    );
    let mut sessions = writers
        .iter_mut()
        .map(|w| {
            w.write_session(spec.parent_sha256.clone())
                .map_err(anyhow::Error::msg)
        })
        .collect::<Result<Vec<_>>>()?;
    let mut input = File::open(&spec.append_vectors)?;
    let mut routes = Vec::new();
    for row in 0..size / row_bytes {
        let mut bytes = vec![0u8; row_bytes as usize];
        input.read_exact(&mut bytes)?;
        let mut vector: Vec<_> = bytes
            .chunks_exact(4)
            .map(|b| f32::from_le_bytes(b.try_into().unwrap()))
            .collect();
        if spec.metric == "cosine" {
            normalize(&mut vector)?;
        }
        let id = spec.old_count + row;
        let route = append_owned_global(
            &mut sessions,
            &spec.parent_sha256,
            &mut ownership,
            id,
            &vector,
            Some(&boundaries),
        )
        .map_err(anyhow::Error::msg)?;
        routes.push(serde_json::json!({"id":id,"subtree_id":spec.owners[route.owner].subtree_id,"leaf_id":route.leaf,"local_id":ownership.maps()[route.owner].len()-1,"distance":route.distance}));
    }
    ensure!(
        digest(&spec.append_vectors)? == spec.append_sha256,
        "append input changed during planning"
    );
    for owner in &spec.owners {
        ensure!(
            digest(
                &owner
                    .checkpoint_dir
                    .join(format!("manifest-{}.json", owner.generation))
            )? == owner.manifest_sha256,
            "parent manifest changed during planning"
        );
    }
    drop(sessions);
    let mut saved = Vec::new();
    for (index, owner) in spec.owners.iter().enumerate() {
        let m = &mut manifests[index];
        let ids = &ownership.maps()[index];
        if ids.len() as u64 == m.vector_count {
            continue;
        }
        let previous_generation = m.generation;
        let generation = previous_generation
            .checked_add(1)
            .context("generation overflow")?;
        ensure!(
            !owner
                .checkpoint_dir
                .join(format!("manifest-{generation}.json"))
                .exists(),
            "orphan target generation cannot be overwritten"
        );
        for name in [
            "construction-input.f32",
            "construction-input.u64le",
            "build-provenance.json",
            "compactor-execution.json",
            "write-plan-spec.json",
        ] {
            let source = owner.checkpoint_dir.join(name);
            if source.exists() {
                let retained = owner
                    .checkpoint_dir
                    .join(format!("retained-{previous_generation}-{name}"));
                ensure!(!retained.exists(), "retained execution input collision");
                fs::rename(source, retained)?;
            }
        }
        let input_vectors = owner.checkpoint_dir.join("construction-input.f32");
        let input_map = owner.checkpoint_dir.join("construction-input.u64le");
        let mut vectors = OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(&input_vectors)?;
        let mut mapping = OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(&input_map)?;
        use std::io::{Seek, SeekFrom};
        for &id in ids {
            canonical.seek(SeekFrom::Start(id * row_bytes))?;
            let mut bytes = vec![0; row_bytes as usize];
            canonical.read_exact(&mut bytes)?;
            vectors.write_all(&bytes)?;
            mapping.write_all(&id.to_le_bytes())?;
        }
        vectors.sync_all()?;
        mapping.sync_all()?;
        drop(vectors);
        drop(mapping);
        let prepared = id_map::prepare(
            Some(&input_map),
            m.id_mapping.as_ref(),
            m.vector_count,
            ids.len() as u64,
            &owner.checkpoint_dir,
            generation,
        )?
        .context("explicit saved map required")?;
        let provider = provider(&owner.checkpoint_dir).await?;
        let previous = m.block_ids.ids()?;
        let flushed = writers[index]
            .commit(&provider, Some(&previous))
            .await
            .map_err(|e| anyhow::anyhow!("selected writer commit: {e}"))?
            .flush()
            .await
            .map_err(|e| anyhow::anyhow!("selected writer flush: {e}"))?;
        let reader = HierarchicalSpannReader::open(
            &provider,
            flushed.clone(),
            metric(&spec.metric)?,
            read_config(&m.construction)?,
        )
        .await
        .map_err(|e| anyhow::anyhow!("selected writer reopen: {e}"))?;
        let coverage = reader
            .audit_reachable_ids(
                ids.len() as u64,
                hierarchical_index::reader::AuditLimits {
                    max_bitmap_bytes: 1_000_000,
                    max_nodes: 1_000_000,
                    max_leaf_entries: 100_000,
                },
            )
            .await
            .map_err(|e| anyhow::anyhow!("saved ownership audit: {e}"))?;
        let mut raw = File::open(&input_vectors)?;
        for local in 0..ids.len() {
            let mut bytes = vec![0; row_bytes as usize];
            raw.read_exact(&mut bytes)?;
            let mut expected: Vec<_> = bytes
                .chunks_exact(4)
                .map(|b| f32::from_le_bytes(b.try_into().unwrap()))
                .collect();
            if spec.metric == "cosine" {
                normalize(&mut expected)?;
            }
            ensure!(
                reader
                    .verify_vector_bits(local as u32, &expected)
                    .await
                    .map_err(|e| anyhow::anyhow!("saved content proof: {e}"))?,
                "persisted vector differs from canonical input bits"
            );
        }
        let selected: Vec<_> = routes
            .iter()
            .filter(|route| route["subtree_id"] == owner.subtree_id)
            .cloned()
            .collect();
        for route in &selected {
            ensure!(
                reader
                    .verify_live_leaf_member(
                        route["leaf_id"].as_u64().context("missing selected leaf")? as u32,
                        route["local_id"]
                            .as_u64()
                            .context("missing selected local ID")? as u32
                    )
                    .await
                    .map_err(|e| anyhow::anyhow!("saved leaf proof: {e}"))?,
                "selected registration moved to a different leaf"
            );
        }
        let root = reader
            .describe_root()
            .map_err(|e| anyhow::anyhow!("saved root: {e}"))?;
        let map = prepared.publish()?;
        m.generation = generation;
        m.vector_count = ids.len() as u64;
        m.block_ids = BlockIds::from_ids(&flushed);
        m.global_id_end = m
            .id_offset
            .checked_add(
                m.vector_count
                    .checked_mul(m.id_stride)
                    .context("ID range overflow")?,
            )
            .context("ID range overflow")?;
        m.id_mapping = Some(map.clone());
        m.root_ref = Some(serde_json::json!({"kind":root.kind,"id":root.id}));
        m.integrity_audit = Some(
            serde_json::json!({"generation":generation,"block_ids":BlockIds::from_ids(&flushed),"id_map_sha256":map.sha256,"coverage":coverage}),
        );
        publish(&owner.checkpoint_dir, m)?;
        let receipt = serde_json::json!({"subtree_id":owner.subtree_id,"parent_sha256":spec.parent_sha256,"parent_manifest_sha256":owner.manifest_sha256,"generation":generation,"snapshot_sha256":digest(&owner.checkpoint_dir.join(format!("manifest-{generation}.json")))?,"exact_vector_rows_verified":ids.len(),"selected_leaf_memberships":selected,"all_selected_leaf_memberships_verified":true});
        let proof_path = owner.checkpoint_dir.join("compactor-execution.json");
        let mut proof = OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(&proof_path)?;
        serde_json::to_writer(&mut proof, &receipt)?;
        proof.sync_all()?;
        saved.push(receipt);
    }
    ensure!(
        digest(&spec.canonical_vectors)? == spec.canonical_sha256,
        "canonical input changed during commit"
    );
    let owners: Vec<_> = spec
        .owners
        .iter()
        .zip(ownership.maps())
        .map(|(owner, ids)| serde_json::json!({"subtree_id":owner.subtree_id,"ids":ids}))
        .collect();
    println!(
        "{}",
        serde_json::json!({"status":"saved-private-global-writes","parent_sha256":spec.parent_sha256,"append_sha256":spec.append_sha256,"owners":owners,"routes":routes,"saved":saved,"durable":true})
    );
    Ok(())
}
