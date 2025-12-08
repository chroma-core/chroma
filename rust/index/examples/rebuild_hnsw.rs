use chroma_distance::DistanceFunction;
use chroma_index::{HnswIndex, HnswIndexConfig, Index, IndexConfig, IndexUuid, PersistentIndex};
use std::path::Path;
use uuid::Uuid;

fn main() {
    let old_path = "/Users/hammad/Downloads/rebuild";
    let new_path = "/Users/hammad/Downloads/rebuild_clean";
    let dimensionality = 1536;
    let ef_search = 200;
    let ef_construction = 200; // Typical good value
    let m = 16; // Typical good value for HNSW
    let id = Uuid::parse_str("0173ba76-f3f7-4674-8d17-2fd57ded623a").unwrap();

    let index_config = IndexConfig::new(dimensionality, DistanceFunction::Euclidean);
    let index_uuid = IndexUuid(id);

    // === STEP 1: Load old index ===
    println!("=== STEP 1: Loading old index ===");
    println!("Path: {}", old_path);

    let old_index = match HnswIndex::load(old_path, &index_config, ef_search, index_uuid) {
        Ok(idx) => idx,
        Err(e) => {
            eprintln!("Failed to load old index: {:?}", e);
            return;
        }
    };

    let old_total = old_index.len_with_deleted();
    let old_active = old_index.len();
    let old_deleted = old_total - old_active;

    println!("Old index stats:");
    println!("  Total (with deleted): {}", old_total);
    println!("  Active: {}", old_active);
    println!("  Deleted: {}", old_deleted);
    println!(
        "  Deletion ratio: {:.2}%",
        (old_deleted as f64 / old_total as f64) * 100.0
    );
    println!();

    // === STEP 2: Extract all active IDs and vectors ===
    println!("=== STEP 2: Extracting active vectors ===");

    let (active_ids, deleted_ids) = match old_index.get_all_ids() {
        Ok(ids) => ids,
        Err(e) => {
            eprintln!("Failed to get all IDs: {:?}", e);
            return;
        }
    };

    println!("Active IDs count: {}", active_ids.len());
    println!("Deleted IDs count: {}", deleted_ids.len());

    // Collect all vectors
    let mut vectors: Vec<(usize, Vec<f32>)> = Vec::with_capacity(active_ids.len());
    let mut failed_gets = 0;

    for (i, &id) in active_ids.iter().enumerate() {
        if i % 50000 == 0 {
            println!("  Extracting vector {}/{}", i, active_ids.len());
        }

        match old_index.get(id) {
            Ok(Some(vec)) => {
                vectors.push((id, vec));
            }
            Ok(None) => {
                eprintln!("  Warning: ID {} returned None", id);
                failed_gets += 1;
            }
            Err(e) => {
                eprintln!("  Warning: Failed to get ID {}: {:?}", id, e);
                failed_gets += 1;
            }
        }
    }

    println!(
        "Extracted {} vectors ({} failed)",
        vectors.len(),
        failed_gets
    );
    println!();

    // === STEP 3: Create new index ===
    println!("=== STEP 3: Creating new clean index ===");
    println!("Path: {}", new_path);

    // Create output directory
    if let Err(e) = std::fs::create_dir_all(new_path) {
        eprintln!("Failed to create output directory: {}", e);
        return;
    }

    let hnsw_config =
        match HnswIndexConfig::new_persistent(m, ef_construction, ef_search, Path::new(new_path)) {
            Ok(cfg) => cfg,
            Err(e) => {
                eprintln!("Failed to create HNSW config: {:?}", e);
                return;
            }
        };

    let mut new_index = match HnswIndex::init(&index_config, Some(&hnsw_config), index_uuid) {
        Ok(idx) => idx,
        Err(e) => {
            eprintln!("Failed to init new index: {:?}", e);
            return;
        }
    };

    // Resize to fit all vectors
    println!("Resizing new index to capacity: {}", vectors.len());
    if let Err(e) = new_index.resize(vectors.len()) {
        eprintln!("Failed to resize: {:?}", e);
        return;
    }

    // === STEP 4: Add all vectors ===
    println!("=== STEP 4: Adding vectors to new index ===");

    let mut add_failures = 0;
    for (i, (id, vec)) in vectors.iter().enumerate() {
        if i % 50000 == 0 {
            println!("  Adding vector {}/{}", i, vectors.len());
        }

        if let Err(e) = new_index.add(*id, vec) {
            eprintln!("  Warning: Failed to add ID {}: {:?}", id, e);
            add_failures += 1;
        }
    }

    println!(
        "Added {} vectors ({} failures)",
        vectors.len() - add_failures,
        add_failures
    );
    println!();

    // === STEP 5: Save new index ===
    println!("=== STEP 5: Saving new index ===");

    if let Err(e) = new_index.save() {
        eprintln!("Failed to save new index: {:?}", e);
        return;
    }

    println!("New index saved successfully!");
    println!();

    // === STEP 6: Scrub/Verify ===
    println!("=== STEP 6: Verification (Scrubbing) ===");

    let new_total = new_index.len_with_deleted();
    let new_active = new_index.len();
    let new_deleted = new_total - new_active;

    println!("New index stats:");
    println!("  Total (with deleted): {}", new_total);
    println!("  Active: {}", new_active);
    println!("  Deleted: {}", new_deleted);
    println!();

    // Verify counts match
    let expected_count = vectors.len();
    if new_active == expected_count {
        println!(
            "✓ Count verification PASSED: {} == {}",
            new_active, expected_count
        );
    } else {
        println!(
            "✗ Count verification FAILED: {} != {}",
            new_active, expected_count
        );
    }

    // Sample verification - check some random vectors
    println!();
    println!("Verifying sample vectors...");
    let sample_size = std::cmp::min(1000, vectors.len());
    let step = vectors.len() / sample_size;
    let mut sample_mismatches = 0;

    for i in (0..vectors.len()).step_by(step) {
        let (id, original_vec) = &vectors[i];

        match new_index.get(*id) {
            Ok(Some(new_vec)) => {
                // Compare vectors
                if original_vec.len() != new_vec.len() {
                    println!(
                        "  ✗ ID {}: dimension mismatch {} vs {}",
                        id,
                        original_vec.len(),
                        new_vec.len()
                    );
                    sample_mismatches += 1;
                } else {
                    let max_diff: f32 = original_vec
                        .iter()
                        .zip(new_vec.iter())
                        .map(|(a, b)| (a - b).abs())
                        .fold(0.0f32, f32::max);

                    if max_diff > 1e-6 {
                        println!("  ✗ ID {}: max diff = {}", id, max_diff);
                        sample_mismatches += 1;
                    }
                }
            }
            Ok(None) => {
                println!("  ✗ ID {}: not found in new index", id);
                sample_mismatches += 1;
            }
            Err(e) => {
                println!("  ✗ ID {}: error getting from new index: {:?}", id, e);
                sample_mismatches += 1;
            }
        }
    }

    let samples_checked = (vectors.len() + step - 1) / step;
    if sample_mismatches == 0 {
        println!(
            "✓ Sample verification PASSED: {} vectors checked",
            samples_checked
        );
    } else {
        println!(
            "✗ Sample verification FAILED: {}/{} mismatches",
            sample_mismatches, samples_checked
        );
    }

    // === Summary ===
    println!();
    println!("========== SUMMARY ==========");
    println!(
        "Old index: {} active, {} deleted ({:.2}% deleted)",
        old_active,
        old_deleted,
        (old_deleted as f64 / old_total as f64) * 100.0
    );
    println!(
        "New index: {} active, {} deleted ({:.2}% deleted)",
        new_active,
        new_deleted,
        if new_total > 0 {
            (new_deleted as f64 / new_total as f64) * 100.0
        } else {
            0.0
        }
    );
    println!(
        "Space savings: {:.2}x reduction in index size",
        old_total as f64 / new_total as f64
    );
    println!("New index location: {}", new_path);
    println!("==============================");
}
