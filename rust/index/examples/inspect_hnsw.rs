use chroma_distance::DistanceFunction;
use chroma_index::{HnswIndex, IndexConfig, IndexUuid, PersistentIndex};
use uuid::Uuid;

fn main() {
    let path = "/Users/hammad/Downloads/rebuild";
    let dimensionality = 1536;
    let ef_search = 200;
    let id = Uuid::parse_str("0173ba76-f3f7-4674-8d17-2fd57ded623a").unwrap();

    let index_config = IndexConfig::new(dimensionality, DistanceFunction::Cosine);
    let index_uuid = IndexUuid(id);

    println!("Loading HNSW index from: {}", path);
    println!("  Dimensions: {}", dimensionality);
    println!("  ef_search: {}", ef_search);
    println!("  ID: {}", id);
    println!();

    match HnswIndex::load(path, &index_config, ef_search, index_uuid) {
        Ok(index) => {
            let total_with_deleted = index.len_with_deleted();
            let active = index.len();
            let deleted = total_with_deleted - active;

            println!("=== HNSW Index Stats ===");
            println!("Total elements (including deleted): {}", total_with_deleted);
            println!("Active elements: {}", active);
            println!("Deleted elements: {}", deleted);
            println!("Capacity: {}", index.capacity());
            println!("Dimensionality: {}", index.dimensionality());
            println!();
            println!(
                "Deletion ratio: {:.2}%",
                if total_with_deleted > 0 {
                    (deleted as f64 / total_with_deleted as f64) * 100.0
                } else {
                    0.0
                }
            );
        }
        Err(e) => {
            eprintln!("Failed to load index: {:?}", e);
        }
    }
}

