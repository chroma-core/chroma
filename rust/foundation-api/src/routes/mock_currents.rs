use chroma_types::{Metadata, MetadataValue};
use serde::Deserialize;

pub(crate) struct MockCurrentRecord {
    pub(crate) id: String,
    pub(crate) document: String,
    pub(crate) metadata: Metadata,
}

#[derive(Debug, Deserialize)]
struct MockCurrentFixture {
    tilegroup_id: String,
    label: String,
    headline: String,
    summary: String,
    tiles: Vec<MockTileFixture>,
}

#[derive(Debug, Deserialize)]
struct MockTileFixture {
    slug: String,
    title: String,
    role: String,
    blurb: String,
}

pub(crate) fn mock_currents_records() -> Vec<MockCurrentRecord> {
    let fixture = include_str!("mock_currents.json");
    let tilegroups: Vec<MockCurrentFixture> =
        serde_json::from_str(fixture).expect("mock_currents.json must be valid");
    tilegroups.into_iter().map(mock_current_record).collect()
}

fn mock_current_record(fixture: MockCurrentFixture) -> MockCurrentRecord {
    let MockCurrentFixture {
        tilegroup_id,
        label,
        headline,
        summary,
        tiles,
    } = fixture;
    let page_slugs: Vec<String> = tiles.iter().map(|tile| tile.slug.clone()).collect();
    let tile_roles: Vec<String> = tiles.iter().map(|tile| tile.role.clone()).collect();
    let mut metadata = Metadata::new();
    metadata.insert(
        "tilegroup_id".to_string(),
        MetadataValue::Str(tilegroup_id.clone()),
    );
    metadata.insert("label".to_string(), MetadataValue::Str(label.clone()));
    metadata.insert("headline".to_string(), MetadataValue::Str(headline.clone()));
    metadata.insert("summary".to_string(), MetadataValue::Str(summary.clone()));
    metadata.insert(
        "tile_count".to_string(),
        MetadataValue::Int(tiles.len() as i64),
    );
    metadata.insert(
        "page_slugs".to_string(),
        MetadataValue::StringArray(page_slugs),
    );
    metadata.insert(
        "tile_roles".to_string(),
        MetadataValue::StringArray(tile_roles),
    );
    for (idx, tile) in tiles.iter().enumerate() {
        let key = format!("tile_{:02}_json", idx + 1);
        let value = serde_json::json!({
            "order": idx + 1,
            "slug": tile.slug,
            "title": tile.title,
            "role": tile.role,
            "blurb": tile.blurb,
        });
        metadata.insert(key, MetadataValue::Str(value.to_string()));
    }

    let tiles_document = tiles
        .iter()
        .enumerate()
        .map(|(idx, tile)| {
            format!(
                "Tile {}\nSlug: {}\nTitle: {}\nRole: {}\nBlurb: {}",
                idx + 1,
                tile.slug,
                tile.title,
                tile.role,
                tile.blurb
            )
        })
        .collect::<Vec<_>>()
        .join("\n\n");
    let document = format!("{}: {}. {}\n\n{}", label, headline, summary, tiles_document);

    MockCurrentRecord {
        id: format!("tilegroup:{tilegroup_id}"),
        document,
        metadata,
    }
}
