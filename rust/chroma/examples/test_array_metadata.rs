//! Metadata arrays: search with `$contains` filters.
//!
//! ```sh
//! cargo run --example test_array_metadata -p chroma
//! ```

use chroma::client::ChromaHttpClientOptions;
use chroma::types::{Key, Metadata, MetadataValue, SearchPayload};
use chroma::ChromaHttpClient;

const API_KEY: &str = "ck-91kdyeTN15Wac8rRi7rudSxrgCb7o6CJoJPvjhkkFynh";
const DATABASE: &str = "Demo";

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let client = ChromaHttpClient::new(ChromaHttpClientOptions::cloud(API_KEY, DATABASE)?);

    let coll_name = format!("arr_example_{}", uuid::Uuid::new_v4().as_simple());
    let coll = client.create_collection(&coll_name, None, None).await?;

    // Add three movies with genre tags (string array) and a release year (scalar).
    let movies = vec![
        (
            "m1",
            vec![1.0, 0.0, 0.0],
            metadata(&["action", "comedy"], 2020),
        ),
        ("m2", vec![0.0, 1.0, 0.0], metadata(&["drama"], 2021)),
        (
            "m3",
            vec![0.0, 0.0, 1.0],
            metadata(&["action", "thriller"], 2022),
        ),
    ];
    let (ids, embs, mds) = unzip3(movies);
    coll.add(ids, embs, None, None, Some(mds)).await?;

    // Search: year >= 2021 AND genres $contains "action"  →  only m3 (2022, action/thriller)
    let results = coll
        .search(vec![SearchPayload::default()
            .r#where(
                Key::field("year").gte(2021i64) & Key::field("genres").contains_value("action"),
            )
            .limit(Some(10), 0)
            .select([Key::Metadata])])
        .await?;

    let ids = &results.ids[0];
    let md = results.metadatas[0].as_ref().unwrap()[0].as_ref().unwrap();
    println!("ids:    {ids:?}");
    println!("genres: {:?}", md.get("genres"));
    println!("year:   {:?}", md.get("year"));

    assert_eq!(ids, &["m3"]);

    client.delete_collection(&coll_name).await?;
    println!("\nDone.");
    Ok(())
}

fn metadata(genres: &[&str], year: i64) -> Metadata {
    let mut m = Metadata::new();
    m.insert(
        "genres".into(),
        MetadataValue::StringArray(genres.iter().map(|s| s.to_string()).collect()),
    );
    m.insert("year".into(), MetadataValue::Int(year));
    m
}

fn unzip3(
    v: Vec<(&str, Vec<f32>, Metadata)>,
) -> (Vec<String>, Vec<Vec<f32>>, Vec<Option<Metadata>>) {
    let (mut a, mut b, mut c) = (Vec::new(), Vec::new(), Vec::new());
    for (x, y, z) in v {
        a.push(x.into());
        b.push(y);
        c.push(Some(z));
    }
    (a, b, c)
}
