use std::path::PathBuf;

use anyhow::Result;
use tokio::io::{AsyncReadExt, BufReader};

use super::{
    types::{Record, RecordDataset},
    util::get_or_populate_cached_dataset_file,
};

pub struct GistDataset {
    base_file_path: PathBuf,
    dimension: usize,
    num_records: usize,
}

const FLOAT32_SIZE: usize = 4;

impl RecordDataset for GistDataset {
    const DISPLAY_NAME: &'static str = "Gist";
    const NAME: &'static str = "gist";

    async fn init() -> Result<Self> {
        // TODO(Sanket): Download file if it doesn't exist.
        // move file from downloads to cached path.
        let current_path = "/Users/sanketkedia/Downloads/siftsmall/siftsmall_base.fvecs";
        let base_file_path = get_or_populate_cached_dataset_file(
            "gist",
            "siftsmall_base.fvecs",
            None,
            |mut writer| async move {
                let mut file = tokio::fs::File::open(current_path).await?;
                tokio::io::copy(&mut file, &mut writer).await?;
                Ok(())
            },
        )
        .await?;

        Ok(Self {
            base_file_path,
            dimension: 128,
            num_records: 10000,
        })
    }

    async fn create_records_stream(
        &self,
    ) -> anyhow::Result<impl futures::Stream<Item = anyhow::Result<super::types::Record>>> {
        let chunk_size = FLOAT32_SIZE + self.dimension * FLOAT32_SIZE;
        let total_bytes = self.num_records * chunk_size;
        let file = tokio::fs::File::open(self.base_file_path.clone()).await?;
        let mut buf_read = BufReader::new(file);
        let mut buf = vec![0u8; total_bytes];
        // TODO(Sanket): For now reading everything in one go. Need to read in chunks.
        let _ = buf_read.read_exact(&mut buf).await?;

        let mut vec_records = Vec::new();
        for bin_of_vec in buf.chunks(chunk_size) {
            let (_, vec_values) = bin_of_vec.split_at(FLOAT32_SIZE);
            let emb = vec_values
                .chunks(FLOAT32_SIZE)
                .map(|byte_slice| byte_slice.try_into().expect("Slice with wrong length!"))
                .map(f32::from_le_bytes)
                .collect::<Vec<f32>>();
            vec_records.push(Record {
                document: "".to_string(),
                metadata: Default::default(),
                embedding: Some(emb),
            });
        }
        println!("Read {:?} records", vec_records.len());

        Ok(futures::stream::iter(vec_records.into_iter().map(Ok)))
    }
}
