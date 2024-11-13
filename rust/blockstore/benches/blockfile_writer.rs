use chroma_blockstore::{
    arrow::{
        provider::ArrowBlockfileProvider,
        types::{ArrowWriteableKey, ArrowWriteableValue},
    },
    BlockfileWriterOptions,
};
use chroma_cache::UnboundedCacheConfig;
use chroma_storage::{local::LocalStorage, Storage};
use criterion::{criterion_group, criterion_main, Criterion};
use rand::{thread_rng, Rng};
use tokio::runtime::Runtime;

trait DataGenerator {
    type Key: ArrowWriteableKey + Ord;
    type Value: ArrowWriteableValue;
    type DataSize: Clone;

    fn generate(size: Self::DataSize) -> Self;
    fn num_bytes(&self) -> usize;
    fn data(self) -> Vec<(String, Self::Key, Self::Value)>;
}

/// Simulates writing trigram frequencies for the full text search index.
struct TrigramFrequencyGenerator {
    data: Vec<(String, u32)>,
}

#[derive(Clone)]
struct TrigramFrequencyDataSize(usize);

impl DataGenerator for TrigramFrequencyGenerator {
    type Key = u32;
    type Value = u32;
    type DataSize = TrigramFrequencyDataSize;

    fn generate(num: Self::DataSize) -> Self {
        let mut data = Vec::with_capacity(num.0);
        for _ in 0..num.0 {
            let key = (0..3)
                .map(|_| {
                    let ascii_code = thread_rng().gen_range(32..127) as u8;
                    ascii_code as char
                })
                .collect::<String>();
            let value = thread_rng().gen::<u32>();
            data.push((key, value));
        }

        Self { data }
    }

    fn num_bytes(&self) -> usize {
        self.data.len() * (3 + 4 + 4)
    }

    fn data(self) -> Vec<(String, Self::Key, Self::Value)> {
        self.data
            .into_iter()
            .map(|(key, value)| (key, 0, value))
            .collect()
    }
}

/// Simulates writing trigram posting lists for the full text search index.
struct TrigramPostingListGenerator {
    data: Vec<(String, u32, Vec<u32>)>,
}

#[derive(Clone)]
struct TrigramPostingListDataSize {
    num_documents: usize,
    num_trigrams_per_document: usize,
    num_unique_trigrams: usize,
    num_postings_per_trigram_document: usize, // todo: range
}

impl DataGenerator for TrigramPostingListGenerator {
    type Key = u32;
    type Value = Vec<u32>;
    type DataSize = TrigramPostingListDataSize;

    fn generate(size: Self::DataSize) -> Self {
        let mut trigrams = Vec::with_capacity(size.num_unique_trigrams);
        for _ in 0..size.num_unique_trigrams {
            let trigram = (0..3)
                .map(|_| {
                    let ascii_code = thread_rng().gen_range(32..127) as u8;
                    ascii_code as char
                })
                .collect::<String>();
            trigrams.push(trigram);
        }

        let mut data = Vec::with_capacity(size.num_documents * size.num_trigrams_per_document);
        for _ in 0..size.num_documents {
            let document_id = thread_rng().gen::<u32>();
            for _ in 0..size.num_trigrams_per_document {
                let trigram = trigrams[thread_rng().gen_range(0..size.num_unique_trigrams)].clone();
                let posting_list = (0..size.num_postings_per_trigram_document)
                    .map(|_| thread_rng().gen::<u32>())
                    .collect();
                data.push((trigram, document_id as u32, posting_list));
            }
        }

        Self { data }
    }

    fn num_bytes(&self) -> usize {
        self.data.iter().fold(0, |acc, (prefix, _, postings)| {
            acc + (prefix.len() + 4 + postings.len() * 4) // prefix + key + postings
        })
    }

    fn data(self) -> Vec<(String, Self::Key, Self::Value)> {
        self.data
    }
}

fn bench_writer_for_generator_and_size<D: DataGenerator>(
    c: &mut Criterion,
    runner: &Runtime,
    name: &str,
    size: D::DataSize,
    provider: &ArrowBlockfileProvider,
) where
    <D as DataGenerator>::Value: chroma_blockstore::memory::Writeable,
{
    let generator = D::generate(size.clone());
    let data_byte_size = generator.num_bytes();
    println!(
        "Benchmarking {} with {} bytes of data",
        name, data_byte_size
    );
    let data = generator.data();

    let name_writer_options_data = [
        (
            "UnorderedBlockfileWriter",
            BlockfileWriterOptions::new().unordered_mutations(),
            data.clone(),
        ),
        (
            "OrderedBlockfileWriter",
            BlockfileWriterOptions::new().ordered_mutations(),
            {
                let mut data = data;
                data.sort_unstable_by(|a, b| a.0.cmp(&b.0).then_with(|| a.1.cmp(&b.1)));
                data
            },
        ),
    ];

    {
        let mut fresh_writer = c.benchmark_group(format!("fresh_writer_{}", name));
        fresh_writer.throughput(criterion::Throughput::Bytes(data_byte_size as u64));

        for (name, writer_options, data) in name_writer_options_data.iter() {
            fresh_writer.bench_function(*name, |b| {
                b.to_async(runner).iter_batched(
                    || data.clone(),
                    |data| async {
                        let writer = provider
                            .write::<D::Key, D::Value>(*writer_options)
                            .await
                            .unwrap();
                        for (prefix, key, value) in data {
                            writer.set(&prefix, key, value).await.unwrap();
                        }

                        writer.commit::<D::Key, D::Value>().await.unwrap();
                    },
                    criterion::BatchSize::LargeInput,
                );
            });
        }
    }

    {
        let populated_blockfile_id = runner.block_on(async {
            let writer = provider
                .write::<D::Key, D::Value>(BlockfileWriterOptions::new().unordered_mutations())
                .await
                .unwrap();

            let generator = D::generate(size.clone());
            for (prefix, key, value) in generator.data() {
                writer.set(&prefix, key, value).await.unwrap();
            }

            let writer_id = writer.id();
            let flusher = writer.commit::<D::Key, D::Value>().await.unwrap();
            flusher.flush::<D::Key, D::Value>().await.unwrap();
            writer_id
        });

        let mut forked_writer = c.benchmark_group(format!("forked_writer_{}", name));
        forked_writer.throughput(criterion::Throughput::Bytes(data_byte_size as u64));

        for (name, writer_options, data) in name_writer_options_data.iter() {
            forked_writer.bench_function(*name, |b| {
                b.to_async(runner).iter_batched(
                    || data.clone(),
                    |data| async {
                        let writer = provider
                            .write::<D::Key, D::Value>(writer_options.fork(populated_blockfile_id))
                            .await
                            .unwrap();

                        for (prefix, key, value) in data {
                            writer.set(&prefix, key, value).await.unwrap();
                        }

                        let writer_id = writer.id();
                        let flusher = writer.commit::<D::Key, D::Value>().await.unwrap();
                        flusher.flush::<D::Key, D::Value>().await.unwrap();

                        let reader = provider.read::<u32, &[u32]>(&writer_id).await.unwrap();
                        match reader {
                            chroma_blockstore::BlockfileReader::ArrowBlockfileReader(reader) => {
                                assert!(reader.is_valid().await)
                            }
                            _ => panic!("Expected ArrowBlockfileReader"),
                        }
                    },
                    criterion::BatchSize::LargeInput,
                );
            });
        }
    }
}

// todo: maybe this should be a parameter
const BLOCK_SIZE: usize = 1024 * 1024 * 8; // 8MB

/// This benchmark compares the performance of UnorderedBlockfileWriter and OrderedBlockfileWriter across various use-cases.
pub fn benchmark(c: &mut Criterion) {
    let runner = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .expect("Failed to create runtime");

    let tmp_dir = tempfile::tempdir().unwrap();
    let storage = Storage::Local(LocalStorage::new(tmp_dir.path().to_str().unwrap()));
    let block_cache = Box::new(UnboundedCacheConfig {}.build()) as _;
    let sparse_index_cache = Box::new(UnboundedCacheConfig {}.build()) as _;
    let arrow_blockfile_provider =
        ArrowBlockfileProvider::new(storage.clone(), BLOCK_SIZE, block_cache, sparse_index_cache);

    // todo: sizes should be configurable
    bench_writer_for_generator_and_size::<TrigramFrequencyGenerator>(
        c,
        &runner,
        "trigram_frequencies",
        TrigramFrequencyDataSize(100_000),
        &arrow_blockfile_provider,
    );

    bench_writer_for_generator_and_size::<TrigramPostingListGenerator>(
        c,
        &runner,
        "trigram_posting_lists",
        TrigramPostingListDataSize {
            num_documents: 1_000,
            num_trigrams_per_document: 3_000,
            num_unique_trigrams: 50_000,
            num_postings_per_trigram_document: 5,
        },
        &arrow_blockfile_provider,
    );
}

criterion_group!(benches, benchmark);
criterion_main!(benches);
