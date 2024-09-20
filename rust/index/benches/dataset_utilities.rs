use std::time::Duration;

use chroma_benchmark_datasets::types::{FrozenQuerySubset, QueryDataset, RecordDataset};
use futures::TryFutureExt;
use indicatif::{MultiProgress, ProgressBar, ProgressStyle};

pub async fn get_record_dataset<RecordCorpus: RecordDataset>() -> RecordCorpus {
    let style = ProgressStyle::default_spinner()
        .template("{spinner:.green} {msg}")
        .unwrap();

    let finish_style = ProgressStyle::default_spinner()
        .template("{prefix:.green} {msg}")
        .unwrap();

    let record_corpus_spinner = ProgressBar::new_spinner()
        .with_message(RecordCorpus::DISPLAY_NAME)
        .with_style(style.clone());
    record_corpus_spinner.enable_steady_tick(Duration::from_millis(50));

    let record_corpus = RecordCorpus::init()
        .and_then(|r| async {
            record_corpus_spinner.set_style(finish_style.clone());
            record_corpus_spinner.set_prefix("✔︎");
            record_corpus_spinner.finish_and_clear();
            Ok(r)
        })
        .await
        .expect("Failed to initialize record corpus");

    record_corpus
}

pub async fn get_record_query_dataset_pair<
    RecordCorpus: RecordDataset,
    QueryCorpus: QueryDataset,
>(
    min_results_per_query: usize,
    max_num_of_queries: usize,
) -> (RecordCorpus, FrozenQuerySubset) {
    let progress = MultiProgress::new();

    let style = ProgressStyle::default_spinner()
        .template("  {spinner:.green} {msg}")
        .unwrap();

    let finish_style = ProgressStyle::default_spinner()
        .template("  {prefix:.green} {msg}")
        .unwrap();

    let parent_task_style = ProgressStyle::default_spinner()
        .template("📁 initializing datasets...")
        .unwrap();
    let parent_task = ProgressBar::new_spinner().with_style(parent_task_style);

    let record_corpus_spinner = ProgressBar::new_spinner()
        .with_message(RecordCorpus::DISPLAY_NAME)
        .with_style(style.clone());
    let query_corpus_spinner = ProgressBar::new_spinner()
        .with_message(QueryCorpus::DISPLAY_NAME)
        .with_style(style.clone());

    let parent_task = progress.add(parent_task);
    let record_corpus_spinner = progress.add(record_corpus_spinner);
    let query_corpus_spinner = progress.add(query_corpus_spinner);

    parent_task.enable_steady_tick(Duration::from_millis(50));
    record_corpus_spinner.enable_steady_tick(Duration::from_millis(50));
    query_corpus_spinner.enable_steady_tick(Duration::from_millis(50));

    let record_corpus_init = RecordCorpus::init().and_then(|r| async {
        record_corpus_spinner.set_style(finish_style.clone());
        record_corpus_spinner.set_prefix("✔︎");
        record_corpus_spinner.finish_and_clear();
        Ok(r)
    });
    let query_corpus_init = QueryCorpus::init().and_then(|r| async {
        query_corpus_spinner.set_style(finish_style.clone());
        query_corpus_spinner.set_prefix("✔");
        query_corpus_spinner.finish_and_clear();
        Ok(r)
    });

    let (record_corpus, query_corpus) = futures::join!(record_corpus_init, query_corpus_init);
    let record_corpus = record_corpus.expect("Failed to initialize record corpus");
    let query_corpus = query_corpus.expect("Failed to initialize query corpus");

    let query_spinner = ProgressBar::new_spinner()
        .with_message("creating query subset...")
        .with_style(style.clone());

    let query_spinner = progress.add(query_spinner);
    query_spinner.enable_steady_tick(Duration::from_millis(50));

    let subset = query_corpus
        .get_or_create_frozen_query_subset(
            &record_corpus,
            min_results_per_query,
            max_num_of_queries,
            None,
        )
        .and_then(|r| async {
            query_spinner.set_style(finish_style.clone());
            query_spinner.set_prefix("✔");
            query_spinner.finish_and_clear();
            Ok(r)
        })
        .await
        .expect("Failed to create query subset");

    progress.clear().unwrap();

    (record_corpus, subset)
}
