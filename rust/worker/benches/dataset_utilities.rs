use std::time::Duration;

use chroma_benchmark_datasets::types::{DocumentDataset, FrozenQuerySubset, QueryDataset};
use futures::TryFutureExt;
use indicatif::{MultiProgress, ProgressBar, ProgressStyle};

pub async fn get_document_dataset<DocumentCorpus: DocumentDataset>() -> DocumentCorpus {
    let style = ProgressStyle::default_spinner()
        .template("{spinner:.green} {msg}")
        .unwrap();

    let finish_style = ProgressStyle::default_spinner()
        .template("{prefix:.green} {msg}")
        .unwrap();

    let document_corpus_spinner = ProgressBar::new_spinner()
        .with_message(DocumentCorpus::get_display_name())
        .with_style(style.clone());
    document_corpus_spinner.enable_steady_tick(Duration::from_millis(50));

    let document_corpus = DocumentCorpus::init()
        .and_then(|r| async {
            document_corpus_spinner.set_style(finish_style.clone());
            document_corpus_spinner.set_prefix("✔︎");
            document_corpus_spinner.finish_and_clear();
            Ok(r)
        })
        .await
        .expect("Failed to initialize document corpus");

    document_corpus
}

pub async fn get_document_query_dataset_pair<
    DocumentCorpus: DocumentDataset,
    QueryCorpus: QueryDataset,
>(
    min_results_per_query: usize,
    max_num_of_queries: usize,
) -> (DocumentCorpus, FrozenQuerySubset) {
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

    let document_corpus_spinner = ProgressBar::new_spinner()
        .with_message(DocumentCorpus::get_display_name())
        .with_style(style.clone());
    let query_corpus_spinner = ProgressBar::new_spinner()
        .with_message(QueryCorpus::get_display_name())
        .with_style(style.clone());

    let parent_task = progress.add(parent_task);
    let document_corpus_spinner = progress.add(document_corpus_spinner);
    let query_corpus_spinner = progress.add(query_corpus_spinner);

    parent_task.enable_steady_tick(Duration::from_millis(50));
    document_corpus_spinner.enable_steady_tick(Duration::from_millis(50));
    query_corpus_spinner.enable_steady_tick(Duration::from_millis(50));

    let document_corpus_init = DocumentCorpus::init().and_then(|r| async {
        document_corpus_spinner.set_style(finish_style.clone());
        document_corpus_spinner.set_prefix("✔︎");
        document_corpus_spinner.finish_and_clear();
        Ok(r)
    });
    let query_corpus_init = QueryCorpus::init().and_then(|r| async {
        query_corpus_spinner.set_style(finish_style.clone());
        query_corpus_spinner.set_prefix("✔");
        query_corpus_spinner.finish_and_clear();
        Ok(r)
    });

    let (document_corpus, query_corpus) = futures::join!(document_corpus_init, query_corpus_init);
    let document_corpus = document_corpus.expect("Failed to initialize document corpus");
    let query_corpus = query_corpus.expect("Failed to initialize query corpus");

    let query_spinner = ProgressBar::new_spinner()
        .with_message("creating query subset...")
        .with_style(style.clone());

    let query_spinner = progress.add(query_spinner);
    query_spinner.enable_steady_tick(Duration::from_millis(50));

    let subset = query_corpus
        .get_or_create_frozen_query_subset(
            &document_corpus,
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

    (document_corpus, subset)
}
