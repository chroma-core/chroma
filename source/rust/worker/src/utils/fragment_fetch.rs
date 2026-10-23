use std::collections::HashSet;
use std::sync::Arc;

use chroma_types::CollectionUuid;

use crate::execution::operators::fragment_fetch::FragmentFetcher;

/// Return the fragment fetcher for `collection_id`, or `None` when fragment fetch is disabled.
///
/// Fragment fetch is enabled when `use_fragment_fetch` is true or the collection appears in
/// `collections_for_fragment_fetch`.
pub(crate) fn fragment_fetcher_for_collection(
    fragment_fetcher: &Option<Arc<FragmentFetcher>>,
    use_fragment_fetch: bool,
    collections_for_fragment_fetch: &HashSet<CollectionUuid>,
    collection_id: CollectionUuid,
) -> Option<Arc<FragmentFetcher>> {
    let fetcher = fragment_fetcher.as_ref()?;
    if use_fragment_fetch || collections_for_fragment_fetch.contains(&collection_id) {
        Some(Arc::clone(fetcher))
    } else {
        None
    }
}
