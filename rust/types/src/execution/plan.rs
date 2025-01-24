use super::operator::{Filter, KnnProjection, Limit, Projection, Scan};

/// The `Count` plan shoud ouutput the total number of records in the collection
pub struct Count {
    pub scan: Scan,
}

/// The `Get` plan should output records matching the specified filter and limit in the collection
pub struct Get {
    pub scan: Scan,
    pub filter: Filter,
    pub limit: Limit,
    pub proj: Projection,
}

/// The `Knn` plan should output records nearest to the target embeddings that matches the specified filter
pub struct Knn {
    pub scan: Scan,
    pub filter: Filter,
    pub knn: Vec<Knn>,
    pub proj: KnnProjection,
}
