use chroma_error::ChromaError;
use chroma_segment::test::TestReferenceSegment;
use chroma_types::operator::{Filter, Limit, Projection, Scan};
use chroma_types::plan::{Count, Get};
use chroma_types::{
    test_segment, Collection, CollectionAndSegments, Database, Include,
    InternalCollectionConfiguration, Segment,
};
use parking_lot::Mutex;
use std::collections::HashSet;
use std::sync::Arc;

use super::utils::to_records;

struct InMemoryCollection {
    collection: Collection,
    metadata_segment: Segment,
    vector_segment: Segment,
    record_segment: Segment,
    reference_impl: TestReferenceSegment,
}

#[derive(Default)]
struct Inner {
    tenants: HashSet<String>,
    databases: Vec<Database>,
    collections: Vec<InMemoryCollection>,
}

#[derive(Clone, Default)]
pub struct InMemoryFrontend {
    inner: Arc<Mutex<Inner>>,
}

impl InMemoryFrontend {
    pub fn new() -> Self {
        Default::default()
    }

    pub async fn reset(&mut self) -> Result<chroma_types::ResetResponse, chroma_types::ResetError> {
        let mut inner = self.inner.lock();
        *inner = Inner::default();

        Ok(chroma_types::ResetResponse {})
    }

    pub async fn heartbeat(
        &self,
    ) -> Result<chroma_types::HeartbeatResponse, chroma_types::HeartbeatError> {
        Ok(chroma_types::HeartbeatResponse {
            nanosecond_heartbeat: 0,
        })
    }

    pub fn get_max_batch_size(&mut self) -> u32 {
        1024 // Example placeholder
    }

    pub async fn create_tenant(
        &mut self,
        request: chroma_types::CreateTenantRequest,
    ) -> Result<chroma_types::CreateTenantResponse, chroma_types::CreateTenantError> {
        let mut inner = self.inner.lock();

        let was_new = inner.tenants.insert(request.name.clone());
        if !was_new {
            return Err(chroma_types::CreateTenantError::AlreadyExists(request.name));
        }

        Ok(chroma_types::CreateTenantResponse {})
    }

    pub async fn get_tenant(
        &mut self,
        request: chroma_types::GetTenantRequest,
    ) -> Result<chroma_types::GetTenantResponse, chroma_types::GetTenantError> {
        let inner = self.inner.lock();
        if inner.tenants.contains(&request.name) {
            Ok(chroma_types::GetTenantResponse { name: request.name })
        } else {
            Err(chroma_types::GetTenantError::NotFound(request.name))
        }
    }

    pub async fn create_database(
        &mut self,
        request: chroma_types::CreateDatabaseRequest,
    ) -> Result<chroma_types::CreateDatabaseResponse, chroma_types::CreateDatabaseError> {
        let mut inner = self.inner.lock();

        if inner.databases.iter().any(|db| {
            db.id == request.database_id
                || (db.name == request.database_name && db.tenant == request.tenant_id)
        }) {
            return Err(chroma_types::CreateDatabaseError::AlreadyExists(
                request.database_name,
            ));
        }

        inner.databases.push(Database {
            id: request.database_id,
            name: request.database_name,
            tenant: request.tenant_id,
        });

        Ok(chroma_types::CreateDatabaseResponse {})
    }

    pub async fn list_databases(
        &mut self,
        request: chroma_types::ListDatabasesRequest,
    ) -> Result<chroma_types::ListDatabasesResponse, chroma_types::ListDatabasesError> {
        let inner = self.inner.lock();
        let databases: Vec<_> = inner
            .databases
            .iter()
            .filter(|db| db.tenant == request.tenant_id)
            .cloned()
            .collect();

        Ok(databases[request.offset as usize
            ..request.offset as usize + request.limit.unwrap_or(10000000) as usize]
            .to_vec())
    }

    pub async fn get_database(
        &mut self,
        request: chroma_types::GetDatabaseRequest,
    ) -> Result<chroma_types::GetDatabaseResponse, chroma_types::GetDatabaseError> {
        let inner = self.inner.lock();
        if let Some(db) = inner
            .databases
            .iter()
            .find(|db| db.name == request.database_name && db.tenant == request.tenant_id)
        {
            Ok(db.clone())
        } else {
            Err(chroma_types::GetDatabaseError::NotFound(
                request.database_name,
            ))
        }
    }

    pub async fn delete_database(
        &mut self,
        request: chroma_types::DeleteDatabaseRequest,
    ) -> Result<chroma_types::DeleteDatabaseResponse, chroma_types::DeleteDatabaseError> {
        let mut inner = self.inner.lock();
        if let Some(pos) = inner
            .databases
            .iter()
            .position(|db| db.name == request.database_name && db.tenant == request.tenant_id)
        {
            inner.databases.remove(pos);
            Ok(chroma_types::DeleteDatabaseResponse {})
        } else {
            Err(chroma_types::DeleteDatabaseError::NotFound(
                request.database_name,
            ))
        }
    }

    pub async fn list_collections(
        &mut self,
        request: chroma_types::ListCollectionsRequest,
    ) -> Result<chroma_types::ListCollectionsResponse, chroma_types::GetCollectionsError> {
        let inner = self.inner.lock();
        let collections: Vec<_> = inner
            .collections
            .iter()
            .filter(|c| {
                c.collection.tenant == request.tenant_id
                    && c.collection.database == request.database_name
            })
            .map(|c| c.collection.clone())
            .collect();

        Ok(collections[request.offset as usize
            ..request.offset as usize + request.limit.unwrap_or(10000000) as usize]
            .to_vec())
    }

    pub async fn count_collections(
        &mut self,
        request: chroma_types::CountCollectionsRequest,
    ) -> Result<chroma_types::CountCollectionsResponse, chroma_types::CountCollectionsError> {
        let inner = self.inner.lock();
        let count = inner
            .collections
            .iter()
            .filter(|c| {
                c.collection.tenant == request.tenant_id
                    && c.collection.database == request.database_name
            })
            .count();

        Ok(count as u32)
    }

    pub async fn get_collection(
        &mut self,
        request: chroma_types::GetCollectionRequest,
    ) -> Result<chroma_types::GetCollectionResponse, chroma_types::GetCollectionError> {
        let inner = self.inner.lock();
        if let Some(collection) = inner.collections.iter().find(|c| {
            c.collection.name == request.collection_name && c.collection.tenant == request.tenant_id
        }) {
            Ok(collection.collection.clone())
        } else {
            Err(chroma_types::GetCollectionError::NotFound(
                request.collection_name,
            ))
        }
    }

    pub async fn create_collection(
        &mut self,
        request: chroma_types::CreateCollectionRequest,
    ) -> Result<chroma_types::CreateCollectionResponse, chroma_types::CreateCollectionError> {
        let mut inner = self.inner.lock();

        if inner.collections.iter().any(|c| {
            c.collection.name == request.name
                && c.collection.tenant == request.tenant_id
                && c.collection.database == request.database_name
        }) {
            return Err(chroma_types::CreateCollectionError::AlreadyExists(
                request.name,
            ));
        }

        let collection = Collection {
            name: request.name,
            tenant: request.tenant_id,
            database: request.database_name,
            config: request
                .configuration
                .map(|c| c.try_into().unwrap())
                .unwrap_or(InternalCollectionConfiguration::default_hnsw()),
            ..Default::default()
        };

        let reference_impl = TestReferenceSegment::default();

        inner.collections.push(InMemoryCollection {
            collection: collection.clone(),
            metadata_segment: test_segment(
                collection.collection_id,
                chroma_types::SegmentScope::METADATA,
            ),
            vector_segment: test_segment(
                collection.collection_id,
                chroma_types::SegmentScope::VECTOR,
            ),
            record_segment: test_segment(
                collection.collection_id,
                chroma_types::SegmentScope::RECORD,
            ),
            reference_impl,
        });

        Ok(collection)
    }

    pub async fn update_collection(
        &mut self,
        _request: chroma_types::UpdateCollectionRequest,
    ) -> Result<chroma_types::UpdateCollectionResponse, chroma_types::UpdateCollectionError> {
        Ok(chroma_types::UpdateCollectionResponse {})
    }

    pub async fn delete_collection(
        &mut self,
        request: chroma_types::DeleteCollectionRequest,
    ) -> Result<chroma_types::DeleteCollectionRecordsResponse, chroma_types::DeleteCollectionError>
    {
        let mut inner = self.inner.lock();
        if let Some(pos) = inner.collections.iter().position(|c| {
            c.collection.name == request.collection_name
                && c.collection.tenant == request.tenant_id
                && c.collection.database == request.database_name
        }) {
            inner.collections.remove(pos);
            Ok(chroma_types::DeleteCollectionRecordsResponse {})
        } else {
            Err(chroma_types::DeleteCollectionError::NotFound(
                request.collection_name,
            ))
        }
    }

    pub async fn add(
        &mut self,
        request: chroma_types::AddCollectionRecordsRequest,
    ) -> Result<chroma_types::AddCollectionRecordsResponse, chroma_types::AddCollectionRecordsError>
    {
        let mut inner = self.inner.lock();
        let collection = inner
            .collections
            .iter_mut()
            .find(|c| {
                c.collection.collection_id == request.collection_id
                    && c.collection.tenant == request.tenant_id
                    && c.collection.database == request.database_name
            })
            .ok_or(chroma_types::AddCollectionRecordsError::Collection(
                chroma_types::GetCollectionError::NotFound(request.collection_id.to_string()),
            ))?;

        let chroma_types::AddCollectionRecordsRequest {
            ids,
            embeddings,
            documents,
            metadatas,
            uris,
            ..
        } = request;

        let embeddings = embeddings.map(|embeddings| embeddings.into_iter().map(Some).collect());

        let (records, _) = to_records(
            ids,
            embeddings,
            documents,
            uris,
            metadatas,
            chroma_types::Operation::Add,
        )
        .map_err(|e| e.boxed())?;

        collection
            .reference_impl
            .apply_operation_records(records, collection.metadata_segment.id);

        Ok(chroma_types::AddCollectionRecordsResponse {})
    }

    pub async fn update(
        &mut self,
        _request: chroma_types::UpdateCollectionRecordsRequest,
    ) -> Result<
        chroma_types::UpdateCollectionRecordsResponse,
        chroma_types::UpdateCollectionRecordsError,
    > {
        Ok(chroma_types::UpdateCollectionRecordsResponse {})
    }

    pub async fn upsert(
        &mut self,
        _request: chroma_types::UpsertCollectionRecordsRequest,
    ) -> Result<
        chroma_types::UpsertCollectionRecordsResponse,
        chroma_types::UpsertCollectionRecordsError,
    > {
        Ok(chroma_types::UpsertCollectionRecordsResponse {})
    }

    pub async fn delete(
        &mut self,
        _request: chroma_types::DeleteCollectionRecordsRequest,
    ) -> Result<
        chroma_types::DeleteCollectionRecordsResponse,
        chroma_types::DeleteCollectionRecordsError,
    > {
        Ok(chroma_types::DeleteCollectionRecordsResponse {})
    }

    pub async fn count(
        &mut self,
        request: chroma_types::CountRequest,
    ) -> Result<chroma_types::CountResponse, chroma_types::QueryError> {
        let inner = self.inner.lock();
        let collection = inner
            .collections
            .iter()
            .find(|c| {
                c.collection.collection_id == request.collection_id
                    && c.collection.tenant == request.tenant_id
                    && c.collection.database == request.database_name
            })
            .ok_or(
                chroma_types::GetCollectionError::NotFound(request.collection_id.to_string())
                    .boxed(),
            )?;

        let count = collection
            .reference_impl
            .count(Count {
                scan: Scan {
                    collection_and_segments: CollectionAndSegments {
                        collection: collection.collection.clone(),
                        metadata_segment: collection.metadata_segment.clone(),
                        vector_segment: collection.vector_segment.clone(),
                        record_segment: collection.record_segment.clone(),
                    },
                },
            })
            .map_err(|e| e.boxed())?;

        Ok(count.count)
    }

    pub async fn get(
        &mut self,
        request: chroma_types::GetRequest,
    ) -> Result<chroma_types::GetResponse, chroma_types::QueryError> {
        let inner = self.inner.lock();
        let collection = inner
            .collections
            .iter()
            .find(|c| {
                c.collection.collection_id == request.collection_id
                    && c.collection.tenant == request.tenant_id
                    && c.collection.database == request.database_name
            })
            .ok_or(
                chroma_types::GetCollectionError::NotFound(request.collection_id.to_string())
                    .boxed(),
            )?;

        let chroma_types::GetRequest {
            ids,
            include,
            r#where,
            offset,
            limit,
            ..
        } = request;

        let filter = Filter {
            query_ids: ids,
            where_clause: r#where,
        };

        let get_response = collection
            .reference_impl
            .get(Get {
                scan: Scan {
                    collection_and_segments: CollectionAndSegments {
                        collection: collection.collection.clone(),
                        metadata_segment: collection.metadata_segment.clone(),
                        vector_segment: collection.vector_segment.clone(),
                        record_segment: collection.record_segment.clone(),
                    },
                },
                filter,
                limit: Limit {
                    skip: offset,
                    fetch: limit,
                },
                proj: Projection {
                    document: include.0.contains(&Include::Document),
                    embedding: include.0.contains(&Include::Embedding),
                    // If URI is requested, metadata is also requested so we can extract the URI.
                    metadata: (include.0.contains(&Include::Metadata)
                        || include.0.contains(&Include::Uri)),
                },
            })
            .map_err(|e| e.boxed())?;

        Ok((get_response, include).into())
    }

    pub async fn query(
        &mut self,
        _request: chroma_types::QueryRequest,
    ) -> Result<chroma_types::QueryResponse, chroma_types::QueryError> {
        todo!()
    }

    pub async fn healthcheck(&self) -> chroma_types::HealthCheckResponse {
        chroma_types::HealthCheckResponse {
            is_executor_ready: true, // Example placeholder
        }
    }
}

#[cfg(test)]
mod tests {
    use chroma_types::{
        DocumentExpression, IncludeList, Metadata, MetadataComparison, MetadataExpression,
        MetadataValue, PrimitiveOperator, Where,
    };

    use super::*;

    #[tokio::test]
    async fn test_collection() {
        let tenant_name = "test".to_string();
        let database_name = "test".to_string();
        let collection_name = "test".to_string();

        let mut frontend = InMemoryFrontend::new();
        let request = chroma_types::CreateTenantRequest::try_new(tenant_name.clone()).unwrap();
        frontend.create_tenant(request).await.unwrap();

        let request = chroma_types::CreateDatabaseRequest::try_new(
            tenant_name.clone(),
            database_name.clone(),
        )
        .unwrap();
        frontend.create_database(request).await.unwrap();

        let request = chroma_types::CreateCollectionRequest::try_new(
            tenant_name.clone(),
            database_name.clone(),
            collection_name.clone(),
            None,
            None,
            false,
        )
        .unwrap();
        let collection = frontend.create_collection(request).await.unwrap();

        let ids = vec!["id1".to_string(), "id2".to_string()];
        let embeddings = vec![vec![1.0, 1.0, 1.0], vec![2.0, 2.0, 2.0]];
        let documents = vec![Some("doc1".to_string()), Some("doc2".to_string())];

        let mut metadata1 = Metadata::new();
        metadata1.insert("key1".to_string(), MetadataValue::Str("value1".to_string()));
        metadata1.insert("key2".to_string(), MetadataValue::Int(16));

        let mut metadata2 = Metadata::new();
        metadata2.insert("key1".to_string(), MetadataValue::Str("value2".to_string()));
        metadata2.insert("key2".to_string(), MetadataValue::Int(32));

        let metadatas = vec![Some(metadata1), Some(metadata2)];

        let request = chroma_types::AddCollectionRecordsRequest::try_new(
            tenant_name.clone(),
            database_name.clone(),
            collection.collection_id,
            ids,
            Some(embeddings),
            Some(documents),
            None,
            Some(metadatas),
        )
        .unwrap();
        frontend.add(request).await.unwrap();

        // Test count
        let count = frontend
            .count(
                chroma_types::CountRequest::try_new(
                    tenant_name.clone(),
                    database_name.clone(),
                    collection.collection_id,
                )
                .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(count, 2);

        // Test metadata filter
        let request = chroma_types::GetRequest::try_new(
            tenant_name.clone(),
            database_name.clone(),
            collection.collection_id,
            None,
            Some(Where::Metadata(MetadataExpression {
                key: "key1".to_string(),
                comparison: MetadataComparison::Primitive(
                    PrimitiveOperator::Equal,
                    MetadataValue::Str("value1".to_string()),
                ),
            })),
            None,
            0,
            IncludeList::default_get(),
        )
        .unwrap();
        let response = frontend.get(request).await.unwrap();
        assert_eq!(response.ids.len(), 1);
        assert_eq!(response.ids[0], "id1");

        // Test full text index
        let request = chroma_types::GetRequest::try_new(
            tenant_name.clone(),
            database_name.clone(),
            collection.collection_id,
            None,
            Some(Where::Document(DocumentExpression {
                operator: chroma_types::DocumentOperator::Contains,
                text: "doc2".to_string(),
            })),
            None,
            0,
            IncludeList::default_get(),
        )
        .unwrap();
        let response = frontend.get(request).await.unwrap();
        assert_eq!(response.ids.len(), 1);
        assert_eq!(response.ids[0], "id2");
    }
}
