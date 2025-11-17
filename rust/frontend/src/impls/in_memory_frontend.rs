use super::utils::to_records;
use chroma_distance::DistanceFunction;
use chroma_error::ChromaError;
use chroma_segment::test::TestReferenceSegment;
use chroma_types::operator::{Filter, KnnBatch, KnnProjection, Limit, Projection, Scan};
use chroma_types::plan::{Count, Get, Knn};
use chroma_types::{
    test_segment, Collection, CollectionAndSegments, CreateCollectionError, Database, Include,
    IncludeList, InternalCollectionConfiguration, KnnIndex, Schema, SchemaError, Segment,
    VectorIndexConfiguration,
};
use std::collections::HashSet;

#[derive(Debug, Clone)]
struct InMemoryCollection {
    collection: Collection,
    metadata_segment: Segment,
    vector_segment: Segment,
    record_segment: Segment,
    reference_impl: TestReferenceSegment,
}

#[derive(Default, Debug, Clone)]
struct Inner {
    tenants: HashSet<String>,
    databases: Vec<Database>,
    collections: Vec<InMemoryCollection>,
}

#[derive(Clone, Default, Debug)]
pub struct InMemoryFrontend {
    inner: Inner,
}

impl InMemoryFrontend {
    pub fn new() -> Self {
        Default::default()
    }

    pub fn reset(&mut self) -> Result<chroma_types::ResetResponse, chroma_types::ResetError> {
        self.inner = Inner::default();
        Ok(chroma_types::ResetResponse {})
    }

    pub fn heartbeat(
        &self,
    ) -> Result<chroma_api_types::HeartbeatResponse, chroma_types::HeartbeatError> {
        Ok(chroma_api_types::HeartbeatResponse {
            nanosecond_heartbeat: 0,
        })
    }

    pub fn get_max_batch_size(&mut self) -> u32 {
        1024
    }

    pub fn create_tenant(
        &mut self,
        request: chroma_types::CreateTenantRequest,
    ) -> Result<chroma_types::CreateTenantResponse, chroma_types::CreateTenantError> {
        let was_new = self.inner.tenants.insert(request.name.clone());
        if !was_new {
            return Err(chroma_types::CreateTenantError::AlreadyExists(request.name));
        }

        Ok(chroma_types::CreateTenantResponse {})
    }

    pub fn get_tenant(
        &mut self,
        request: chroma_types::GetTenantRequest,
    ) -> Result<chroma_types::GetTenantResponse, chroma_types::GetTenantError> {
        if self.inner.tenants.contains(&request.name) {
            Ok(chroma_types::GetTenantResponse {
                name: request.name,
                resource_name: None,
            })
        } else {
            Err(chroma_types::GetTenantError::NotFound(request.name))
        }
    }

    pub fn create_database(
        &mut self,
        request: chroma_types::CreateDatabaseRequest,
    ) -> Result<chroma_types::CreateDatabaseResponse, chroma_types::CreateDatabaseError> {
        if self.inner.databases.iter().any(|db| {
            db.id == request.database_id
                || (db.name == request.database_name && db.tenant == request.tenant_id)
        }) {
            return Err(chroma_types::CreateDatabaseError::AlreadyExists(
                request.database_name,
            ));
        }

        self.inner.databases.push(Database {
            id: request.database_id,
            name: request.database_name,
            tenant: request.tenant_id,
        });

        Ok(chroma_types::CreateDatabaseResponse {})
    }

    pub fn list_databases(
        &mut self,
        request: chroma_types::ListDatabasesRequest,
    ) -> Result<chroma_types::ListDatabasesResponse, chroma_types::ListDatabasesError> {
        let databases: Vec<_> = self
            .inner
            .databases
            .iter()
            .filter(|db| db.tenant == request.tenant_id)
            .skip(request.offset as usize)
            .take(request.limit.unwrap_or(10000000) as usize)
            .cloned()
            .collect();

        Ok(databases)
    }

    pub fn get_database(
        &mut self,
        request: chroma_types::GetDatabaseRequest,
    ) -> Result<chroma_types::GetDatabaseResponse, chroma_types::GetDatabaseError> {
        if let Some(db) = self
            .inner
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

    pub fn delete_database(
        &mut self,
        request: chroma_types::DeleteDatabaseRequest,
    ) -> Result<chroma_types::DeleteDatabaseResponse, chroma_types::DeleteDatabaseError> {
        if let Some(pos) = self
            .inner
            .databases
            .iter()
            .position(|db| db.name == request.database_name && db.tenant == request.tenant_id)
        {
            self.inner.databases.remove(pos);
            Ok(chroma_types::DeleteDatabaseResponse {})
        } else {
            Err(chroma_types::DeleteDatabaseError::NotFound(
                request.database_name,
            ))
        }
    }

    pub fn list_collections(
        &mut self,
        request: chroma_types::ListCollectionsRequest,
    ) -> Result<chroma_types::ListCollectionsResponse, chroma_types::GetCollectionsError> {
        let collections: Vec<_> = self
            .inner
            .collections
            .iter()
            .filter(|c| {
                c.collection.tenant == request.tenant_id
                    && c.collection.database == request.database_name
            })
            .skip(request.offset as usize)
            .take(request.limit.unwrap_or(10000000) as usize)
            .map(|c| c.collection.clone())
            .collect();

        Ok(collections)
    }

    pub fn count_collections(
        &mut self,
        request: chroma_types::CountCollectionsRequest,
    ) -> Result<chroma_types::CountCollectionsResponse, chroma_types::CountCollectionsError> {
        let count = self
            .inner
            .collections
            .iter()
            .filter(|c| {
                c.collection.tenant == request.tenant_id
                    && c.collection.database == request.database_name
            })
            .count();

        Ok(count as u32)
    }

    pub fn get_collection(
        &mut self,
        request: chroma_types::GetCollectionRequest,
    ) -> Result<chroma_types::GetCollectionResponse, chroma_types::GetCollectionError> {
        if let Some(collection) = self.inner.collections.iter().find(|c| {
            c.collection.name == request.collection_name && c.collection.tenant == request.tenant_id
        }) {
            Ok(collection.collection.clone())
        } else {
            Err(chroma_types::GetCollectionError::NotFound(
                request.collection_name,
            ))
        }
    }

    pub fn create_collection(
        &mut self,
        request: chroma_types::CreateCollectionRequest,
    ) -> Result<chroma_types::CreateCollectionResponse, chroma_types::CreateCollectionError> {
        if self.inner.collections.iter().any(|c| {
            c.collection.name == request.name
                && c.collection.tenant == request.tenant_id
                && c.collection.database == request.database_name
        }) {
            return Err(chroma_types::CreateCollectionError::AlreadyExists(
                request.name,
            ));
        }

        let schema = Schema::reconcile_schema_and_config(
            request.schema.as_ref(),
            request.configuration.as_ref(),
            KnnIndex::Hnsw,
        )
        .map_err(CreateCollectionError::InvalidSchema)?;

        let config = InternalCollectionConfiguration::try_from(&schema).map_err(|e| {
            CreateCollectionError::InvalidSchema(SchemaError::InvalidSchema { reason: e })
        })?;

        let collection = Collection {
            name: request.name.clone(),
            tenant: request.tenant_id.clone(),
            database: request.database_name.clone(),
            metadata: request.metadata,
            config,
            schema: Some(schema),
            ..Default::default()
        };

        // Prevent SPANN usage in InMemoryFrontend
        if matches!(
            collection.config.vector_index,
            VectorIndexConfiguration::Spann(_)
        ) {
            return Err(CreateCollectionError::SpannNotImplemented);
        }

        let metadata_segment = test_segment(
            collection.collection_id,
            chroma_types::SegmentScope::METADATA,
        );
        let vector_segment =
            test_segment(collection.collection_id, chroma_types::SegmentScope::VECTOR);
        let record_segment =
            test_segment(collection.collection_id, chroma_types::SegmentScope::RECORD);

        let mut reference_impl = TestReferenceSegment::default();
        reference_impl.create_segment(metadata_segment.clone());
        reference_impl.create_segment(vector_segment.clone());
        reference_impl.create_segment(record_segment.clone());

        self.inner.collections.push(InMemoryCollection {
            collection: collection.clone(),
            metadata_segment,
            vector_segment,
            record_segment,
            reference_impl,
        });

        Ok(collection)
    }

    pub fn update_collection(
        &mut self,
        _request: chroma_types::UpdateCollectionRequest,
    ) -> Result<chroma_types::UpdateCollectionResponse, chroma_types::UpdateCollectionError> {
        unimplemented!()
    }

    pub fn delete_collection(
        &mut self,
        request: chroma_types::DeleteCollectionRequest,
    ) -> Result<chroma_types::DeleteCollectionRecordsResponse, chroma_types::DeleteCollectionError>
    {
        let inner = &mut self.inner;
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

    pub fn add(
        &mut self,
        request: chroma_types::AddCollectionRecordsRequest,
    ) -> Result<chroma_types::AddCollectionRecordsResponse, chroma_types::AddCollectionRecordsError>
    {
        let collection = self
            .inner
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

        let embeddings = Some(embeddings.into_iter().map(Some).collect());

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

    pub fn update(
        &mut self,
        request: chroma_types::UpdateCollectionRecordsRequest,
    ) -> Result<
        chroma_types::UpdateCollectionRecordsResponse,
        chroma_types::UpdateCollectionRecordsError,
    > {
        let collection = self
            .inner
            .collections
            .iter_mut()
            .find(|c| {
                c.collection.collection_id == request.collection_id
                    && c.collection.tenant == request.tenant_id
                    && c.collection.database == request.database_name
            })
            .ok_or(chroma_types::UpdateCollectionRecordsError::Other(
                chroma_types::GetCollectionError::NotFound(request.collection_id.to_string())
                    .boxed(),
            ))?;

        let chroma_types::UpdateCollectionRecordsRequest {
            ids,
            embeddings,
            documents,
            metadatas,
            uris,
            ..
        } = request;

        let (records, _) = to_records(
            ids,
            embeddings,
            documents,
            uris,
            metadatas,
            chroma_types::Operation::Update,
        )
        .map_err(|e| e.boxed())?;

        collection
            .reference_impl
            .apply_operation_records(records, collection.metadata_segment.id);

        Ok(chroma_types::UpdateCollectionRecordsResponse {})
    }

    pub fn upsert(
        &mut self,
        request: chroma_types::UpsertCollectionRecordsRequest,
    ) -> Result<
        chroma_types::UpsertCollectionRecordsResponse,
        chroma_types::UpsertCollectionRecordsError,
    > {
        let collection = self
            .inner
            .collections
            .iter_mut()
            .find(|c| {
                c.collection.collection_id == request.collection_id
                    && c.collection.tenant == request.tenant_id
                    && c.collection.database == request.database_name
            })
            .ok_or(chroma_types::UpsertCollectionRecordsError::Other(
                chroma_types::GetCollectionError::NotFound(request.collection_id.to_string())
                    .boxed(),
            ))?;

        let chroma_types::UpsertCollectionRecordsRequest {
            ids,
            embeddings,
            documents,
            metadatas,
            uris,
            ..
        } = request;

        let embeddings = Some(embeddings.into_iter().map(Some).collect());

        let (records, _) = to_records(
            ids,
            embeddings,
            documents,
            uris,
            metadatas,
            chroma_types::Operation::Upsert,
        )
        .map_err(|e| e.boxed())?;

        collection
            .reference_impl
            .apply_operation_records(records, collection.metadata_segment.id);

        Ok(chroma_types::UpsertCollectionRecordsResponse {})
    }

    pub fn delete(
        &mut self,
        request: chroma_types::DeleteCollectionRecordsRequest,
    ) -> Result<
        chroma_types::DeleteCollectionRecordsResponse,
        chroma_types::DeleteCollectionRecordsError,
    > {
        if request.ids.is_none() && request.r#where.is_none() {
            return Ok(chroma_types::DeleteCollectionRecordsResponse {});
        }

        let ids_to_delete = self
            .get(
                chroma_types::GetRequest::try_new(
                    request.tenant_id.clone(),
                    request.database_name.clone(),
                    request.collection_id,
                    request.ids,
                    request.r#where,
                    None,
                    0,
                    IncludeList::empty(),
                )
                .unwrap(),
            )
            .map_err(|e| e.boxed())
            .map(|response| response.ids)?;

        let collection = self
            .inner
            .collections
            .iter_mut()
            .find(|c| {
                c.collection.collection_id == request.collection_id
                    && c.collection.tenant == request.tenant_id
                    && c.collection.database == request.database_name
            })
            .ok_or(chroma_types::DeleteCollectionRecordsError::Internal(
                chroma_types::GetCollectionError::NotFound(request.collection_id.to_string())
                    .boxed(),
            ))?;

        let records = ids_to_delete
            .into_iter()
            .map(|id| chroma_types::OperationRecord {
                id,
                operation: chroma_types::Operation::Delete,
                encoding: None,
                embedding: None,
                document: None,
                metadata: None,
            })
            .collect::<Vec<_>>();
        collection
            .reference_impl
            .apply_operation_records(records, collection.metadata_segment.id);

        Ok(chroma_types::DeleteCollectionRecordsResponse {})
    }

    pub fn count(
        &self,
        request: chroma_types::CountRequest,
    ) -> Result<chroma_types::CountResponse, chroma_types::QueryError> {
        let collection = self
            .inner
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

    pub fn get(
        &self,
        request: chroma_types::GetRequest,
    ) -> Result<chroma_types::GetResponse, chroma_types::QueryError> {
        let collection = self
            .inner
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
                limit: Limit { offset, limit },
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

    pub fn query(
        &mut self,
        request: chroma_types::QueryRequest,
    ) -> Result<chroma_types::QueryResponse, chroma_types::QueryError> {
        let collection = self
            .inner
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

        let chroma_types::QueryRequest {
            r#where,
            include,
            ids,
            embeddings,
            n_results,
            ..
        } = request;

        let filter = Filter {
            query_ids: ids,
            where_clause: r#where,
        };

        let params = collection
            .collection
            .schema
            .as_ref()
            .map(|schema| {
                schema.get_internal_hnsw_config_with_legacy_fallback(&collection.vector_segment)
            })
            .transpose()
            .map_err(|e| e.boxed())?
            .flatten()
            .expect("HNSW configuration missing for collection schema");
        let distance_function: DistanceFunction = params.space.into();

        let query_response = collection
            .reference_impl
            .knn(
                Knn {
                    scan: Scan {
                        collection_and_segments: CollectionAndSegments {
                            collection: collection.collection.clone(),
                            metadata_segment: collection.metadata_segment.clone(),
                            vector_segment: collection.vector_segment.clone(),
                            record_segment: collection.record_segment.clone(),
                        },
                    },
                    filter,
                    knn: KnnBatch {
                        embeddings,
                        fetch: n_results,
                    },
                    proj: KnnProjection {
                        projection: Projection {
                            document: include.0.contains(&Include::Document),
                            embedding: include.0.contains(&Include::Embedding),
                            // If URI is requested, metadata is also requested so we can extract the URI.
                            metadata: (include.0.contains(&Include::Metadata)
                                || include.0.contains(&Include::Uri)),
                        },
                        distance: include.0.contains(&Include::Distance),
                    },
                },
                distance_function,
            )
            .map_err(|e| e.boxed())?;

        Ok((query_response, include).into())
    }

    pub fn healthcheck(&self) -> chroma_types::HealthCheckResponse {
        chroma_types::HealthCheckResponse {
            is_executor_ready: true,
            is_log_client_ready: true,
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

    fn create_test_collection() -> (InMemoryFrontend, Collection) {
        let tenant_name = "test".to_string();
        let database_name = "test".to_string();
        let collection_name = "test".to_string();

        let mut frontend = InMemoryFrontend::new();
        let request = chroma_types::CreateTenantRequest::try_new(tenant_name.clone()).unwrap();
        frontend.create_tenant(request).unwrap();

        let request = chroma_types::CreateDatabaseRequest::try_new(
            tenant_name.clone(),
            database_name.clone(),
        )
        .unwrap();
        frontend.create_database(request).unwrap();

        let request = chroma_types::CreateCollectionRequest::try_new(
            tenant_name.clone(),
            database_name.clone(),
            collection_name.clone(),
            None,
            None,
            None,
            false,
        )
        .unwrap();

        let collection = frontend.create_collection(request).unwrap();
        (frontend, collection)
    }

    #[test]
    fn test_collection_get_query() {
        let (mut frontend, collection) = create_test_collection();
        let ids = vec!["id1".to_string(), "id2".to_string()];
        let embeddings = vec![vec![-1.0, -1.0, -1.0], vec![0.0, 0.0, 0.0]];
        let documents = vec![Some("doc1".to_string()), Some("doc2".to_string())];

        let mut metadata1 = Metadata::new();
        metadata1.insert("key1".to_string(), MetadataValue::Str("value1".to_string()));
        metadata1.insert("key2".to_string(), MetadataValue::Int(16));

        let mut metadata2 = Metadata::new();
        metadata2.insert("key1".to_string(), MetadataValue::Str("value2".to_string()));
        metadata2.insert("key2".to_string(), MetadataValue::Int(32));

        let metadatas = vec![Some(metadata1), Some(metadata2)];

        let request = chroma_types::AddCollectionRecordsRequest::try_new(
            collection.tenant.clone(),
            collection.database.clone(),
            collection.collection_id,
            ids,
            embeddings,
            Some(documents),
            None,
            Some(metadatas),
        )
        .unwrap();
        frontend.add(request).unwrap();

        // Test count
        let count = frontend
            .count(
                chroma_types::CountRequest::try_new(
                    collection.tenant.clone(),
                    collection.database.clone(),
                    collection.collection_id,
                )
                .unwrap(),
            )
            .unwrap();
        assert_eq!(count, 2);

        // Test metadata filter
        let request = chroma_types::GetRequest::try_new(
            collection.tenant.clone(),
            collection.database.clone(),
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
        let response = frontend.get(request).unwrap();
        assert_eq!(response.ids.len(), 1);
        assert_eq!(response.ids[0], "id1");

        // Test full text query
        let request = chroma_types::GetRequest::try_new(
            collection.tenant.clone(),
            collection.database.clone(),
            collection.collection_id,
            None,
            Some(Where::Document(DocumentExpression {
                operator: chroma_types::DocumentOperator::Contains,
                pattern: "doc2".to_string(),
            })),
            None,
            0,
            IncludeList::default_get(),
        )
        .unwrap();
        let response = frontend.get(request).unwrap();
        assert_eq!(response.ids.len(), 1);
        assert_eq!(response.ids[0], "id2");

        // Test vector query
        let request = chroma_types::QueryRequest::try_new(
            collection.tenant.clone(),
            collection.database.clone(),
            collection.collection_id,
            None,
            None,
            vec![vec![0.5, 0.5, 0.5]],
            10,
            IncludeList::default_query(),
        )
        .unwrap();
        let response = frontend.query(request).unwrap();
        assert_eq!(response.ids[0].len(), 2);
        assert_eq!(response.ids[0], vec!["id2", "id1"]);
    }
}
