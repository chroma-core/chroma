use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

use mdac::{Scorecard, ScorecardTicket};

use chroma_config::Configurable;
use chroma_error::ChromaError;
use chroma_sysdb::sysdb;
use chroma_types::{
    operator::{Filter, KnnBatch, KnnProjection, Projection, Scan},
    plan::Knn,
    CollectionUuid, CreateDatabaseError, CreateDatabaseResponse, GetDatabaseError, Include,
    QueryError,
};

use crate::{config::FrontendConfig, executor::Executor};

#[allow(dead_code)]
const DEFAULT_TENANT: &str = "default_tenant";
#[allow(dead_code)]
const DEFAULT_DATABASE: &str = "default_database";

struct ScorecardGuard {
    scorecard: Arc<Scorecard<'static>>,
    ticket: Option<ScorecardTicket>,
}

impl Drop for ScorecardGuard {
    fn drop(&mut self) {
        if let Some(ticket) = self.ticket.take() {
            self.scorecard.untrack(ticket);
        }
    }
}

#[derive(Clone)]
pub struct Frontend {
    #[allow(dead_code)]
    executor: Executor,
    sysdb_client: Box<sysdb::SysDb>,
    scorecard_enabled: Arc<AtomicBool>,
    scorecard: Arc<Scorecard<'static>>,
}

impl Frontend {
    pub fn new(sysdb_client: Box<sysdb::SysDb>) -> Self {
        let scorecard_enabled = Arc::new(AtomicBool::new(false));
        // NOTE(rescrv):  Assume statically no more than 128 threads because we won't deploy on
        // hardware with that many threads anytime soon for frontends, if ever.
        // SAFETY(rescrv):  This is safe because 128 is non-zero.
        let scorecard = Arc::new(Scorecard::new(&(), vec![], 128.try_into().unwrap()));
        Frontend {
            // WARN: This is a placeholder impl, which should be replaced by proper initialization from config
            executor: Executor::default(),
            sysdb_client,
            scorecard_enabled,
            scorecard,
        }
    }

    fn scorecard_request(&self, tags: &[&str]) -> Option<ScorecardGuard> {
        if self.scorecard_enabled.load(Ordering::Relaxed) {
            self.scorecard.track(tags).map(|ticket| ScorecardGuard {
                scorecard: Arc::clone(&self.scorecard),
                ticket: Some(ticket),
            })
        } else {
            Some(ScorecardGuard {
                scorecard: Arc::clone(&self.scorecard),
                ticket: None,
            })
        }
    }

    pub async fn create_database(
        &mut self,
        request: chroma_types::CreateDatabaseRequest,
    ) -> Result<chroma_types::CreateDatabaseResponse, CreateDatabaseError> {
        let tags = &[
            "op:create_database",
            &format!("tenant_id:{}", request.tenant_id),
            &format!("database_id:{}", request.database_id),
        ];
        let _guard = self
            .scorecard_request(tags)
            .ok_or(CreateDatabaseError::RateLimited)?;
        let res = self
            .sysdb_client
            .create_database(
                request.database_id,
                request.database_name,
                request.tenant_id,
            )
            .await;
        match res {
            Ok(()) => Ok(CreateDatabaseResponse {}),
            Err(e) => Err(e),
        }
    }

    pub async fn get_database(
        &mut self,
        request: chroma_types::GetDatabaseRequest,
    ) -> Result<chroma_types::GetDatabaseResponse, GetDatabaseError> {
        self.sysdb_client
            .get_database(request.database_name, request.tenant_id)
            .await
    }

    pub async fn query(
        &mut self,
        request: chroma_types::QueryRequest,
    ) -> Result<chroma_types::QueryResponse, QueryError> {
        let collectio_id = CollectionUuid(request.collection_id);
        let collection_and_segments = self
            .sysdb_client
            .get_collection_with_segments(collectio_id)
            .await
            .map_err(|_| QueryError::CollectionSegments)?;
        let query_result = self
            .executor
            .knn(Knn {
                scan: Scan {
                    collection_and_segments,
                },
                filter: Filter {
                    query_ids: None,
                    where_clause: None,
                },
                knn: KnnBatch {
                    embeddings: request.embeddings,
                    fetch: request.n_results,
                },
                proj: KnnProjection {
                    projection: Projection {
                        document: request.include.includes.contains(&Include::Document),
                        embedding: request.include.includes.contains(&Include::Embedding),
                        metadata: request.include.includes.contains(&Include::Metadata),
                    },
                    distance: request.include.includes.contains(&Include::Distance),
                },
            })
            .await?;
        Ok((query_result, request.include).into())
    }
}

#[async_trait::async_trait]
impl Configurable<FrontendConfig> for Frontend {
    async fn try_from_config(config: &FrontendConfig) -> Result<Self, Box<dyn ChromaError>> {
        let sysdb_client = chroma_sysdb::from_config(&config.sysdb).await?;

        Ok(Frontend::new(sysdb_client))
    }
}
