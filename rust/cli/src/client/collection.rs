use crate::client::chroma_client::ChromaClient;
use crate::client::prelude::CollectionModel;
use crate::client::utils::send_request;
use chroma_frontend::server::GetRequestPayload;
use chroma_types::{CountResponse, GetResponse, IncludeList, RawWhereFields};
use reqwest::Method;
use std::error::Error;
use std::ops::Deref;
use serde_json::{json, Map, Value};
use thiserror::Error;

#[derive(Debug, Error)]
pub enum CollectionAPIError {
    #[error("Failed to count records from collection {0}")]
    Count(String),
    #[error("Failed to get records from collection {0}")]
    Get(String),
}

#[derive(Debug, Clone, Default)]
pub struct Collection {
    chroma_client: ChromaClient,
    collection: CollectionModel,
}

impl Deref for Collection {
    type Target = CollectionModel;

    fn deref(&self) -> &Self::Target {
        &self.collection
    }
}

impl Collection {
    pub fn new(chroma_client: ChromaClient, collection: CollectionModel) -> Self {
        Self {
            chroma_client,
            collection,
        }
    }

    pub async fn get(
        &self,
        ids: Option<Vec<String>>,
        r#where: Option<&str>,
        where_document: Option<&str>,
        include: Option<IncludeList>,
        limit: Option<u32>,
        offset: Option<u32>,
    ) -> Result<GetResponse, Box<dyn Error>> {
        let route = format!(
            "/api/v2/tenants/{}/databases/{}/collections/{}/get",
            self.chroma_client.tenant_id, self.chroma_client.db, self.collection.collection_id
        );

        let mut payload = Map::new();

        if let Some(ids) = ids {
            payload.insert("ids".to_string(), json!(ids));
        }

        if let Some(r#where) = r#where {
            let parsed: Value = serde_json::from_str(r#where)?;
            payload.insert("where".to_string(), parsed);
        }

        if let Some(where_document) = where_document {
            let parsed: Value = serde_json::from_str(where_document)?;
            payload.insert("where_document".to_string(), parsed);
        }

        if let Some(include) = include {
            payload.insert("include".to_string(), json!(include));
        }

        if let Some(limit) = limit {
            payload.insert("limit".to_string(), json!(limit));
        }

        if let Some(offset) = offset {
            payload.insert("offset".to_string(), json!(offset));
        }
        
        let response = send_request::<Map<String, Value>, GetResponse>(
            &self.chroma_client.host,
            Method::POST,
            &route,
            self.chroma_client.headers()?,
            Some(&payload),
        )
        .await
        .map_err(|e| {
            CollectionAPIError::Get(self.collection.name.clone())
        })?;
        Ok(response)
    }

    pub async fn count(&self) -> Result<CountResponse, Box<dyn Error>> {
        let route = format!(
            "/api/v2/tenants/{}/databases/{}/collections/{}/count",
            self.chroma_client.tenant_id, self.chroma_client.db, self.collection.collection_id
        );
        let response = send_request::<(), CountResponse>(
            &self.chroma_client.host,
            Method::GET,
            &route,
            self.chroma_client.headers()?,
            None,
        )
        .await
        .map_err(|_| CollectionAPIError::Count(self.collection.name.clone()))?;
        Ok(response)
    }
}

mod tests {
    use futures_util::TryStreamExt;
    use serde_json::{Map, Value};
    use chroma_types::RawWhereFields;
    use crate::client::admin_client::AdminClient;
    use crate::client::chroma_client::ChromaClient;
    use crate::tui::collection_browser::app::App;
    use crate::tui::collection_browser::query_editor::Operator;
    use crate::utils::{get_current_profile, AddressBook};
    
    #[tokio::test]
    async fn test_get() {
        let profile = get_current_profile().expect("Failed to get current profile");
        let admin_client = AdminClient::from_profile(AddressBook::cloud().frontend_url, &profile.1);
        let chrom_client = ChromaClient::with_admin_client(admin_client, String::from("docs"));
        
        let collection = chrom_client.get_collection(String::from("docs-content")).await.expect("Failed to get collection");
        
        let mut app = App::default();
        app.query_editor.operators = vec![Operator::Equal];
        app.query_editor.metadata_key = "page".to_string();
        app.query_editor.metadata_value = "add-data".to_string();
        
        let x = app.query_editor.parse_metadata();
        println!("{:?}", x);
        
        let records = collection.get(
            None,x.as_deref(), None, None, None, None
        ).await.map_err(|e| {
            println!("{}", e);
        });
        println!("{:#?}", records);
    }
}