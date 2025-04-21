use crate::client::chroma_client::ChromaClient;
use crate::client::prelude::CollectionModel;
use crate::client::utils::send_request;
use chroma_frontend::server::GetRequestPayload;
use chroma_types::{CountResponse, GetResponse, IncludeList, RawWhereFields};
use reqwest::Method;
use std::error::Error;
use std::ops::Deref;
use thiserror::Error;

#[derive(Debug, Error)]
pub enum CollectionAPIError {
    #[error("Failed to count records from collection {0}")]
    Count(String),
    #[error("Failed to get records from collection {0}")]
    Get(String),
}

#[derive(Debug, Clone)]
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
        let where_fields = RawWhereFields::from_json_str(r#where, where_document)?;
        
        let payload = GetRequestPayload::new(
            ids,
            where_fields,
            limit,
            offset,
            include.unwrap_or(IncludeList::default_get()),
        );
        
        let response = send_request::<GetRequestPayload, GetResponse>(
            &self.chroma_client.host,
            Method::POST,
            &route,
            self.chroma_client.headers()?,
            Some(&payload),
        )
        .await
        .map_err(|e| {
            println!("{}", e);
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
    use serde_json::Value;
    use chroma_types::{IncludeList, WhereValidationError};
    use crate::client::admin_client::AdminClient;
    use crate::client::chroma_client::ChromaClient;
    use crate::utils::{get_current_profile, AddressBook};

    #[tokio::test]
    async fn test_get() {
        let profile = get_current_profile().expect("Failed to get current profile");
        let admin_client = AdminClient::from_profile(AddressBook::cloud().frontend_url, &profile.1);
        let db = admin_client.get_database(String::from("chroma-game")).await.expect("Failed to get database");
        let chrom_client = ChromaClient::with_admin_client(admin_client, db.name);
        
        let collection = chrom_client.get_collection(String::from("conversations")).await.expect("Failed to get collection");
        let records = collection.get(
            Some(vec!["11d6a76c-2dcb-4e24-8fe2-3aed0160bb70".to_string()]), Some("{\"friends\": {\"$eq\": \"Ava, Leo\"}}"), None, None, None, None
        ).await.map_err(|e| {
            println!("{}", e);
        });
        println!("{:#?}", records);
        
    }
}