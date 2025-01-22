use async_trait::async_trait;
use chroma_sysdb::sqlite_sysdb::SqliteSysDb;
use chroma_system::{Component, ComponentContext, ComponentHandle, Handler, System};
use chroma_types::{
    Collection, CollectionUuid, Database, Metadata, Segment, SegmentScope, SegmentType,
    SegmentUuid, Tenant, UserIdentity,
};
use numpy::{PyReadonlyArray, PyReadonlyArray1, PyReadonlyArray2};
use pyo3::{exceptions::PyOSError, prelude::*, types::PyList};
use std::{collections::HashMap, time::SystemTime};

// TODO: Add modules to pyclass macro

/////////////////////////////// ServerAPI ///////////////////////////////

// TODO: move this to chroma/ as its not part of bindings and can be reused for the http server
// The ServerAPI implements ServerAPI in python
#[derive(Debug)]
struct ServerAPI {
    sysdb: SqliteSysDb,
}

impl ServerAPI {
    fn new(sysdb: SqliteSysDb) -> Self {
        ServerAPI { sysdb }
    }

    //////////////////////////// Database Methods ////////////////////////////

    async fn create_database(
        &self,
        id: uuid::Uuid,
        name: &str,
        tenant: Option<&str>,
    ) -> Result<(), String> {
        // TODO: copy validation from python (validation should be shoved into sysdb ideally)
        self.sysdb.create_database(id, name, tenant).await
    }

    async fn get_database(&self, name: &str, tenant: &str) -> Result<Database, String> {
        self.sysdb.get_database(name, Some(tenant)).await
    }

    //////////////////////////// Tenant Methods ////////////////////////////

    async fn create_tenant(&self, name: &str) -> Result<Tenant, String> {
        self.sysdb.create_tenant(name).await
    }

    async fn get_tenant(&self, name: &str) -> Result<Tenant, String> {
        self.sysdb.get_tenant(name).await
    }

    //////////////////////////// Collection Methods ////////////////////////////

    #[allow(clippy::too_many_arguments)]
    async fn create_collection(
        &self,
        name: &str,
        // TODO: collection config
        metadata: Option<Metadata>,
        get_or_create: bool,
        tenant: Option<&str>,
        database: Option<&str>,
    ) -> Result<Collection, String> {
        // NOTE(hammadb) This replicates the behavior of the python segment manager
        // to create the segment information data. We will need to abstract this to support
        // local vs distributed segment creation

        // TODO HACK collectionid could be optional, probably prepare a CreateSegment type
        let metadata_segment = Segment {
            id: SegmentUuid::new(),
            r#type: SegmentType::Sqlite,
            scope: SegmentScope::METADATA,
            collection: CollectionUuid::new(),
            metadata: metadata.clone(),
            file_path: HashMap::new(),
        };
        let vector_segment = Segment {
            id: SegmentUuid::new(),
            r#type: SegmentType::Sqlite,
            scope: SegmentScope::VECTOR,
            collection: CollectionUuid::new(),
            metadata: metadata.clone(),
            file_path: HashMap::new(),
        };
        let segments = vec![metadata_segment, vector_segment];

        let _ = Python::with_gil(|py| -> PyResult<()> {
            let ffi_cstr = std::ffi::CString::new("print('Hello, World!')").unwrap();
            let _ = py.eval(&ffi_cstr, None, None).unwrap();
            Ok(())
        });

        self.sysdb
            .create_collection(
                Some(CollectionUuid::new()),
                name,
                // TODO: implement segment prep
                segments,
                metadata.as_ref(),
                None,
                get_or_create,
                tenant,
                database,
            )
            .await
            .map(|(collection, _)| collection)
    }

    //////////////////////////// Record Methods ////////////////////////////

    //def _add(
    //     self,
    //     ids: IDs,
    //     collection_id: UUID,
    //     embeddings: Embeddings,
    //     metadatas: Optional[Metadatas] = None,
    //     documents: Optional[Documents] = None,
    //     uris: Optional[URIs] = None,
    //     tenant: str = DEFAULT_TENANT,
    //     database: str = DEFAULT_DATABASE,
    // ) -> bool:

    async fn add(
        &self,
        ids: Vec<String>,
        collection_id: CollectionUuid,
        embeddings: Vec<Vec<f32>>,
        metadatas: Option<Vec<Metadata>>,
        documents: Option<Vec<String>>,
        uris: Option<Vec<String>>,
        tenant: Option<&str>,
        database: Option<&str>,
    ) -> Result<bool, String> {
        unimplemented!();
    }

    fn get_user_identity(&self) -> UserIdentity {
        UserIdentity::default()
    }
}

impl Component for ServerAPI {
    fn get_name() -> &'static str {
        "ServerAPI"
    }

    fn queue_size(&self) -> usize {
        1000
    }
}

////////////////////////// ServerAPI Handlers ////////////////////////////

#[derive(Debug)]
struct CreateDatabaseMessage {
    id: uuid::Uuid,
    name: String,
    tenant: Option<String>,
}
#[async_trait]
impl Handler<CreateDatabaseMessage> for ServerAPI {
    type Result = Result<(), String>;
    async fn handle(
        &mut self,
        message: CreateDatabaseMessage,
        _ctx: &ComponentContext<Self>,
    ) -> Self::Result {
        self.create_database(message.id, &message.name, message.tenant.as_deref())
            .await
    }
}

#[derive(Debug)]
struct GetDatabaseMessage {
    name: String,
    tenant: String,
}
#[async_trait]
impl Handler<GetDatabaseMessage> for ServerAPI {
    type Result = Result<Database, String>;
    async fn handle(
        &mut self,
        message: GetDatabaseMessage,
        _ctx: &ComponentContext<Self>,
    ) -> Self::Result {
        self.get_database(&message.name, &message.tenant).await
    }
}

#[derive(Debug)]
struct CreateTenantMessage {
    name: String,
}

#[async_trait]
impl Handler<CreateTenantMessage> for ServerAPI {
    type Result = Result<Tenant, String>;
    async fn handle(
        &mut self,
        message: CreateTenantMessage,
        _ctx: &ComponentContext<Self>,
    ) -> Self::Result {
        self.create_tenant(&message.name).await
    }
}

#[derive(Debug)]
struct GetTenantMessage {
    name: String,
}
#[async_trait]
impl Handler<GetTenantMessage> for ServerAPI {
    type Result = Result<Tenant, String>;
    async fn handle(
        &mut self,
        _message: GetTenantMessage,
        _ctx: &ComponentContext<Self>,
    ) -> Self::Result {
        self.get_tenant(&_message.name).await
    }
}

#[derive(Debug)]
struct GetUserIdentityMessage;
#[async_trait]
impl Handler<GetUserIdentityMessage> for ServerAPI {
    type Result = UserIdentity;
    async fn handle(
        &mut self,
        _message: GetUserIdentityMessage,
        _ctx: &ComponentContext<Self>,
    ) -> Self::Result {
        self.get_user_identity()
    }
}

#[derive(Debug)]
struct CreateCollectionMessage {
    name: String,
    metadata: Option<Metadata>,
    get_or_create: bool,
    tenant: Option<String>,
    database: Option<String>,
}
#[async_trait]
impl Handler<CreateCollectionMessage> for ServerAPI {
    type Result = Result<Collection, String>;
    async fn handle(
        &mut self,
        message: CreateCollectionMessage,
        _ctx: &ComponentContext<Self>,
    ) -> Self::Result {
        self.create_collection(
            &message.name,
            message.metadata,
            message.get_or_create,
            message.tenant.as_deref(),
            message.database.as_deref(),
        )
        .await
    }
}

///////////////////////////// Python Bindings ///////////////////////////////

#[pyclass]
struct Bindings {
    server_api_handle: ComponentHandle<ServerAPI>,
    // A tokio runtime is required to run async functions
    // we block the python thread on the response of the async function
    // we can free the GIL while waiting for the async function to complete
    // then we can reacquire the GIL to return the result
    runtime: tokio::runtime::Runtime,
}

#[pymethods]
impl Bindings {
    // TODO: use PyResult
    #[new]
    fn new() -> Self {
        // TODO: runtime config
        let runtime = tokio::runtime::Runtime::new().unwrap();
        let _guard = runtime.enter();

        // TODO: user should provide a directory not a path
        // TODO: do we create dir if not exists?
        // TODO: bootstrapping client to create this
        let hack_path = "/Users/hammad/Documents/chroma/chroma/chroma_rust_bindings.sqlite3";
        let sysdb = runtime.block_on(SqliteSysDb::new_hack_test(hack_path));
        let server_api = ServerAPI::new(sysdb);
        let system = System::new();
        let server_api_handle = system.start_component(server_api);
        Bindings {
            server_api_handle,
            runtime,
        }
    }

    /// Returns the current eopch time in ns
    fn heartbeat(&self) -> PyResult<u128> {
        let duration_since_epoch =
            match std::time::SystemTime::now().duration_since(SystemTime::UNIX_EPOCH) {
                Ok(duration) => duration,
                Err(_) => return Err(PyOSError::new_err("Failed to get system time")),
            };
        Ok(duration_since_epoch.as_nanos())
    }

    fn create_database(&self, id: String, name: String, tenant: String) -> PyResult<()> {
        let id = uuid::Uuid::parse_str(&id).map_err(|e| PyOSError::new_err(e.to_string()))?;
        let message = CreateDatabaseMessage {
            id,
            name,
            tenant: Some(tenant),
        };
        let result = self
            .runtime
            .block_on(self.server_api_handle.request(message, None));

        // TODO: error handling
        match result {
            Ok(inner) => match inner {
                Ok(_) => Ok(()),
                Err(e) => Err(PyOSError::new_err(e)),
            },
            Err(e) => Err(PyOSError::new_err(e.to_string())),
        }
    }

    fn get_database(&self, name: String, tenant: String) -> PyResult<Database> {
        let result = self.runtime.block_on(
            self.server_api_handle
                .request(GetDatabaseMessage { name, tenant }, None),
        );

        // TODO: error handling
        match result {
            Ok(database) => match database {
                Ok(database) => Ok(database),
                Err(e) => Err(PyOSError::new_err(e)),
            },
            Err(e) => Err(PyOSError::new_err(e.to_string())),
        }
    }

    fn create_tenant(&self, name: String) -> PyResult<Tenant> {
        let result = self.runtime.block_on(
            self.server_api_handle
                .request(CreateTenantMessage { name }, None),
        );

        // TODO: error handling
        match result {
            Ok(tenant) => match tenant {
                Ok(tenant) => Ok(tenant),
                Err(e) => Err(PyOSError::new_err(e)),
            },
            Err(e) => Err(PyOSError::new_err(e.to_string())),
        }
    }

    fn get_tenant(&self, name: String) -> PyResult<Tenant> {
        let result = self.runtime.block_on(
            self.server_api_handle
                .request(GetTenantMessage { name }, None),
        );
        match result {
            Ok(tenant) => match tenant {
                Ok(tenant) => Ok(tenant),
                Err(e) => Err(PyOSError::new_err(e)),
            },
            Err(e) => Err(PyOSError::new_err(e.to_string())),
        }
    }

    #[pyo3(
        signature = (name, metadata = None, get_or_create = false, tenant = "default_tenant".to_string(), database = "default_database".to_string())
    )]
    #[allow(clippy::too_many_arguments)]
    fn create_collection(
        &self,
        name: String,
        metadata: Option<Metadata>,
        get_or_create: bool,
        tenant: String,
        database: String,
        py: Python<'_>,
    ) -> PyResult<Collection> {
        let result = py.allow_threads(|| {
            let message = CreateCollectionMessage {
                name,
                metadata,
                get_or_create,
                tenant: Some(tenant),
                database: Some(database),
            };
            let result = self
                .runtime
                .block_on(self.server_api_handle.request(message, None));
            result
        });

        // let message = CreateCollectionMessage {
        //     name,
        //     metadata,
        //     get_or_create,
        //     // TODO: maybe get rid of the optionality downstream and push defaults up to application layer
        //     tenant: Some(tenant),
        //     database: Some(database),
        // };
        // let result = self
        //     .runtime
        //     .block_on(self.server_api_handle.request(message, None));

        // // TODO: error handling
        match result {
            Ok(collection) => match collection {
                Ok(collection) => Ok(collection),
                Err(e) => Err(PyOSError::new_err(e)),
            },
            Err(e) => Err(PyOSError::new_err(e.to_string())),
        }
    }

    #[pyo3(
        signature = (ids, collection_id, embeddings, metadatas = None, documents = None, uris = None, tenant = "default_tenant".to_string(), database = "default_database".to_string())
    )]
    #[allow(clippy::too_many_arguments)]
    fn add(
        &self,
        ids: Vec<String>,
        collection_id: String,
        embeddings: Vec<PyReadonlyArray1<f32>>,
        metadatas: Option<Vec<Metadata>>,
        documents: Option<Vec<String>>,
        uris: Option<Vec<String>>,
        tenant: String,
        database: String,
    ) -> PyResult<bool> {
        println!("embeddings: {:?}", embeddings);
        for embedding in embeddings {
            let e_minor = embedding.as_slice().unwrap();
            let as_vec = e_minor.to_vec();
        }
        // let collection_id =
        //     uuid::Uuid::parse_str(&collection_id).map_err(|e| PyOSError::new_err(e.to_string()))?;
        // let collection_id = CollectionUuid(collection_id);
        // let result = self.runtime.block_on(self.server_api_handle.request(
        //     AddMessage {
        //         ids,
        //         collection_id,
        //         embeddings,
        //         metadatas,
        //         documents,
        //         uris,
        //         tenant,
        //         database,
        //     },
        //     None,
        // ));
        // match result {
        //     Ok(success) => Ok(success),
        //     Err(e) => Err(PyOSError::new_err(e.to_string())),
        // }
        return Ok(true);
    }

    fn get_user_identity(&self) -> PyResult<UserIdentity> {
        let result = self
            .runtime
            .block_on(self.server_api_handle.request(GetUserIdentityMessage, None));
        match result {
            Ok(user_identity) => Ok(user_identity),
            Err(e) => Err(PyOSError::new_err(e.to_string())),
        }
    }
}

/// A Python module implemented in Rust.
/// TODO: reason about GIL
#[pymodule]
fn rust_bindings(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_class::<Bindings>()?;
    Ok(())
}
