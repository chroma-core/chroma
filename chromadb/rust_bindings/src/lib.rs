use async_trait::async_trait;
use chroma_sysdb::sqlite_sysdb::SqliteSysDb;
use chroma_system::{Component, ComponentContext, ComponentHandle, Handler, System};
use chroma_types::{Database, Tenant, UserIdentity};
use pyo3::{exceptions::PyOSError, prelude::*, types::PyType};
use std::time::SystemTime;

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

    async fn create_tenant(&self, name: &str) -> Result<Tenant, String> {
        self.sysdb.create_tenant(name).await
    }

    async fn get_tenant(&self, name: &str) -> Result<Tenant, String> {
        self.sysdb.get_tenant(name).await
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
