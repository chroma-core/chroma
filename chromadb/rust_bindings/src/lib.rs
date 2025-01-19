use async_trait::async_trait;
use chroma_sysdb::sqlite_sysdb::SqliteSysDb;
use chroma_system::{Component, ComponentContext, ComponentHandle, Handler, System};
use pyo3::{exceptions::PyOSError, prelude::*, types::PyType};
use std::time::SystemTime;

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
        // TODO: copy validation from python
        println!("Creating database: {} for tenant: {:?}", name, tenant);
        self.sysdb.create_database(id, name, tenant).await
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
        println!("Creating new runtime");
        let runtime = tokio::runtime::Runtime::new().unwrap();
        let _guard = runtime.enter();

        // TODO: user should share a directory
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
}

/// Returns the current eopch time in ns
#[pyfunction]
fn heartbeat() -> PyResult<u128> {
    let duration_since_epoch =
        match std::time::SystemTime::now().duration_since(SystemTime::UNIX_EPOCH) {
            Ok(duration) => duration,
            Err(_) => return Err(PyOSError::new_err("Failed to get system time")),
        };
    Ok(duration_since_epoch.as_nanos())
}

/// A Python module implemented in Rust.
/// TODO: reason about GIL
#[pymodule]
fn rust_bindings(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_function(wrap_pyfunction!(heartbeat, m)?)?;
    m.add_class::<Bindings>()?;
    Ok(())
}
