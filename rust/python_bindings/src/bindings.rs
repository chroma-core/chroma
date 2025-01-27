use pyo3::{exceptions::PyOSError, pyclass, pymethods, Py, PyAny, PyObject, PyResult, Python};
use std::time::SystemTime;

#[pyclass]
pub(crate) struct Bindings {
    // TODO(sanketkedia, hammadb): Add ServerAPI handle here
    // server_api_handle: ComponentHandle<ServerAPI>,
    // runtime: tokio::runtime::Runtime,
    // TODO(hammadb): In order to make CI green, we proxy all
    // calls back into python.
    // We should slowly start moving the logic from python to rust
    proxy_frontend: Py<PyAny>,
}

//////////////////////// PyMethods Implementation ////////////////////////
#[pymethods]
impl Bindings {
    #[new]
    #[allow(dead_code)]
    pub fn py_new(proxy_frontend: Py<PyAny>) -> Self {
        Bindings { proxy_frontend }
    }

    /// Returns the current eopch time in ns
    /// TODO(hammadb): This should proxy to ServerAPI
    #[allow(dead_code)]
    fn heartbeat(&self) -> PyResult<u128> {
        let duration_since_epoch =
            match std::time::SystemTime::now().duration_since(SystemTime::UNIX_EPOCH) {
                Ok(duration) => duration,
                Err(_) => return Err(PyOSError::new_err("Failed to get system time")),
            };
        Ok(duration_since_epoch.as_nanos())
    }

    // TODO(hammadb): Determine our pattern for optional arguments in python
    // options include using Option or passing defaults from python
    // or using pyargs annotations such as
    // #[pyargs(limit = "None", offset = "None")]

    ////////////////////////////// Admin API //////////////////////////////

    fn create_database(&self, name: String, tenant: String, py: Python<'_>) -> PyResult<PyObject> {
        self.proxy_frontend
            .call_method1(py, "create_database", (name, tenant))
    }

    fn get_database(&self, name: String, tenant: String, py: Python<'_>) -> PyResult<PyObject> {
        self.proxy_frontend
            .call_method1(py, "get_database", (name, tenant))
    }

    fn delete_database(&self, name: String, tenant: String, py: Python<'_>) -> PyResult<PyObject> {
        self.proxy_frontend
            .call_method1(py, "delete_database", (name, tenant))
    }

    #[pyo3(signature = (limit = None, offset = None, tenant = "DEFAULT_TENANT".to_string()))]
    fn list_databases(
        &self,
        limit: Option<i32>,
        offset: Option<i32>,
        tenant: String,
        py: Python<'_>,
    ) -> PyResult<PyObject> {
        self.proxy_frontend
            .call_method1(py, "list_databases", (limit, offset, tenant))
    }

    fn create_tenant(&self, name: String, py: Python<'_>) -> PyResult<PyObject> {
        self.proxy_frontend
            .call_method1(py, "create_tenant", (name,))
    }

    fn get_tenant(&self, name: String, py: Python<'_>) -> PyResult<PyObject> {
        self.proxy_frontend.call_method1(py, "get_tenant", (name,))
    }

    ////////////////////////////// Base API //////////////////////////////
    #[allow(clippy::too_many_arguments)]
    fn create_collection(
        &self,
        name: String,
        configuration: PyObject,
        metadata: PyObject,
        get_or_create: bool,
        tenant: String,
        database: String,
        py: Python<'_>,
    ) -> PyResult<PyObject> {
        self.proxy_frontend.call_method1(
            py,
            "create_collection",
            (
                name,
                configuration,
                metadata,
                get_or_create,
                tenant,
                database,
            ),
        )
    }
}
