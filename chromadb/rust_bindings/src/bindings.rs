use pyo3::{exceptions::PyOSError, pyclass, pymethods, PyResult};
use std::time::SystemTime;

#[pyclass]
pub(crate) struct Bindings {
    // TODO(sanketkedia, hammadb): Add ServerAPI handle here
    // server_api_handle: ComponentHandle<ServerAPI>,
    // runtime: tokio::runtime::Runtime,
}

//////////////////////// PyMethods Implementation ////////////////////////
#[pymethods]
impl Bindings {
    #[new]
    #[allow(dead_code)]
    pub fn py_new() -> Self {
        Bindings {}
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
}

//////////////////////// Normal Implementation ////////////////////////
impl Bindings {
    #[allow(dead_code)]
    pub fn new() -> Self {
        Bindings {}
    }
}
