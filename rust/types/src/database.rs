use pyo3::pyclass;
use uuid::Uuid;

#[derive(Debug, PartialEq)]
#[pyclass]
pub struct Database {
    // TODO: implement IntoPyObject for Uuid
    pub id: Uuid,
    #[pyo3(get)]
    pub name: String,
    #[pyo3(get)]
    pub tenant: String,
}
