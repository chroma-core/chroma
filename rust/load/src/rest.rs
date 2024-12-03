use uuid::Uuid;

use crate::{Workload, WorkloadSummary};

#[derive(Clone, Debug, serde::Deserialize, serde::Serialize)]
pub struct Description {
    pub name: String,
    pub description: String,
    pub json: serde_json::Value,
}

impl From<&dyn crate::DataSet> for Description {
    fn from(data_set: &dyn crate::DataSet) -> Self {
        Self {
            name: data_set.name(),
            description: data_set.description(),
            json: data_set.json(),
        }
    }
}

#[derive(Clone, Debug, serde::Deserialize, serde::Serialize)]
pub struct Status {
    pub running: Vec<WorkloadSummary>,
    pub data_sets: Vec<Description>,
    pub workloads: Vec<Description>,
}

#[derive(Clone, Debug, serde::Deserialize, serde::Serialize)]
pub struct StartRequest {
    pub name: String,
    pub workload: Workload,
    pub data_set: String,
    pub expires: String,
    pub throughput: f64,
}

#[derive(Clone, Debug, serde::Deserialize, serde::Serialize)]
pub struct StopRequest {
    pub uuid: Uuid,
}
