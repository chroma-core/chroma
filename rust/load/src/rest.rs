use uuid::Uuid;

use crate::{Connection, Throughput, Workload, WorkloadSummary};

/// A description of a data set.
#[derive(Clone, Debug, serde::Deserialize, serde::Serialize)]
pub struct Description {
    /// The name of the data set.
    pub name: String,
    /// The description of the data set.
    pub description: String,
    /// The JSON representation of the data set.
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

/// The status of the chroma-load service.
#[derive(Clone, Debug, serde::Deserialize, serde::Serialize)]
pub struct Status {
    /// Whether the service is inhibited.
    pub inhibited: bool,
    /// The workloads that are currently running.
    pub running: Vec<WorkloadSummary>,
    /// The data sets that are available.
    pub data_sets: Vec<Description>,
    /// The workloads that are available.
    pub workloads: Vec<Workload>,
}

/// A request to start a workload.
#[derive(Clone, Debug, serde::Deserialize, serde::Serialize)]
pub struct StartRequest {
    /// The name of the workload.  This is used to identify the workload in the status command.
    pub name: String,
    /// The workload to run.
    pub workload: Workload,
    /// The data set to use, referred to by name.
    pub data_set: Option<String>,
    /// The custom data set to use.
    pub custom_data_set: Option<serde_json::Value>,
    /// The connection to use.
    pub connection: Connection,
    /// When the workload should expire.
    pub expires: String,
    /// The throughput to use.
    pub throughput: Throughput,
}

/// A request to stop a workload.
#[derive(Clone, Debug, serde::Deserialize, serde::Serialize)]
pub struct StopRequest {
    /// The UUID of the workload to stop.
    pub uuid: Uuid,
}
