pub(crate) mod config;
pub(crate) mod sysdb;
pub(crate) mod test_sysdb;

use crate::{
    config::{Configurable, WorkerConfig},
    errors::ChromaError,
};

pub(crate) async fn from_config(
    config: &WorkerConfig,
) -> Result<Box<dyn sysdb::SysDb>, Box<dyn ChromaError>> {
    match &config.sysdb {
        crate::sysdb::config::SysDbConfig::Grpc(_) => {
            Ok(Box::new(sysdb::GrpcSysDb::try_from_config(config).await?))
        }
    }
}
