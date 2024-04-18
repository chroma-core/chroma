pub(crate) mod config;
pub(crate) mod sysdb;
pub(crate) mod test_sysdb;

use self::config::SysDbConfig;
use crate::config::Configurable;
use crate::errors::ChromaError;

pub(crate) async fn from_config(
    config: &SysDbConfig,
) -> Result<Box<dyn sysdb::SysDb>, Box<dyn ChromaError>> {
    match &config {
        crate::sysdb::config::SysDbConfig::Grpc(_) => {
            Ok(Box::new(sysdb::GrpcSysDb::try_from_config(config).await?))
        }
    }
}
