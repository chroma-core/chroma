pub(crate) mod config;
#[allow(clippy::module_inception)]
pub(crate) mod sysdb;
pub(crate) mod test_sysdb;

use self::config::SysDbConfig;
use chroma_config::Configurable;
use chroma_error::ChromaError;

pub(crate) async fn from_config(
    config: &SysDbConfig,
) -> Result<Box<sysdb::SysDb>, Box<dyn ChromaError>> {
    match &config {
        crate::sysdb::config::SysDbConfig::Grpc(_) => Ok(Box::new(sysdb::SysDb::Grpc(
            sysdb::GrpcSysDb::try_from_config(config).await?,
        ))),
    }
}
