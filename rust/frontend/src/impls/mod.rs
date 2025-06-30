pub mod in_memory_frontend;
pub mod service_based_frontend;
mod utils;

use service_based_frontend::ServiceBasedFrontend;

pub type Frontend = ServiceBasedFrontend;
