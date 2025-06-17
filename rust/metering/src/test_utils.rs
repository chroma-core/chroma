use std::sync::{Arc, Mutex};

use chroma_system::{Component, ComponentContext, Handler};

use crate::MeterEvent;

/// This is a test component with a test receiver that is exported to make it easier for
/// other crates to test their metering logic
#[derive(Clone)]
pub struct MeteringTestComponent {
    pub messages: Arc<Mutex<Vec<String>>>,
}

impl std::fmt::Debug for MeteringTestComponent {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("MeteringTestComponent").finish()
    }
}

#[async_trait::async_trait]
impl Component for MeteringTestComponent {
    fn get_name() -> &'static str {
        "MeteringTestComponent"
    }

    fn queue_size(&self) -> usize {
        100
    }

    async fn on_start(&mut self, _: &ComponentContext<Self>) {}

    fn on_stop_timeout(&self) -> std::time::Duration {
        std::time::Duration::from_secs(1)
    }
}

#[async_trait::async_trait]
impl Handler<MeterEvent> for MeteringTestComponent {
    type Result = Option<()>;

    async fn handle(
        &mut self,
        message: MeterEvent,
        _context: &ComponentContext<Self>,
    ) -> Self::Result {
        self.messages.lock().unwrap().push(format!("{:?}", message));
        None
    }
}
