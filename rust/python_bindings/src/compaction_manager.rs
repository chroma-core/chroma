use std::fmt::{Debug, Formatter};
use std::time::Duration;

use async_trait::async_trait;
use chroma_log::Log;
use chroma_system::Handler;
use chroma_system::{Component, ComponentContext};

pub struct LocalCompactionManager {
    #[allow(dead_code)]
    log: Box<Log>,
    // TODO(Sanket): config
}

impl LocalCompactionManager {
    #[allow(dead_code)]
    pub fn new(log: Box<Log>) -> Self {
        LocalCompactionManager { log }
    }
}

#[async_trait]
impl Component for LocalCompactionManager {
    fn get_name() -> &'static str {
        "Local Compaction manager"
    }

    fn queue_size(&self) -> usize {
        // TODO(Sanket): Make this configurable.
        1000
    }

    async fn start(&mut self, ctx: &ComponentContext<Self>) -> () {
        // TODO(Sanket): Make the compaction interval configurable.
        // TODO(Sanket): Add span for orchestration.
        ctx.scheduler.schedule(
            ScheduledCompactionMessage {},
            Duration::from_secs(10),
            ctx,
            || None,
        );
    }
}

#[derive(Clone, Debug)]
pub struct ScheduledCompactionMessage {}

impl Debug for LocalCompactionManager {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("LocalCompactionManager").finish()
    }
}

// ============== Handlers ==============
#[async_trait]
impl Handler<ScheduledCompactionMessage> for LocalCompactionManager {
    type Result = ();

    async fn handle(
        &mut self,
        _message: ScheduledCompactionMessage,
        ctx: &ComponentContext<LocalCompactionManager>,
    ) {
        // TODO(Sanket): Implement compaction.

        // Compaction is done, schedule the next compaction
        ctx.scheduler.schedule(
            ScheduledCompactionMessage {},
            Duration::from_secs(10),
            ctx,
            || None,
        );
    }
}
