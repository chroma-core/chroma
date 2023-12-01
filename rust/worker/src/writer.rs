use crate::Component;
use pulsar::{Consumer, Pulsar, TokioExecutor};
use std::Collections::HashSet;

struct Writer {
    curr_topics: HashSet<String>,
    memberlist_provider: Rc<dyn MemberlistProvider>,
}

impl Writer {
    pub fn new(memberlist_provider: Rc<dyn MemberlistProvider>) -> Writer {
        /// TODO: cleanup and configure
        let pulsar: Pulsar<TokioExecutor> =
            Pulsar::builder("pulsar://localhost:6650", TokioExecutor)
                .build()
                .await
                .unwrap();

        return Writer {
            curr_topics: HashSet::new(),
            memberlist_provider: memberlist_provider,
        };
    }
}

impl Component for Writer {
    fn start(&self) {}

    fn stop(&self) {}
}

// A writer uses a memberlist_provider and a segment_provider to write to the index
// that is appropriate for the topic
