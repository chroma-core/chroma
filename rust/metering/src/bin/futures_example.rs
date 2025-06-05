use std::sync::{Arc, Mutex as StdMutex};
use tokio::time::{sleep, Duration};
use tracing::Span;

use async_trait::async_trait;
use std::fmt::Debug;

chroma_metering::initialize_metering! {
    #[attribute(name = "nested_attribute")]
    type NestedAttribute = Option<u8>;

    #[event]
    #[derive(Debug, Default, Clone)]
    struct ParentEvent {
        parent_value: usize,
        #[field(attribute = "nested_attribute", mutator = "parent_mutator")]
        parent_field: Option<u8>,
    }

    #[event]
    #[derive(Debug, Default, Clone)]
    struct ChildEvent {
        child_value: usize,
        #[field(attribute = "nested_attribute", mutator = "child_mutator")]
        child_field: Option<u8>,
    }
}

fn parent_mutator(evt: &mut ParentEvent, value: Option<u8>) {
    evt.parent_field = value;
}

fn child_mutator(evt: &mut ChildEvent, value: Option<u8>) {
    evt.child_field = value;
}

#[derive(Clone, Debug)]
struct CollectingReceiver {
    seen: Arc<StdMutex<Vec<String>>>,
}

#[async_trait]
impl chroma_system::ReceiverForMessage<Box<dyn MeteringEvent>> for CollectingReceiver {
    async fn send(
        &self,
        message: Box<dyn MeteringEvent>,
        _tracing_context: Option<Span>,
    ) -> Result<(), chroma_system::ChannelError> {
        let mut guard = self.seen.lock().unwrap();

        guard.push(format!("{:?}", message));
        Ok(())
    }
}

async fn parent_scope(_collector: CollectingReceiver) {
    let _parent_guard = create(ParentEvent {
        parent_value: 10,
        parent_field: None,
    });

    current().nested_attribute(Some(5));
    println!("[parent_scope] after mutator: {:?}", current());

    if let Some(event) = close::<ParentEvent>() {
        event.submit().await;
    }

    let child_handle = tokio::spawn(async move {
        let _child_guard = create(ChildEvent {
            child_value: 20,
            child_field: None,
        });

        current().nested_attribute(Some(15));
        println!("[child task] after child mutator: {:?}", current());

        if let Some(event) = close::<ChildEvent>() {
            event.submit().await;
        }
    });

    let _ = child_handle.await;

    println!("[parent_scope] back to parent, current = {:?}", current());
}

async fn isolated_tasks_demo(_collector: CollectingReceiver) {
    let a = tokio::spawn(async move {
        let _guard_a = create(ParentEvent {
            parent_value: 100,
            parent_field: None,
        });
        current().nested_attribute(Some(50));
        println!("[task A] current() = {:?}", current());

        if let Some(event) = close::<ParentEvent>() {
            event.submit().await;
        }
    });

    let b = tokio::spawn(async move {
        sleep(Duration::from_millis(5)).await;
        let _guard_b = create(ChildEvent {
            child_value: 200,
            child_field: None,
        });
        current().nested_attribute(Some(75));
        println!("[task B] current() = {:?}", current());

        if let Some(event) = close::<ChildEvent>() {
            event.submit().await;
        }
    });

    let _ = tokio::join!(a, b);
}

async fn some_work(x: usize) -> usize {
    sleep(Duration::from_millis(10)).await;
    x * 2
}

async fn instrumentation_demo() {
    let _guard = create(ParentEvent {
        parent_value: 999,
        parent_field: None,
    });

    current().nested_attribute(Some(123));
    println!("[instrumentation_demo] before async work: {:?}", current());

    let result = some_work(42).metered(_guard).await;
    println!("[instrumentation_demo] result = {}", result);

    println!(
        "[instrumentation_demo] after async work, current = {:?}",
        current()
    );
}

#[tokio::main]
async fn main() {
    let collector = CollectingReceiver {
        seen: Arc::new(StdMutex::new(Vec::new())),
    };
    register_receiver(Box::new(collector.clone()));

    println!("=== Phase 1: Testing nested parent/child scopes ===");
    parent_scope(collector.clone()).await;

    sleep(Duration::from_millis(50)).await;

    println!("\n=== Phase 2: Testing isolated tasks ===");
    isolated_tasks_demo(collector.clone()).await;

    sleep(Duration::from_millis(50)).await;

    println!("\n=== Phase 3: Instrumentation demo ===");
    instrumentation_demo().await;

    sleep(Duration::from_millis(50)).await;

    let seen = collector.seen.lock().unwrap();
    println!("\n--- Collector saw these submissions (in order) ---");
    for entry in seen.iter() {
        println!("{}", entry);
    }
}
