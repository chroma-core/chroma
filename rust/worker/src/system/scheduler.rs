use parking_lot::RwLock;
use std::fmt::Debug;
use std::sync::Arc;
use std::time::Duration;
use tokio::select;

use super::{Component, ComponentContext, Handler, Message};

#[derive(Debug)]
pub(crate) struct SchedulerTaskHandle {
    join_handle: Option<tokio::task::JoinHandle<()>>,
    cancel: tokio_util::sync::CancellationToken,
}

#[derive(Clone, Debug)]
pub(crate) struct Scheduler {
    handles: Arc<RwLock<Vec<SchedulerTaskHandle>>>,
}

impl Scheduler {
    pub(crate) fn new() -> Scheduler {
        Scheduler {
            handles: Arc::new(RwLock::new(Vec::new())),
        }
    }

    pub(crate) fn schedule<C, M>(&self, message: M, duration: Duration, ctx: &ComponentContext<C>)
    where
        C: Component + Handler<M>,
        M: Message,
    {
        let cancel = ctx.cancellation_token.clone();
        let sender = ctx.receiver().clone();
        let handle = tokio::spawn(async move {
            select! {
                _ = cancel.cancelled() => {
                    return;
                }
                _ = tokio::time::sleep(duration) => {
                    match sender.send(message, None).await {
                        Ok(_) => {
                            return;
                        },
                        Err(e) => {
                            // TODO: log error
                            println!("Error: {:?}", e);
                            return;
                        }
                    }
                }
            }
        });
        let handle = SchedulerTaskHandle {
            join_handle: Some(handle),
            cancel: ctx.cancellation_token.clone(),
        };
        self.handles.write().push(handle);
    }

    pub(crate) fn schedule_interval<C, M>(
        &self,
        message: M,
        duration: Duration,
        num_times: Option<usize>,
        ctx: &ComponentContext<C>,
    ) where
        C: Component + Handler<M>,
        M: Message + Clone,
    {
        let cancel = ctx.cancellation_token.clone();

        let sender = ctx.receiver().clone();

        let handle = tokio::spawn(async move {
            let mut counter = 0;
            while Self::should_continue(num_times, counter) {
                select! {
                    _ = cancel.cancelled() => {
                        return;
                    }
                    _ = tokio::time::sleep(duration) => {
                        match sender.send(message.clone(), None).await {
                            Ok(_) => {
                            },
                            Err(e) => {
                                // TODO: log error
                                println!("Error: {:?}", e);
                            }
                        }
                    }
                }
                counter += 1;
            }
        });
        let handle = SchedulerTaskHandle {
            join_handle: Some(handle),
            cancel: ctx.cancellation_token.clone(),
        };
        self.handles.write().push(handle);
    }

    fn should_continue(num_times: Option<usize>, counter: usize) -> bool {
        if num_times.is_some() {
            let num_times = num_times.unwrap();
            if counter >= num_times {
                return false;
            }
        }
        true
    }

    // Note: this method holds the lock on the handles, should call it only after stop is
    // called.
    pub(crate) async fn join(&self) {
        let mut handles = self.handles.write();
        for handle in handles.iter_mut() {
            if let Some(join_handle) = handle.join_handle.take() {
                match join_handle.await {
                    Ok(_) => {}
                    Err(e) => {
                        println!("Error: {:?}", e);
                    }
                }
            }
        }
    }

    pub(crate) fn stop(&self) {
        let handles = self.handles.read();
        for handle in handles.iter() {
            handle.cancel.cancel();
        }
    }
}

mod tests {
    use super::*;
    use crate::system::System;
    use async_trait::async_trait;
    use std::sync::Arc;
    use std::time::Duration;

    use std::sync::atomic::{AtomicUsize, Ordering};

    #[derive(Debug)]
    struct TestComponent {
        queue_size: usize,
        counter: Arc<AtomicUsize>,
    }

    #[derive(Clone, Debug)]
    struct ScheduleMessage {}

    impl TestComponent {
        fn new(queue_size: usize, counter: Arc<AtomicUsize>) -> Self {
            TestComponent {
                queue_size,
                counter,
            }
        }
    }
    #[async_trait]
    impl Handler<ScheduleMessage> for TestComponent {
        type Result = ();

        async fn handle(
            &mut self,
            _message: ScheduleMessage,
            _ctx: &ComponentContext<TestComponent>,
        ) {
            self.counter.fetch_add(1, Ordering::SeqCst);
        }
    }

    #[async_trait]
    impl Component for TestComponent {
        fn get_name() -> &'static str {
            "Test component"
        }

        fn queue_size(&self) -> usize {
            self.queue_size
        }

        async fn on_start(&mut self, ctx: &ComponentContext<TestComponent>) -> () {
            let duration = Duration::from_millis(100);
            ctx.scheduler.schedule(ScheduleMessage {}, duration, ctx);

            let num_times = 4;
            ctx.scheduler
                .schedule_interval(ScheduleMessage {}, duration, Some(num_times), ctx);
        }
    }

    #[tokio::test]
    async fn test_schedule() {
        let system = System::new();
        let counter = Arc::new(AtomicUsize::new(0));
        let component = TestComponent::new(10, counter.clone());
        let _handle = system.start_component(component);
        // yield to allow the component to process the messages
        tokio::task::yield_now().await;
        // We should have scheduled the message once
        system.join().await;
        assert_eq!(counter.load(Ordering::SeqCst), 5);
    }
}
