use parking_lot::RwLock;
use std::sync::atomic::AtomicU64;
use std::sync::{Arc, Weak};
use std::time::Duration;
use std::{collections::HashMap, fmt::Debug};
use tokio::select;
use tracing::Span;

use super::{Component, ComponentContext, Handler, Message};

pub(crate) struct SchedulerTaskHandle {
    join_handle: Option<tokio::task::JoinHandle<()>>,
    cancel: tokio_util::sync::CancellationToken,
}

impl Debug for SchedulerTaskHandle {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SchedulerTaskHandle").finish()
    }
}

#[derive(Copy, Clone, Eq, PartialEq, Hash, Debug)]
pub(crate) struct TaskId(u64);

pub(crate) struct HandleGuard {
    weak_handles: Weak<RwLock<HashMap<TaskId, SchedulerTaskHandle>>>,
    task_id: TaskId,
}

impl Drop for HandleGuard {
    fn drop(&mut self) {
        if let Some(handles) = self.weak_handles.upgrade() {
            let mut handles = handles.write();
            handles.remove(&self.task_id);
        }
    }
}

#[derive(Clone, Debug)]
pub struct Scheduler {
    handles: Arc<RwLock<HashMap<TaskId, SchedulerTaskHandle>>>,
    next_id: Arc<AtomicU64>,
}

impl Scheduler {
    pub(crate) fn new() -> Scheduler {
        Scheduler {
            handles: Arc::new(RwLock::new(HashMap::new())),
            next_id: Arc::new(AtomicU64::new(1)),
        }
    }

    /// Allocate the next task ID.
    fn allocate_id(&self) -> TaskId {
        let id = self
            .next_id
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        TaskId(id)
    }

    /// Schedule a message to be sent to the component after the specified duration.
    ///
    /// `span_factory` is called immediately before sending the scheduled message to the component.
    pub fn schedule<C, M, S>(
        &self,
        message: M,
        duration: Duration,
        ctx: &ComponentContext<C>,
        // (This needs to be a factory, otherwise the span duration will include the time spent waiting for the scheduler to trigger).
        span_factory: S,
    ) where
        C: Component + Handler<M>,
        M: Message,
        S: (Fn() -> Option<Span>) + Send + Sync + 'static,
    {
        let id = self.allocate_id();
        let handles_weak = Arc::downgrade(&self.handles);

        let cancel = ctx.cancellation_token.clone();
        let sender = ctx.receiver().clone();
        let handle = tokio::spawn(async move {
            let _guard = HandleGuard {
                weak_handles: handles_weak,
                task_id: id,
            };

            select! {
                _ = cancel.cancelled() => {}
                _ = tokio::time::sleep(duration) => {
                    let span = span_factory();
                    match sender.send(message, span).await {
                        Ok(_) => {
                        },
                        Err(e) => {
                            tracing::error!("Error: {:?}", e);
                        }
                    }
                }
            }
        });
        let handle = SchedulerTaskHandle {
            join_handle: Some(handle),
            cancel: ctx.cancellation_token.clone(),
        };
        self.handles.write().insert(id, handle);
    }

    /// Schedule a message to be sent to the component at a regular interval.
    ///
    /// `span_factory` is called immediately before sending the scheduled message to the component.
    #[cfg(test)]
    pub(crate) fn schedule_interval<C, M, S>(
        &self,
        message: M,
        duration: Duration,
        num_times: Option<usize>,
        ctx: &ComponentContext<C>,
        span_factory: S,
    ) where
        C: Component + Handler<M>,
        M: Message + Clone,
        S: (Fn() -> Option<Span>) + Send + Sync + 'static,
    {
        let id = self.allocate_id();
        let handles_weak = Arc::downgrade(&self.handles);
        let cancel = ctx.cancellation_token.clone();
        let sender = ctx.receiver().clone();

        let handle = tokio::spawn(async move {
            let _guard = HandleGuard {
                weak_handles: handles_weak,
                task_id: id,
            };
            let mut counter = 0;
            while Self::should_continue(num_times, counter) {
                select! {
                    _ = cancel.cancelled() => {
                        return;
                    }
                    _ = tokio::time::sleep(duration) => {
                        let span = span_factory();
                        match sender.send(message.clone(), span).await {
                            Ok(_) => {
                            },
                            Err(e) => {
                                tracing::error!("Error: {:?}", e);
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
        self.handles.write().insert(id, handle);
    }

    #[cfg(test)]
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
        let mut handles = {
            // NOTE(rescrv):  We take the handles in a block so the lock is released prior to
            // awaiting the handles.
            let mut handles = self.handles.write();
            handles
                .iter_mut()
                .flat_map(|(_, h)| h.join_handle.take())
                .collect::<Vec<_>>()
        };
        for join_handle in handles.iter_mut() {
            match join_handle.await {
                Ok(_) => {}
                Err(e) => {
                    tracing::error!("Error joining scheduler task: {:?}", e);
                }
            }
        }
    }

    pub(crate) fn stop(&self) {
        let handles = self.handles.read();
        for handle in handles.iter() {
            handle.1.cancel.cancel();
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::system::System;
    use async_trait::async_trait;
    use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
    use std::sync::Arc;
    use std::time::Duration;

    #[derive(Debug)]
    struct SimpleScheduleIntervalComponent {
        queue_size: usize,
        counter: Arc<AtomicUsize>,
    }

    #[derive(Clone, Debug)]
    struct ScheduleMessage {}

    impl SimpleScheduleIntervalComponent {
        fn new(queue_size: usize, counter: Arc<AtomicUsize>) -> Self {
            SimpleScheduleIntervalComponent {
                queue_size,
                counter,
            }
        }
    }
    #[async_trait]
    impl Handler<ScheduleMessage> for SimpleScheduleIntervalComponent {
        type Result = ();

        async fn handle(
            &mut self,
            _message: ScheduleMessage,
            _ctx: &ComponentContext<SimpleScheduleIntervalComponent>,
        ) {
            self.counter.fetch_add(1, Ordering::SeqCst);
        }
    }

    #[async_trait]
    impl Component for SimpleScheduleIntervalComponent {
        fn get_name() -> &'static str {
            "Test component"
        }

        fn queue_size(&self) -> usize {
            self.queue_size
        }

        async fn on_start(
            &mut self,
            ctx: &ComponentContext<SimpleScheduleIntervalComponent>,
        ) -> () {
            let duration = Duration::from_millis(100);
            ctx.scheduler
                .schedule(ScheduleMessage {}, duration, ctx, || None);

            let num_times = 4;
            ctx.scheduler.schedule_interval(
                ScheduleMessage {},
                duration,
                Some(num_times),
                ctx,
                || None,
            );
        }
    }

    #[tokio::test]
    async fn test_schedule() {
        let system = System::new();
        let counter = Arc::new(AtomicUsize::new(0));
        let component = SimpleScheduleIntervalComponent::new(10, counter.clone());
        let _handle = system.start_component(component);
        // yield to allow the component to process the messages
        tokio::task::yield_now().await;
        // We should have scheduled the message once
        system.join().await;
        assert_eq!(counter.load(Ordering::SeqCst), 5);
    }

    #[derive(Debug)]
    struct OneMessageComponent {
        queue_size: usize,
        counter: Arc<AtomicUsize>,
        handles_empty_after: Arc<AtomicBool>,
    }

    impl OneMessageComponent {
        fn new(
            queue_size: usize,
            counter: Arc<AtomicUsize>,
            handles_empty_after: Arc<AtomicBool>,
        ) -> Self {
            OneMessageComponent {
                queue_size,
                counter,
                handles_empty_after,
            }
        }
    }

    #[async_trait]
    impl Component for OneMessageComponent {
        fn get_name() -> &'static str {
            "OneMessageComponent"
        }

        fn queue_size(&self) -> usize {
            self.queue_size
        }

        async fn on_start(&mut self, ctx: &ComponentContext<OneMessageComponent>) -> () {
            let duration = Duration::from_millis(100);
            ctx.scheduler
                .schedule(ScheduleMessage {}, duration, ctx, || None);
        }
    }

    #[async_trait]
    impl Handler<ScheduleMessage> for OneMessageComponent {
        type Result = ();

        async fn handle(
            &mut self,
            _message: ScheduleMessage,
            ctx: &ComponentContext<OneMessageComponent>,
        ) {
            self.counter.fetch_add(1, Ordering::SeqCst);
            self.handles_empty_after
                .store(ctx.scheduler.handles.read().is_empty(), Ordering::SeqCst);
        }
    }

    #[tokio::test]
    async fn test_handle_cleaned_up() {
        let system = System::new();
        let counter = Arc::new(AtomicUsize::new(0));
        let handles_empty_after = Arc::new(AtomicBool::new(false));
        let component = OneMessageComponent::new(10, counter.clone(), handles_empty_after.clone());
        let _handle = system.start_component(component);
        // Wait for the 100ms schedule to trigger
        tokio::time::sleep(Duration::from_millis(500)).await;
        // yield to allow the component to process the messages
        tokio::task::yield_now().await;
        assert!(handles_empty_after.load(Ordering::SeqCst));
        // We should have scheduled the message once
        system.join().await;
    }
}
