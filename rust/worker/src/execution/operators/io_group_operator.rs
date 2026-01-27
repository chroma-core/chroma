use chroma_error::ChromaError;
use chroma_system::{Operator, OperatorType, TaskWrapper};
use futures::{stream::FuturesUnordered, StreamExt};
use parking_lot::Mutex;
use std::sync::Arc;
use thiserror::Error;
use tonic::async_trait;
use tracing::{Instrument, Span};

/// An operator that groups multiple IO-bound subtasks and runs them concurrently.
///
/// This operator is designed to improve IO scheduling by batching multiple IO operations
/// together. Each subtask sends its results directly to its own receiver channel,
/// so this operator doesn't collect or return any subtask results.
///
/// # Example Use Case
/// When fetching multiple posting lists in SPANN search, instead of dispatching each
/// fetch task individually, we can group them all in an IoGroupOperator to execute
/// them together with better IO scheduling.
#[derive(Debug, Default)]
pub struct IoGroupOperator {}

impl IoGroupOperator {
    pub fn new() -> Self {
        Self::default()
    }
}

/// Input for the IoGroupOperator containing a list of subtasks to execute.
///
/// The subtasks are wrapped in an `Arc<Mutex<Option<...>>>` to allow ownership
/// transfer to the operator while maintaining Send safety.
#[derive(Debug)]
pub struct IoGroupOperatorInput {
    #[allow(clippy::type_complexity)]
    sub_tasks: Arc<Mutex<Option<Vec<Box<dyn TaskWrapper>>>>>,
}

impl IoGroupOperatorInput {
    #[allow(clippy::type_complexity)]
    pub fn new(sub_tasks: Arc<Mutex<Option<Vec<Box<dyn TaskWrapper>>>>>) -> Self {
        Self { sub_tasks }
    }
}

/// Output from the IoGroupOperator. Empty because subtask results are sent
/// directly to their own receiver channels.
#[derive(Debug)]
pub struct IoGroupOperatorOutput {}

#[derive(Debug, Error)]
pub enum IoGroupOperatorError {}

impl ChromaError for IoGroupOperatorError {
    fn code(&self) -> chroma_error::ErrorCodes {
        chroma_error::ErrorCodes::Internal
    }
}

#[async_trait]
impl Operator<IoGroupOperatorInput, IoGroupOperatorOutput> for IoGroupOperator {
    type Error = IoGroupOperatorError;

    async fn run(
        &self,
        input: &IoGroupOperatorInput,
    ) -> Result<IoGroupOperatorOutput, IoGroupOperatorError> {
        let mut subtasks = input.sub_tasks.lock().take().unwrap();
        let mut futures = FuturesUnordered::new();

        for task in subtasks.iter_mut() {
            let fut = task.run().instrument(tracing::info_span!(
                parent: Span::current(),
                "IO group subtask",
            ));
            futures.push(fut);
        }

        while let Some(_result) = futures.next().await {
            // No-op since subtask results are sent directly to their receivers
        }

        Ok(IoGroupOperatorOutput {})
    }

    fn get_type(&self) -> OperatorType {
        OperatorType::IO
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use async_trait::async_trait;
    use chroma_error::ErrorCodes;
    use chroma_system::{wrap, Component, ComponentContext, Dispatcher, Handler, System, TaskResult};
    use std::sync::atomic::{AtomicUsize, Ordering};
    use tokio_util::sync::CancellationToken;

    // Test operator that increments a counter when run
    #[derive(Debug)]
    struct TestIncrementOperator {
        counter: Arc<AtomicUsize>,
    }

    #[derive(Debug)]
    struct TestInput;

    #[derive(Debug)]
    struct TestOutput;

    #[derive(Debug, Error)]
    #[error("test error")]
    struct TestError;

    impl ChromaError for TestError {
        fn code(&self) -> ErrorCodes {
            ErrorCodes::Internal
        }
    }

    #[async_trait]
    impl Operator<TestInput, TestOutput> for TestIncrementOperator {
        type Error = TestError;

        async fn run(&self, _input: &TestInput) -> Result<TestOutput, TestError> {
            self.counter.fetch_add(1, Ordering::SeqCst);
            // Add a small delay to simulate IO
            tokio::time::sleep(tokio::time::Duration::from_millis(10)).await;
            Ok(TestOutput)
        }
    }

    // Test component that receives results
    #[derive(Debug)]
    struct TestComponent {
        received_count: Arc<AtomicUsize>,
    }

    #[async_trait]
    impl Component for TestComponent {
        fn get_name() -> &'static str {
            "TestComponent"
        }

        fn queue_size(&self) -> usize {
            1000
        }
    }

    #[async_trait]
    impl Handler<TaskResult<TestOutput, TestError>> for TestComponent {
        type Result = ();

        async fn handle(
            &mut self,
            message: TaskResult<TestOutput, TestError>,
            _ctx: &ComponentContext<Self>,
        ) {
            if message.into_inner().is_ok() {
                self.received_count.fetch_add(1, Ordering::SeqCst);
            }
        }
    }

    #[async_trait]
    impl Handler<TaskResult<IoGroupOperatorOutput, IoGroupOperatorError>> for TestComponent {
        type Result = ();

        async fn handle(
            &mut self,
            _message: TaskResult<IoGroupOperatorOutput, IoGroupOperatorError>,
            _ctx: &ComponentContext<Self>,
        ) {
            // IoGroupOperator output - nothing to do
        }
    }

    #[tokio::test]
    async fn test_io_group_operator_runs_all_subtasks() {
        let system = System::new();
        let dispatcher = Dispatcher::new(chroma_system::DispatcherConfig {
            num_worker_threads: 4,
            task_queue_limit: 1000,
            dispatcher_queue_size: 1000,
            worker_queue_size: 1000,
            active_io_tasks: 100,
        });
        let dispatcher_handle = system.start_component(dispatcher);

        let counter = Arc::new(AtomicUsize::new(0));
        let received_count = Arc::new(AtomicUsize::new(0));

        let component = TestComponent {
            received_count: received_count.clone(),
        };
        let component_handle = system.start_component(component);

        let cancellation_token = CancellationToken::new();
        let num_subtasks = 5;

        // Create subtasks
        let mut subtasks: Vec<Box<dyn TaskWrapper>> = Vec::with_capacity(num_subtasks);
        for _ in 0..num_subtasks {
            let task = wrap(
                Box::new(TestIncrementOperator {
                    counter: counter.clone(),
                }),
                TestInput,
                component_handle.receiver(),
                cancellation_token.clone(),
            );
            subtasks.push(task);
        }

        // Create and run IoGroupOperator
        let io_group_task = wrap(
            Box::new(IoGroupOperator::new()),
            IoGroupOperatorInput::new(Arc::new(Mutex::new(Some(subtasks)))),
            component_handle.receiver(),
            cancellation_token,
        );

        // Send task to dispatcher
        dispatcher_handle.clone().send(io_group_task, None).await.unwrap();

        // Wait for tasks to complete
        tokio::time::sleep(tokio::time::Duration::from_millis(500)).await;

        // Verify all subtasks were executed
        assert_eq!(counter.load(Ordering::SeqCst), num_subtasks);
        // Verify all subtask results were received
        assert_eq!(received_count.load(Ordering::SeqCst), num_subtasks);
    }
}
