use super::task::ExecutionMessageWrapper;
use crate::system::{Component, ComponentContext, Handler};
use async_trait::async_trait;

#[derive(Debug)]
struct Thread {}

impl Component for Thread {
    fn queue_size(&self) -> usize {
        2000
    }

    fn runtime() -> crate::system::ComponentRuntime {
        crate::system::ComponentRuntime::Dedicated
    }
}

#[async_trait]
impl Handler<Box<dyn ExecutionMessageWrapper>> for Thread {
    async fn handle(
        &mut self,
        message: Box<dyn ExecutionMessageWrapper>,
        ctx: &ComponentContext<Thread>,
    ) {
        let mut message = message;
        let _ = message.execute().await;
    }
}

impl Thread {
    pub fn new() -> Self {
        Thread {}
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        execution::task::{wrap, Operator},
        system::{self, ComponentContext},
    };
    use async_trait::async_trait;
    use tokio::sync::{mpsc, oneshot};

    #[derive(Debug)]
    struct OperatorMergeAndSortVecs;
    #[async_trait]
    impl Operator<Vec<String>, Vec<String>> for OperatorMergeAndSortVecs {
        async fn execute(&self, input: Vec<String>) -> Vec<String> {
            let mut out = input;
            out.sort();
            out
        }
    }

    #[tokio::test]
    async fn test_thread() {
        let mut system = system::System::new();
        let thread = Thread::new();
        let handle = system.start_component(thread);
        let recv = handle.receiver();

        let n_ops = 10;
        let op1 = OperatorMergeAndSortVecs;
        let op2 = OperatorMergeAndSortVecs;
        let pipe = op1.parallel(op2);
        let mut out = pipe.execute(vec!["a".to_string(), "b".to_string()]).await;
        let (tx, rx) = oneshot::channel();
        let msg = wrap(pipe, vec!["a".to_string(), "b".to_string()], tx);

        // schedule the task
        let _ = recv.send(msg).await;
        // wait for the task to complete
        let res = rx.await;
        println!("{:?}", res);
    }
}
