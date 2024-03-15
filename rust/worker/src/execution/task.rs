use crate::system::Receiver;
use async_trait::async_trait;
use std::fmt::Debug;

#[derive(Debug)]
struct ExecutionMessage<Op, Input, Output>
where
    Op: Operator<Input, Output>,
    Input: Send + Sync,
    Output: Send + Sync,
{
    operator: Op,
    input: Input,
    reply_to: Option<tokio::sync::oneshot::Sender<Output>>,
}

// TODO: rename
#[async_trait]
pub(super) trait ExecutionMessageWrapper: Send + Sync + Debug {
    async fn execute(&mut self);
}

#[async_trait]
impl<Op, Input, Output> ExecutionMessageWrapper for ExecutionMessage<Op, Input, Output>
where
    Op: Operator<Input, Output>,
    Input: Send + Sync + Clone + Debug,
    Output: Send + Sync + Debug,
{
    async fn execute(&mut self) {
        let result = self.operator.execute(self.input.clone()).await;
        let res = self.reply_to.take().unwrap().send(result);
    }
}

pub(super) fn wrap<Op, Input, Output>(
    operator: Op,
    input: Input,
    reply_to: tokio::sync::oneshot::Sender<Output>,
) -> Box<dyn ExecutionMessageWrapper>
where
    Op: Operator<Input, Output> + 'static,
    Input: Send + Sync + Clone + Debug + 'static,
    Output: Send + Sync + Debug + 'static,
{
    Box::new(ExecutionMessage {
        operator,
        input,
        reply_to: Some(reply_to),
    })
}

struct ExecutionContext {
    dispatch: Box<dyn Receiver<Box<dyn ExecutionMessageWrapper>>>,
}

#[async_trait]
pub(super) trait Operator<Input, Output>
where
    Self: Send + Sync + Sized + Debug,
    Input: Send + Sync,
    Output: Send + Sync,
{
    async fn execute(&self, input: Input) -> Output;

    fn then<Other, OutputNext>(
        self,
        other: Other,
    ) -> Pipeline<Self, Other, Input, Output, OutputNext>
    where
        Other: Operator<Output, OutputNext>,
        OutputNext: Send + Sync,
    {
        Pipeline {
            operator1: self,
            operator2: other,
            _marker: std::marker::PhantomData,
        }
    }

    fn parallel<Other, OutputNext, OutputCombined>(
        self,
        other: Other,
    ) -> Parallel<Self, Other, Input, Output, OutputNext, OutputCombined>
    where
        Input: Clone,
        Other: Operator<Input, OutputNext>,
        OutputNext: Send + Sync,
        OutputCombined: Send + Sync,
    {
        Parallel {
            operator1: self,
            operator2: other,
            _marker: std::marker::PhantomData,
        }
    }
}

trait Combinable<T> {
    type Combined;
    fn combine_with(self, other: T) -> Self::Combined;
}

impl Combinable<Vec<String>> for Vec<String> {
    type Combined = Vec<Vec<String>>;
    fn combine_with(self, other: Vec<String>) -> Vec<Vec<String>> {
        vec![self, other]
    }
}

impl Combinable<Vec<String>> for Vec<Vec<String>> {
    type Combined = Vec<Vec<String>>;
    fn combine_with(self, other: Vec<String>) -> Vec<Vec<String>> {
        let mut result = self;
        result.push(other);
        result
    }
}

impl Combinable<Vec<Vec<String>>> for Vec<Vec<String>> {
    type Combined = Vec<Vec<String>>;
    fn combine_with(self, other: Vec<Vec<String>>) -> Vec<Vec<String>> {
        let mut result = self;
        result.extend(other);
        result
    }
}

impl Combinable<Vec<Vec<String>>> for Vec<String> {
    type Combined = Vec<Vec<String>>;
    fn combine_with(self, other: Vec<Vec<String>>) -> Vec<Vec<String>> {
        let mut result = other;
        result.push(self);
        result
    }
}

pub(super) struct Pipeline<O1, O2, Input, Intermediate, Output>
where
    O1: Operator<Input, Intermediate>,
    O2: Operator<Intermediate, Output>,
    Input: Send + Sync,
    Intermediate: Send + Sync,
    Output: Send + Sync,
{
    operator1: O1,
    operator2: O2,
    _marker: std::marker::PhantomData<(Input, Intermediate, Output)>,
}

impl<O1, O2, Input, Intermediate, Output> std::fmt::Debug
    for Pipeline<O1, O2, Input, Intermediate, Output>
where
    O1: Operator<Input, Intermediate>,
    O2: Operator<Intermediate, Output>,
    Input: Send + Sync,
    Intermediate: Send + Sync,
    Output: Send + Sync,
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Pipeline")
            .field("operator1", &self.operator1)
            .field("operator2", &self.operator2)
            .finish()
    }
}

#[async_trait]
impl<O1, O2, Input, Intermediate, Output> Operator<Input, Output>
    for Pipeline<O1, O2, Input, Intermediate, Output>
where
    O1: Operator<Input, Intermediate>,
    O2: Operator<Intermediate, Output>,
    Input: Send + Sync,
    Intermediate: Send + Sync,
    Output: Send + Sync,
{
    async fn execute(&self, input: Input) -> Output {
        let intermediate = self.operator1.execute(input).await;
        self.operator2.execute(intermediate).await
    }
}

pub(super) struct Parallel<O1, O2, Input, Output1, Output2, OutputCombined>
where
    O1: Operator<Input, Output1>,
    O2: Operator<Input, Output2>,
    Input: Send + Sync + Clone,
    Output1: Send + Sync,
    Output2: Send + Sync,
    OutputCombined: Send + Sync,
{
    operator1: O1,
    operator2: O2,
    _marker: std::marker::PhantomData<(Input, Output1, Output2, OutputCombined)>,
}

#[async_trait]
impl<O1, O2, Input, Output1, Output2, OutputCombined> Operator<Input, OutputCombined>
    for Parallel<O1, O2, Input, Output1, Output2, OutputCombined>
where
    O1: Operator<Input, Output1>,
    O2: Operator<Input, Output2>,
    Input: Send + Sync + Clone,
    Output1: Send + Sync + Combinable<Output2, Combined = OutputCombined>,
    Output2: Send + Sync + Combinable<Output1, Combined = OutputCombined>,
    OutputCombined: Send + Sync,
{
    async fn execute(&self, input: Input) -> OutputCombined {
        let operator1_future = self.operator1.execute(input.clone());
        let operator2_future = self.operator2.execute(input);
        let (res1, res2) = tokio::join!(operator1_future, operator2_future);
        res1.combine_with(res2)
    }
}

impl<O1, O2, Input, Output1, Output2, OutputCombined> std::fmt::Debug
    for Parallel<O1, O2, Input, Output1, Output2, OutputCombined>
where
    O1: Operator<Input, Output1>,
    O2: Operator<Input, Output2>,
    Input: Send + Sync + Clone,
    Output1: Send + Sync,
    Output2: Send + Sync,
    OutputCombined: Send + Sync,
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Parallel")
            .field("operator1", &self.operator1)
            .field("operator2", &self.operator2)
            .finish()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[derive(Debug)]
    struct OperatorA; // Example operator
    #[async_trait]
    impl Operator<u32, String> for OperatorA {
        async fn execute(&self, input: u32) -> String {
            input.to_string()
        }
    }

    #[derive(Debug)]
    struct OperatorB; // Another operator, expecting String and returning usize
    #[async_trait]
    impl Operator<String, u32> for OperatorB {
        async fn execute(&self, input: String) -> u32 {
            input.len() as u32
        }
    }

    #[derive(Debug)]

    struct OperatorC; // Another operator, expecting String and returning String + "2"
    #[async_trait]
    impl Operator<String, String> for OperatorC {
        async fn execute(&self, input: String) -> String {
            input + "2"
        }
    }

    #[tokio::test]
    async fn test_simple() {
        let operator_1 = OperatorA;
        let operator_2 = OperatorB;
        let operator_3 = OperatorA;
        let pipeline = operator_1.then(operator_2).then(operator_3);
        let result = pipeline.execute(42).await;
        println!("Result: {}", result);
        assert_eq!(result, "2".to_owned());
    }

    #[tokio::test]
    async fn test_parallel_complicated_data() {
        #[derive(Debug)]
        struct PullData; // Example operator
        #[async_trait]
        impl Operator<(), Vec<String>> for PullData {
            async fn execute(&self, _: ()) -> Vec<String> {
                vec![
                    "Hello".to_owned(),
                    "World".to_owned(),
                    "42".to_owned(),
                    "2".to_owned(),
                    "42".to_owned(),
                ]
            }
        }

        #[derive(Debug)]
        struct SortData; // Another operator, expecting Vec<String> and returning Vec<String>
        #[async_trait]
        impl Operator<Vec<String>, Vec<String>> for SortData {
            async fn execute(&self, mut input: Vec<String>) -> Vec<String> {
                input.sort();
                input
            }
        }

        #[derive(Debug)]
        // Operator that dedups a range of the input, assumes input is sorted
        struct DedupePartial {
            start: usize,
            end: usize,
        }

        #[async_trait]
        impl Operator<Vec<String>, Vec<String>> for DedupePartial {
            async fn execute(&self, input: Vec<String>) -> Vec<String> {
                let mut range = input[self.start..self.end].to_vec();
                range.dedup();
                range
            }
        }

        #[derive(Debug)]
        struct MergeDedupe; // Another operator, expecting Vec<Vec<String>> and returning Vec<String>
        #[async_trait]
        impl Operator<Vec<Vec<String>>, Vec<String>> for MergeDedupe {
            async fn execute(&self, input: Vec<Vec<String>>) -> Vec<String> {
                let mut result = input.into_iter().flatten().collect::<Vec<String>>();
                result.sort();
                result.dedup();
                result
            }
        }

        let pull_data = PullData;
        let sort_data = SortData;
        let dedupe_partial_1 = DedupePartial { start: 0, end: 2 };
        let dedupe_partial_2 = DedupePartial { start: 2, end: 3 };
        let dedupe_partial_3 = DedupePartial { start: 3, end: 5 };
        let merge_dedupe = MergeDedupe;

        let pipeline = pull_data
            .then(sort_data)
            .then(
                dedupe_partial_1
                    .parallel(dedupe_partial_2)
                    .parallel(dedupe_partial_3),
            )
            .then(merge_dedupe);
        println!("Pipeline: {:?}", pipeline);
        let result = pipeline.execute(()).await;

        println!("Result: {:?}", result);
    }
}
