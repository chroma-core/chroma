use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use chroma_config::registry::Injectable;
use chroma_types::chroma_proto::fault_action::Act;
use chroma_types::chroma_proto::fault_injection_service_server::FaultInjectionService;
use chroma_types::chroma_proto::fault_selector::By;
use chroma_types::chroma_proto::{
    ActionDelay, ActionUnavailable, ClearFaultsRequest, ClearFaultsResponse, FaultAction,
    FaultEntry, FaultSelector, InjectFaultsRequest, InjectFaultsResponse, ListFaultsRequest,
    ListFaultsResponse, SelectFileLine, SelectLabel,
};
use parking_lot::RwLock;
use tonic::{Request, Response, Status};

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum FaultSelectorKind {
    FileLine { file: String, line: u32 },
    Label(String),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum FaultActionKind {
    Unavailable,
    Delay(Duration),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StoredFault {
    pub selector: FaultSelectorKind,
    pub action: FaultActionKind,
}

#[derive(Debug, Default)]
struct Inner {
    faults: RwLock<Vec<StoredFault>>,
}

#[derive(Debug, Clone, Default)]
pub struct FaultRegistry {
    inner: Arc<Inner>,
}

impl FaultRegistry {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn inject(&self, selector: FaultSelectorKind, action: FaultActionKind) {
        self.inner
            .faults
            .write()
            .push(StoredFault { selector, action });
    }

    pub fn list(&self) -> Vec<StoredFault> {
        self.inner.faults.read().clone()
    }

    pub fn clear_all(&self) -> usize {
        let mut faults = self.inner.faults.write();
        let cleared = faults.len();
        faults.clear();
        cleared
    }

    pub fn clear_selector(&self, selector: &FaultSelectorKind) -> usize {
        let mut faults = self.inner.faults.write();
        let before = faults.len();
        faults.retain(|fault| &fault.selector != selector);
        before - faults.len()
    }

    pub fn action_for_file_line(&self, file: &str, line: u32) -> Option<FaultActionKind> {
        self.inner
            .faults
            .read()
            .iter()
            .rev()
            .find_map(|fault| match &fault.selector {
                FaultSelectorKind::FileLine {
                    file: stored_file,
                    line: stored_line,
                } if stored_file == file && *stored_line == line => Some(fault.action.clone()),
                _ => None,
            })
    }

    pub fn action_for_label(&self, label: &str) -> Option<FaultActionKind> {
        self.inner
            .faults
            .read()
            .iter()
            .rev()
            .find_map(|fault| match &fault.selector {
                FaultSelectorKind::Label(stored_label) if stored_label == label => {
                    Some(fault.action.clone())
                }
                _ => None,
            })
    }
}

impl Injectable for FaultRegistry {}

fn invalid_argument(message: &'static str) -> Status {
    Status::invalid_argument(message)
}

fn selector_from_proto(selector: FaultSelector) -> Result<FaultSelectorKind, Status> {
    match selector.by {
        Some(By::FileLine(SelectFileLine { file, line })) => {
            Ok(FaultSelectorKind::FileLine { file, line })
        }
        Some(By::Label(SelectLabel { label })) => Ok(FaultSelectorKind::Label(label)),
        None => Err(invalid_argument("fault selector must specify a target")),
    }
}

fn action_from_proto(action: FaultAction) -> Result<FaultActionKind, Status> {
    match action.act {
        Some(Act::Unavailable(ActionUnavailable {})) => Ok(FaultActionKind::Unavailable),
        Some(Act::Delay(ActionDelay { delay_seconds })) => {
            Ok(FaultActionKind::Delay(Duration::from_secs(delay_seconds)))
        }
        None => Err(invalid_argument("fault action must specify an action")),
    }
}

fn selector_to_proto(selector: &FaultSelectorKind) -> FaultSelector {
    let by = match selector {
        FaultSelectorKind::FileLine { file, line } => By::FileLine(SelectFileLine {
            file: file.clone(),
            line: *line,
        }),
        FaultSelectorKind::Label(label) => By::Label(SelectLabel {
            label: label.clone(),
        }),
    };
    FaultSelector { by: Some(by) }
}

fn action_to_proto(action: &FaultActionKind) -> FaultAction {
    let act = match action {
        FaultActionKind::Unavailable => Act::Unavailable(ActionUnavailable {}),
        FaultActionKind::Delay(delay) => Act::Delay(ActionDelay {
            delay_seconds: delay.as_secs(),
        }),
    };
    FaultAction { act: Some(act) }
}

fn stored_fault_to_proto(fault: &StoredFault) -> FaultEntry {
    FaultEntry {
        selector: Some(selector_to_proto(&fault.selector)),
        action: Some(action_to_proto(&fault.action)),
    }
}

#[async_trait]
impl FaultInjectionService for FaultRegistry {
    #[tracing::instrument(skip(self, request))]
    async fn inject_faults(
        &self,
        request: Request<InjectFaultsRequest>,
    ) -> Result<Response<InjectFaultsResponse>, Status> {
        let request = request.into_inner();
        let selector = request
            .selector
            .ok_or_else(|| invalid_argument("inject_faults requires a selector"))
            .and_then(selector_from_proto)?;
        let action = request
            .action
            .ok_or_else(|| invalid_argument("inject_faults requires an action"))
            .and_then(action_from_proto)?;
        tracing::warn!(selector = ?selector, action = ?action, "inject fault configured");
        self.inject(selector, action);
        Ok(Response::new(InjectFaultsResponse {}))
    }

    #[tracing::instrument(skip(self, _request))]
    async fn list_faults(
        &self,
        _request: Request<ListFaultsRequest>,
    ) -> Result<Response<ListFaultsResponse>, Status> {
        let faults = self
            .list()
            .iter()
            .map(stored_fault_to_proto)
            .collect::<Vec<_>>();
        Ok(Response::new(ListFaultsResponse { faults }))
    }

    #[tracing::instrument(skip(self, request))]
    async fn clear_faults(
        &self,
        request: Request<ClearFaultsRequest>,
    ) -> Result<Response<ClearFaultsResponse>, Status> {
        let request = request.into_inner();
        let selector = request.selector.map(selector_from_proto).transpose()?;
        let cleared_count = match selector.as_ref() {
            Some(selector) => self.clear_selector(selector),
            None => self.clear_all(),
        };
        tracing::warn!(
            selector = ?selector,
            cleared_count,
            "inject fault cleared"
        );
        Ok(Response::new(ClearFaultsResponse {
            cleared_count: cleared_count as u64,
        }))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use chroma_types::chroma_proto::fault_injection_service_server::FaultInjectionService;

    fn label_selector(label: &str) -> FaultSelector {
        FaultSelector {
            by: Some(By::Label(SelectLabel {
                label: label.to_string(),
            })),
        }
    }

    fn file_line_selector(file: &str, line: u32) -> FaultSelector {
        FaultSelector {
            by: Some(By::FileLine(SelectFileLine {
                file: file.to_string(),
                line,
            })),
        }
    }

    fn unavailable_action() -> FaultAction {
        FaultAction {
            act: Some(Act::Unavailable(ActionUnavailable {})),
        }
    }

    fn delay_action(delay_seconds: u64) -> FaultAction {
        FaultAction {
            act: Some(Act::Delay(ActionDelay { delay_seconds })),
        }
    }

    #[tokio::test]
    async fn inject_and_list_label_fault() {
        let registry = FaultRegistry::new();
        registry
            .inject_faults(Request::new(InjectFaultsRequest {
                selector: Some(label_selector("slow-path")),
                action: Some(unavailable_action()),
            }))
            .await
            .expect("inject should succeed");

        let response = registry
            .list_faults(Request::new(ListFaultsRequest {}))
            .await
            .expect("list should succeed")
            .into_inner();

        assert_eq!(response.faults.len(), 1);
        assert_eq!(
            response.faults[0].selector,
            Some(label_selector("slow-path"))
        );
        assert_eq!(response.faults[0].action, Some(unavailable_action()));
    }

    #[tokio::test]
    async fn inject_and_list_file_line_fault() {
        let registry = FaultRegistry::new();
        registry
            .inject_faults(Request::new(InjectFaultsRequest {
                selector: Some(file_line_selector("src/lib.rs", 42)),
                action: Some(delay_action(7)),
            }))
            .await
            .expect("inject should succeed");

        let response = registry
            .list_faults(Request::new(ListFaultsRequest {}))
            .await
            .expect("list should succeed")
            .into_inner();

        assert_eq!(response.faults.len(), 1);
        assert_eq!(
            response.faults[0].selector,
            Some(file_line_selector("src/lib.rs", 42))
        );
        assert_eq!(response.faults[0].action, Some(delay_action(7)));
    }

    #[tokio::test]
    async fn newest_matching_fault_wins() {
        let registry = FaultRegistry::new();
        registry.inject(
            FaultSelectorKind::Label("overlap".to_string()),
            FaultActionKind::Unavailable,
        );
        registry.inject(
            FaultSelectorKind::Label("overlap".to_string()),
            FaultActionKind::Delay(Duration::from_secs(9)),
        );

        assert_eq!(
            registry.action_for_label("overlap"),
            Some(FaultActionKind::Delay(Duration::from_secs(9)))
        );
    }

    #[tokio::test]
    async fn clear_all_faults_returns_count() {
        let registry = FaultRegistry::new();
        registry.inject(
            FaultSelectorKind::Label("a".to_string()),
            FaultActionKind::Unavailable,
        );
        registry.inject(
            FaultSelectorKind::Label("b".to_string()),
            FaultActionKind::Delay(Duration::from_secs(3)),
        );

        let response = registry
            .clear_faults(Request::new(ClearFaultsRequest { selector: None }))
            .await
            .expect("clear should succeed")
            .into_inner();

        assert_eq!(response.cleared_count, 2);
        assert!(registry.list().is_empty());
    }

    #[tokio::test]
    async fn clear_selector_removes_all_matching_faults() {
        let registry = FaultRegistry::new();
        registry.inject(
            FaultSelectorKind::Label("keep".to_string()),
            FaultActionKind::Unavailable,
        );
        registry.inject(
            FaultSelectorKind::Label("drop".to_string()),
            FaultActionKind::Unavailable,
        );
        registry.inject(
            FaultSelectorKind::Label("drop".to_string()),
            FaultActionKind::Delay(Duration::from_secs(1)),
        );

        let response = registry
            .clear_faults(Request::new(ClearFaultsRequest {
                selector: Some(label_selector("drop")),
            }))
            .await
            .expect("clear should succeed")
            .into_inner();

        assert_eq!(response.cleared_count, 2);
        assert_eq!(registry.list().len(), 1);
        assert_eq!(
            registry.action_for_label("keep"),
            Some(FaultActionKind::Unavailable)
        );
        assert_eq!(registry.action_for_label("drop"), None);
    }

    #[tokio::test]
    async fn invalid_requests_return_invalid_argument() {
        let registry = FaultRegistry::new();

        let inject_error = registry
            .inject_faults(Request::new(InjectFaultsRequest {
                selector: Some(FaultSelector { by: None }),
                action: Some(unavailable_action()),
            }))
            .await
            .expect_err("inject should fail");
        assert_eq!(inject_error.code(), tonic::Code::InvalidArgument);

        let clear_error = registry
            .clear_faults(Request::new(ClearFaultsRequest {
                selector: Some(FaultSelector { by: None }),
            }))
            .await
            .expect_err("clear should fail");
        assert_eq!(clear_error.code(), tonic::Code::InvalidArgument);
    }
}
