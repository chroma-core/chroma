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
use uuid::Uuid;

#[derive(Copy, Clone, Debug, Eq, PartialEq, Hash)]
pub struct FaultId(Uuid);

impl FaultId {
    pub fn new() -> Self {
        Self(Uuid::new_v4())
    }
}

impl Default for FaultId {
    fn default() -> Self {
        Self::new()
    }
}

impl std::fmt::Display for FaultId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl TryFrom<&str> for FaultId {
    type Error = Status;

    fn try_from(id: &str) -> Result<Self, Self::Error> {
        id.try_into()
            .map(Self)
            .map_err(|_| invalid_argument("fault id must be a UUID"))
    }
}

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
    pub id: FaultId,
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

    pub fn inject(&self, selector: FaultSelectorKind, action: FaultActionKind) -> FaultId {
        let id = FaultId::new();
        self.inner.faults.write().push(StoredFault {
            id,
            selector,
            action,
        });
        id
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

    pub fn clear_id(&self, id: Option<&FaultId>) -> usize {
        match id {
            Some(id) => {
                let mut faults = self.inner.faults.write();
                let before = faults.len();
                faults.retain(|fault| &fault.id != id);
                before - faults.len()
            }
            None => self.clear_all(),
        }
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

impl TryFrom<FaultSelector> for FaultSelectorKind {
    type Error = Status;

    fn try_from(selector: FaultSelector) -> Result<Self, Self::Error> {
        match selector.by {
            Some(By::FileLine(SelectFileLine { file, line })) => Ok(Self::FileLine { file, line }),
            Some(By::Label(SelectLabel { label })) => Ok(Self::Label(label)),
            None => Err(invalid_argument("fault selector must specify a target")),
        }
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
        id: fault.id.to_string(),
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
            .and_then(|selector| selector.try_into())?;
        let action = request
            .action
            .ok_or_else(|| invalid_argument("inject_faults requires an action"))
            .and_then(action_from_proto)?;
        tracing::warn!(selector = ?selector, action = ?action, "inject fault configured");
        let id = self.inject(selector, action);
        Ok(Response::new(InjectFaultsResponse { id: id.to_string() }))
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
        let fault_id: Option<FaultId> = request.id.as_deref().map(TryInto::try_into).transpose()?;
        let selector: Option<FaultSelectorKind> =
            request.selector.map(TryInto::try_into).transpose()?;
        let cleared_count = match (&fault_id, &selector) {
            (Some(_), Some(_)) => {
                return Err(invalid_argument(
                    "clear_faults requires exactly one of id or selector, not both",
                ));
            }
            (Some(id), None) => self.clear_id(Some(id)),
            (None, Some(sel)) => self.clear_selector(sel),
            (None, None) => self.clear_all(),
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

    fn parse_fault_id(id: &str) -> FaultId {
        id.try_into().expect("fault id should be a UUID")
    }

    #[tokio::test]
    async fn inject_and_list_label_fault() {
        let registry = FaultRegistry::new();
        let inject_response = registry
            .inject_faults(Request::new(InjectFaultsRequest {
                selector: Some(label_selector("slow-path")),
                action: Some(unavailable_action()),
            }))
            .await
            .expect("inject should succeed")
            .into_inner();
        let fault_id = parse_fault_id(&inject_response.id);

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
        assert_eq!(response.faults[0].id, fault_id.to_string());
    }

    #[tokio::test]
    async fn inject_and_list_file_line_fault() {
        let registry = FaultRegistry::new();
        let inject_response = registry
            .inject_faults(Request::new(InjectFaultsRequest {
                selector: Some(file_line_selector("src/lib.rs", 42)),
                action: Some(delay_action(7)),
            }))
            .await
            .expect("inject should succeed")
            .into_inner();
        let fault_id = parse_fault_id(&inject_response.id);

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
        assert_eq!(response.faults[0].id, fault_id.to_string());
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
            .clear_faults(Request::new(ClearFaultsRequest {
                selector: None,
                id: None,
            }))
            .await
            .expect("clear should succeed")
            .into_inner();

        assert_eq!(response.cleared_count, 2);
        assert!(registry.list().is_empty());
    }

    #[tokio::test]
    async fn clear_id_removes_only_matching_fault() {
        let registry = FaultRegistry::new();
        let first = registry
            .inject_faults(Request::new(InjectFaultsRequest {
                selector: Some(label_selector("drop-one")),
                action: Some(unavailable_action()),
            }))
            .await
            .expect("inject should succeed")
            .into_inner();
        let second = registry
            .inject_faults(Request::new(InjectFaultsRequest {
                selector: Some(label_selector("drop-one")),
                action: Some(delay_action(5)),
            }))
            .await
            .expect("inject should succeed")
            .into_inner();

        let response = registry
            .clear_faults(Request::new(ClearFaultsRequest {
                selector: None,
                id: Some(second.id.clone()),
            }))
            .await
            .expect("clear should succeed")
            .into_inner();

        assert_eq!(response.cleared_count, 1);
        assert_eq!(
            registry.action_for_label("drop-one"),
            Some(FaultActionKind::Unavailable)
        );

        let faults = registry.list();
        assert_eq!(faults.len(), 1);
        assert_eq!(faults[0].id, parse_fault_id(&first.id));
    }

    #[tokio::test]
    async fn clear_by_selector_removes_only_matching_faults() {
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
                id: None,
            }))
            .await
            .expect("clear should succeed")
            .into_inner();

        assert_eq!(
            response.cleared_count, 2,
            "only faults matching selector 'drop' should be cleared"
        );
        let remaining = registry.list();
        assert_eq!(remaining.len(), 1, "the 'keep' fault should remain");
        assert_eq!(
            remaining[0].selector,
            FaultSelectorKind::Label("keep".to_string())
        );
        println!("clear_by_selector_removes_only_matching_faults: remaining={remaining:?}");
    }

    #[tokio::test]
    async fn clear_with_both_id_and_selector_returns_invalid_argument() {
        let registry = FaultRegistry::new();
        let inject_response = registry
            .inject_faults(Request::new(InjectFaultsRequest {
                selector: Some(label_selector("ambiguous")),
                action: Some(unavailable_action()),
            }))
            .await
            .expect("inject should succeed")
            .into_inner();

        let err = registry
            .clear_faults(Request::new(ClearFaultsRequest {
                selector: Some(label_selector("ambiguous")),
                id: Some(inject_response.id),
            }))
            .await
            .expect_err("clear should fail when both id and selector are set");
        assert_eq!(err.code(), tonic::Code::InvalidArgument);
        println!("clear_with_both_id_and_selector_returns_invalid_argument: error={err}");
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

        let clear_by_invalid_id_error = registry
            .clear_faults(Request::new(ClearFaultsRequest {
                selector: None,
                id: Some("not-a-uuid".to_string()),
            }))
            .await
            .expect_err("clear should fail");
        assert_eq!(
            clear_by_invalid_id_error.code(),
            tonic::Code::InvalidArgument
        );
    }
}
