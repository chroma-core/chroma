use crate::client::collection::Collection;
use crate::tui::collection_browser::app_state::AppState;
use crate::tui::collection_browser::query_editor::Mode;
use crate::tui::collection_browser::{Record, Screen};
use chroma_types::{GetResponse, IncludeList};
use crossterm::event::{Event, EventStream, KeyCode, KeyEvent, KeyModifiers};
use futures::StreamExt;
use tokio::sync::mpsc::{unbounded_channel, UnboundedReceiver, UnboundedSender};

pub enum MainAction {
    Quit,
    NextRow,
    PreviousRow,
    NextColumn,
    PreviousColumn,
    Expand,
    RecordsLoaded(Vec<Record>, u32),
    Search,
}

pub enum ExpandAction {
    Quit,
    ScrollUp,
    ScrollDown,
    NextColumn,
    PreviousColumn,
}

pub enum SearchAction {
    Submit,
    Quit,
    NextField,
    PreviousField,
    Input(Event),
    Edit,
    Reset,
    EditQuit,
    ToggleOperator,
}

pub enum SearchResultAction {
    Quit,
    NextRow,
    PreviousRow,
    NextColumn,
    PreviousColumn,
    Expand,
    RecordsLoaded(Vec<Record>),
    Search,
}

pub enum Action {
    Main(MainAction),
    Expand(ExpandAction),
    Search(SearchAction),
    SearchResult(SearchResultAction),
    Error(String),
    Quit,
    SubmitSearch,
}

pub struct EventsHandler {
    collection: Collection,
    events: EventStream,
    tx: UnboundedSender<Action>,
    rx: UnboundedReceiver<Action>,
}

impl EventsHandler {
    pub fn new(collection: Collection) -> Self {
        let (tx, rx) = unbounded_channel::<Action>();
        Self {
            collection,
            tx,
            rx,
            events: EventStream::new(),
        }
    }

    fn get_response_to_records(response: GetResponse) -> Vec<Record> {
        let docs = response
            .documents
            .unwrap_or_else(|| vec![None; response.ids.len()]);
        let metas = response
            .metadatas
            .unwrap_or_else(|| vec![None; response.ids.len()]);

        response
            .ids
            .into_iter()
            .zip(docs)
            .zip(metas)
            .map(|((id, document), metadata)| Record {
                id,
                document,
                metadata,
            })
            .collect()
    }

    pub fn load_records(&self, app_state: &mut AppState, offset: u32, limit: u32) {
        let tx = self.tx.clone();
        let collection = self.collection.clone();
        app_state.loading = true;
        tokio::spawn(async move {
            let count_response = collection.count().await;

            let count = count_response.unwrap_or_else(|e| {
                let _ = tx.send(Action::Error(format!(
                    "Failed to get collection size: {}",
                    e
                )));
                0
            });

            let records_response = collection
                .get(
                    None,
                    None,
                    None,
                    Some(IncludeList::default_get()),
                    Some(limit),
                    Some(offset),
                )
                .await;

            match records_response {
                Ok(response) => {
                    let records = Self::get_response_to_records(response);
                    let _ = tx.send(Action::Main(MainAction::RecordsLoaded(records, count)));
                }
                Err(_) => {
                    let _ = tx.send(Action::Error(String::from("Failed to load records")));
                }
            }
        });
    }

    pub fn initialize_records(&self, app_state: &mut AppState) {
        self.load_records(app_state, 0, 100);
    }

    pub fn load_records_batch(&self, app_state: &mut AppState) {
        self.load_records(app_state, app_state.records.len() as u32, 100)
    }

    pub fn submit_search(&self, app_state: &mut AppState) {
        if app_state.loading || app_state.error.is_some() {
            return;
        }

        let ids = app_state.query_editor_state.ids();
        let where_document = app_state.query_editor_state.where_document();
        let metadata = app_state.query_editor_state.metadata();

        if let (None, None, None) = (&ids, &where_document, &metadata) {
            app_state.screen = Screen::Main;
            return;
        }

        app_state.loading = true;
        app_state.screen = Screen::SearchResult;

        let tx = self.tx.clone();
        let collection = self.collection.clone();

        tokio::spawn(async move {
            let records_response = collection
                .get(
                    ids,
                    metadata.as_deref(),
                    where_document.as_deref(),
                    Some(IncludeList::default_get()),
                    None,
                    None,
                )
                .await;

            match records_response {
                Ok(response) => {
                    let records = Self::get_response_to_records(response);
                    let _ = tx.send(Action::SearchResult(SearchResultAction::RecordsLoaded(
                        records,
                    )));
                }
                Err(_) => {
                    let _ = tx.send(Action::Error(String::from("Failed to submit search")));
                }
            }
        });
    }

    pub fn main_events(key: KeyEvent) -> Option<Action> {
        let main_action = match key.code {
            KeyCode::Esc => Some(MainAction::Quit),
            KeyCode::Down => Some(MainAction::NextRow),
            KeyCode::Up => Some(MainAction::PreviousRow),
            KeyCode::Right => Some(MainAction::NextColumn),
            KeyCode::Left => Some(MainAction::PreviousColumn),
            KeyCode::Enter => Some(MainAction::Expand),
            KeyCode::Char('s') => Some(MainAction::Search),
            _ => None,
        };

        match main_action {
            Some(MainAction::Quit) => Some(Action::Quit),
            None => None,
            Some(action) => Some(Action::Main(action)),
        }
    }

    pub fn expand_events(key: KeyEvent) -> Option<Action> {
        let expand_action = match key.code {
            KeyCode::Esc => Some(ExpandAction::Quit),
            KeyCode::Up => Some(ExpandAction::ScrollUp),
            KeyCode::Down => Some(ExpandAction::ScrollDown),
            KeyCode::Right => Some(ExpandAction::NextColumn),
            KeyCode::Left => Some(ExpandAction::PreviousColumn),
            _ => None,
        };

        expand_action.map(Action::Expand)
    }

    pub fn search_events(key: KeyEvent) -> Option<Action> {
        let search_action = match key.code {
            KeyCode::Esc => Some(SearchAction::Quit),
            KeyCode::Tab | KeyCode::Right | KeyCode::Down => Some(SearchAction::NextField),
            KeyCode::BackTab | KeyCode::Left | KeyCode::Up => Some(SearchAction::PreviousField),
            KeyCode::Enter => Some(SearchAction::Submit),
            KeyCode::Char('e') => Some(SearchAction::Edit),
            KeyCode::Char(' ') => Some(SearchAction::ToggleOperator),
            KeyCode::Char('c') => Some(SearchAction::Reset),
            _ => None,
        };

        match search_action {
            Some(SearchAction::Submit) => Some(Action::SubmitSearch),
            Some(action) => Some(Action::Search(action)),
            None => None,
        }
    }

    pub fn search_results_events(key: KeyEvent) -> Option<Action> {
        let search_result_action = match key.code {
            KeyCode::Esc => Some(SearchResultAction::Quit),
            KeyCode::Down => Some(SearchResultAction::NextRow),
            KeyCode::Up => Some(SearchResultAction::PreviousRow),
            KeyCode::Right => Some(SearchResultAction::NextColumn),
            KeyCode::Left => Some(SearchResultAction::PreviousColumn),
            KeyCode::Enter => Some(SearchResultAction::Expand),
            KeyCode::Char('s') => Some(SearchResultAction::Search),
            _ => None,
        };

        search_result_action.map(Action::SearchResult)
    }

    pub fn parse_key(key: KeyEvent, screen: &Screen) -> Option<Action> {
        match screen {
            Screen::Main => Self::main_events(key),
            Screen::Expand => Self::expand_events(key),
            Screen::Search => Self::search_events(key),
            Screen::SearchResult => Self::search_results_events(key),
        }
    }

    pub async fn next(&mut self, app_state: &mut AppState) -> Option<Action> {
        tokio::select! {
            event = self.events.next() => {
                if let Some(Ok(Event::Key(key))) = event {
                    if key.modifiers == KeyModifiers::CONTROL && key.code == KeyCode::Char('c') {
                        return Some(Action::Quit);
                    }
                }

                if app_state.query_editor_state.mode == Mode::Editing {
                    if let Some(Ok(Event::Key(key))) = event {
                        if key.code == KeyCode::Esc {
                            return Some(Action::Search(SearchAction::EditQuit));
                        }
                    }
                    if let Some(Ok(event)) = event {
                        return Some(Action::Search(SearchAction::Input(event)));
                    }
                }
                if let Some(Ok(Event::Key(key))) = event {
                    Self::parse_key(key, &app_state.screen)
                } else {
                    None
                }
            }

            action = self.rx.recv() => {
                action
            }
        }
    }
}
