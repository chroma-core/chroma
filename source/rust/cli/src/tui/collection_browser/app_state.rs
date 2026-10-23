use crate::tui::collection_browser::events::{
    Action, ExpandAction, MainAction, SearchAction, SearchResultAction,
};
use crate::tui::collection_browser::query_editor::{Mode, QueryEditorState};
use crate::tui::collection_browser::table::AppTable;
use crate::tui::collection_browser::{Record, Screen};
use crossterm::event::{Event, KeyCode};
use ratatui::widgets::TableState;
use textwrap::wrap;
use tui_input::backend::crossterm::EventHandler;

pub enum ScrollDirection {
    Up,
    Down,
}

#[derive(Debug, Eq, PartialEq)]
pub enum ExpandContext {
    Main,
    Query,
}

pub struct AppState {
    pub screen: Screen,
    pub records: Vec<Record>,
    pub records_table_state: TableState,
    pub query_records: Vec<Record>,
    pub query_table_state: TableState,
    pub expand_context: ExpandContext,
    pub collection_name: String,
    pub collection_size: u32,
    pub query_editor_state: QueryEditorState,
    pub initialized: bool,
    pub loading: bool,
    pub error: Option<String>,
    pub expand_scroll: u16,
    pub frame_height: u16,
    pub frame_width: u16,
}

impl AppState {
    pub fn new(collection_name: String) -> Self {
        AppState {
            screen: Screen::Main,
            records: vec![],
            records_table_state: TableState::default().with_selected(0),
            query_records: vec![],
            query_table_state: TableState::default().with_selected(0),
            expand_context: ExpandContext::Main,
            collection_name,
            collection_size: 0,
            query_editor_state: QueryEditorState::default(),
            initialized: false,
            loading: false,
            error: None,
            expand_scroll: 0,
            frame_height: 0,
            frame_width: 0,
        }
    }

    pub fn load_more_records(&self) -> bool {
        if self.loading || self.error.is_some() {
            return false;
        }

        let threshold = self.records.len() * 3 / 4;
        if self.records_table_state.selected().unwrap_or(0) >= threshold
            && self.records.len() <= self.collection_size as usize
        {
            return true;
        }

        false
    }

    fn handle_scroll(&mut self, direction: ScrollDirection) {
        let current_cell = AppTable::current_cell_content(&self.records_table_state, &self.records);
        if let Some(current_cell) = current_cell {
            let wrapped: Vec<String> = current_cell
                .content
                .lines()
                .flat_map(|l| wrap(l, self.frame_width as usize))
                .map(|w| w.to_string())
                .collect();
            let total_wrapped_lines = wrapped.len();
            let max_scroll = total_wrapped_lines.saturating_sub(self.frame_height as usize);
            match direction {
                ScrollDirection::Up => {
                    self.expand_scroll = self
                        .expand_scroll
                        .saturating_sub(1)
                        .clamp(0, max_scroll as u16);
                }
                ScrollDirection::Down => {
                    self.expand_scroll = self
                        .expand_scroll
                        .saturating_add(1)
                        .clamp(0, max_scroll as u16);
                }
            }
        }
    }

    pub fn apply_action(&mut self, action: Action) {
        match action {
            Action::Error(error) => {
                self.error = Some(error);
            }
            Action::Main(main_action) => self.main_actions(main_action),
            Action::Expand(expand_action) => self.expand_actions(expand_action),
            Action::Search(search_action) => self.search_actions(search_action),
            Action::SearchResult(search_result_action) => {
                self.search_results_actions(search_result_action)
            }
            _ => {}
        }
    }

    pub fn main_actions(&mut self, action: MainAction) {
        match action {
            MainAction::RecordsLoaded(records, count) => {
                self.records.extend(records);
                self.collection_size = count;
                self.loading = false;
                self.initialized = true;
            }
            MainAction::NextRow => AppTable::next_row(&mut self.records_table_state, &self.records),
            MainAction::PreviousRow => {
                AppTable::previous_row(&mut self.records_table_state, &self.records)
            }
            MainAction::NextColumn => AppTable::next_column(&mut self.records_table_state),
            MainAction::PreviousColumn => AppTable::previous_column(&mut self.records_table_state),
            MainAction::Expand => {
                if self.records_table_state.selected_cell().is_some() {
                    self.expand_context = ExpandContext::Main;
                    self.screen = Screen::Expand;
                }
            }
            MainAction::Search => self.screen = Screen::Search,
            MainAction::Quit => {}
        }
    }

    pub fn expand_actions(&mut self, action: ExpandAction) {
        match action {
            ExpandAction::Quit => {
                self.screen = match self.expand_context {
                    ExpandContext::Main => Screen::Main,
                    ExpandContext::Query => Screen::SearchResult,
                }
            }
            ExpandAction::ScrollUp => self.handle_scroll(ScrollDirection::Up),
            ExpandAction::ScrollDown => self.handle_scroll(ScrollDirection::Down),
            ExpandAction::NextColumn => match self.expand_context {
                ExpandContext::Main => AppTable::next_column(&mut self.records_table_state),
                ExpandContext::Query => AppTable::next_column(&mut self.query_table_state),
            },
            ExpandAction::PreviousColumn => match self.expand_context {
                ExpandContext::Main => AppTable::previous_column(&mut self.records_table_state),
                ExpandContext::Query => AppTable::previous_column(&mut self.query_table_state),
            },
        }
    }

    pub fn search_actions(&mut self, action: SearchAction) {
        match self.query_editor_state.mode {
            Mode::Normal => match action {
                SearchAction::Quit => self.screen = Screen::Main,
                SearchAction::NextField => self.query_editor_state.next_field(),
                SearchAction::PreviousField => self.query_editor_state.previous_field(),
                SearchAction::Submit => {}
                SearchAction::Edit => {
                    let current_field = self.query_editor_state.current_field();
                    if current_field.as_input_mut().is_some() {
                        self.query_editor_state.mode = Mode::Editing
                    }
                }
                SearchAction::ToggleOperator => self.query_editor_state.toggle_operator(),
                SearchAction::Reset => self.query_editor_state.reset(),
                _ => {}
            },
            Mode::Editing => match action {
                SearchAction::EditQuit => self.query_editor_state.mode = Mode::Normal,
                SearchAction::Input(event) => {
                    let input = self.query_editor_state.current_field();
                    if let Event::Key(key) = event {
                        let (mut at_start, mut at_end) = (false, false);

                        if let Some(input_state) = input.as_input_mut() {
                            at_start = input_state.cursor() == 0;
                            at_end = input_state.cursor() == input_state.value().len();
                        }

                        if input.as_toggle_mut().is_some() {
                            (at_start, at_end) = (true, true);
                            if key.code == KeyCode::Char(' ') {
                                self.query_editor_state.toggle_operator();
                                return;
                            }
                        }

                        if key.code == KeyCode::Right && at_end {
                            self.query_editor_state.next_field();
                            return;
                        }

                        if key.code == KeyCode::Left && at_start {
                            self.query_editor_state.previous_field();
                            return;
                        }
                    }

                    if let Some(input_state) = input.as_input_mut() {
                        input_state.handle_event(&event);
                    }
                }
                _ => {}
            },
        }
    }

    fn search_results_actions(&mut self, action: SearchResultAction) {
        match action {
            SearchResultAction::RecordsLoaded(records) => {
                self.loading = false;
                self.query_records = records;
            }
            SearchResultAction::Quit => {
                self.screen = Screen::Main;
                self.query_records = vec![];
                self.query_table_state = TableState::default().with_selected(0);
                self.expand_context = ExpandContext::Main;
            }
            SearchResultAction::NextRow => {
                AppTable::next_row(&mut self.query_table_state, &self.query_records)
            }
            SearchResultAction::PreviousRow => {
                AppTable::previous_row(&mut self.query_table_state, &self.query_records)
            }
            SearchResultAction::NextColumn => AppTable::next_column(&mut self.query_table_state),
            SearchResultAction::PreviousColumn => {
                AppTable::previous_column(&mut self.query_table_state)
            }
            SearchResultAction::Expand => {
                if self.query_table_state.selected_cell().is_some() {
                    self.expand_context = ExpandContext::Query;
                    self.screen = Screen::Expand;
                }
            }
            SearchResultAction::Search => self.screen = Screen::Search,
        }
    }
}
