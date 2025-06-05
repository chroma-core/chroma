use crate::client::collection::Collection;
use crate::tui::collection_browser::app_state::AppState;
use crate::tui::collection_browser::app_ui::AppUI;
use crate::tui::collection_browser::events::{Action, EventsHandler};
use crate::ui_utils::Theme;
use chroma_types::Metadata;
use ratatui::DefaultTerminal;
use thiserror::Error;

mod app_state;
mod app_ui;
mod events;
mod input;
mod query_editor;
mod table;

#[derive(Debug, Error)]
pub enum CollectionBrowserError {
    #[error("Terminal render failed")]
    TerminalRedner,
}

pub struct Record {
    pub id: String,
    pub document: Option<String>,
    pub metadata: Option<Metadata>,
}

pub enum RecordField {
    ID,
    Document,
    Metadata,
}

pub enum Screen {
    Main,
    Search,
    SearchResult,
    Expand,
}

pub struct CollectionBrowser {
    app_state: AppState,
    ui: AppUI,
    events_handler: EventsHandler,
    terminal: DefaultTerminal,
}

impl CollectionBrowser {
    pub fn new(collection: Collection, theme: Theme) -> Self {
        let app_state = AppState::new(collection.name.clone());
        let ui = AppUI::new(theme);
        let events_handler = EventsHandler::new(collection);
        let terminal = ratatui::init();
        Self {
            app_state,
            ui,
            events_handler,
            terminal,
        }
    }

    pub async fn run(&mut self) -> Result<(), CollectionBrowserError> {
        self.events_handler.initialize_records(&mut self.app_state);

        loop {
            if let Some(action) = self.events_handler.next(&mut self.app_state).await {
                match action {
                    Action::Quit => break,
                    Action::SubmitSearch => self.events_handler.submit_search(&mut self.app_state),
                    _ => self.app_state.apply_action(action),
                }
            }

            if self.app_state.load_more_records() {
                self.events_handler.load_records_batch(&mut self.app_state);
            }

            self.terminal
                .draw(|frame| {
                    self.app_state.frame_height = frame.area().height;
                    self.app_state.frame_width = frame.area().width;
                    self.ui.render(frame, &mut self.app_state)
                })
                .map_err(|_| CollectionBrowserError::TerminalRedner)?;
        }

        ratatui::restore();
        Ok(())
    }
}
