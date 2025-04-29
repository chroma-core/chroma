use crate::client::collection::Collection;
use crate::tui::collection_browser::app::{App, Screen};
use crate::tui::collection_browser::ui::UI;
use ratatui::backend::CrosstermBackend;
use ratatui::crossterm::terminal::{
    disable_raw_mode, enable_raw_mode, EnterAlternateScreen, LeaveAlternateScreen,
};
use ratatui::crossterm::{event, execute};
use ratatui::Terminal;
use std::error::Error;
use std::io;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::Mutex;

pub struct CollectionBrowser {
    app: Arc<Mutex<App>>,
    ui: UI,
}

impl CollectionBrowser {
    pub fn new(collection: Collection) -> Self {
        let app = Arc::new(Mutex::new(App::new(collection)));
        Self { app, ui: UI::new() }
    }

    fn get_terminal() -> Result<Terminal<CrosstermBackend<io::Stdout>>, Box<dyn Error>> {
        enable_raw_mode()?;
        let mut stdout = io::stdout();
        execute!(stdout, EnterAlternateScreen)?;
        let backend = CrosstermBackend::new(stdout);
        let mut terminal = Terminal::new(backend)?;
        terminal.show_cursor()?;
        Ok(terminal)
    }

    fn clear_terminal(
        terminal: &mut Terminal<CrosstermBackend<io::Stdout>>,
    ) -> Result<(), Box<dyn Error>> {
        disable_raw_mode()?;
        execute!(terminal.backend_mut(), LeaveAlternateScreen)?;
        terminal.show_cursor()?;
        Ok(())
    }

    pub async fn run(&mut self) -> Result<(), Box<dyn Error>> {
        let mut terminal = Self::get_terminal()?;

        while {
            let app = self.app.lock().await;
            !app.exit
        } {
            // Default poll timeout
            let poll_timeout = Duration::from_millis(50);

            // Only sleep if no events are waiting to be processed
            if !event::poll(Duration::ZERO)? {
                tokio::time::sleep(Duration::from_millis(25)).await;
            }

            if event::poll(poll_timeout)? {
                let mut app = self.app.lock().await;
                app.handle_events()?;
            }

            terminal.draw(|frame| {
                let mut app = futures::executor::block_on(self.app.lock());
                app.width = frame.area().width;
                self.ui.render(frame, &mut app);
            })?;

            let (should_load_more, initialize, submit_query) = {
                let mut app = self.app.lock().await;
                (
                    app.load_more_records(),
                    !app.initialized,
                    app.current_screen == Screen::SearchResults && app.loading,
                )
            };

            if initialize {
                App::init(self.app.clone());
            }

            if should_load_more {
                App::load_records(self.app.clone())
            }

            if submit_query {
                App::submit_query(self.app.clone());
            }
        }

        Self::clear_terminal(&mut terminal)?;

        Ok(())
    }
}
