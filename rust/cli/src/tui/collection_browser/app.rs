use crate::client::collection::{Collection, CollectionAPIError};
use crate::tui::collection_browser::query_editor::QueryEditor;
use chroma_types::{GetResponse, IncludeList, Metadata};
use ratatui::crossterm::event;
use ratatui::crossterm::event::{Event, KeyCode, KeyEvent, KeyModifiers};
use ratatui::widgets::TableState;
use std::collections::BTreeMap;
use std::error::Error;
use std::sync::Arc;
use tokio::spawn;
use tokio::sync::Mutex;

#[derive(Debug, PartialEq, Default)]
pub enum Screen {
    #[default]
    Main,
    Expand,
    SearchEditor,
    SearchResults,
}

#[derive(Debug)]
pub struct Record {
    pub id: String,
    pub document: Option<String>,
    pub metadata: Option<Metadata>,
}

#[derive(Debug, Default)]
pub struct App {
    pub collection: Collection,
    pub loading: bool,
    pub initialized: bool,
    pub error: Option<String>,
    pub current_screen: Screen,
    pub records: Vec<Record>,
    pub query_records: Vec<Record>,
    pub collection_size: u32,
    pub table_state: TableState,
    pub expand_scroll: u16,
    pub query_editor: QueryEditor,
    pub query_table_state: TableState,
    pub width: u16,
    pub exit: bool,
}

impl App {
    pub fn new(collection: Collection) -> Self {
        Self {
            collection,
            loading: true,
            initialized: false,
            error: None,
            current_screen: Screen::Main,
            records: vec![],
            query_records: vec![],
            collection_size: 0,
            table_state: TableState::default().with_selected(0),
            expand_scroll: 0,
            query_editor: QueryEditor::new(),
            query_table_state: TableState::default().with_selected(0),
            width: 10,
            exit: false,
        }
    }

    fn handle_main_events(&mut self, key: KeyEvent) -> Result<(), Box<dyn Error>> {
        match key.code {
            KeyCode::Char('q') | KeyCode::Esc => {
                self.exit = true;
            }
            KeyCode::Down => Self::next_row(&mut self.table_state, &self.records),
            KeyCode::Up => Self::previous_row(&mut self.table_state, &self.records),
            KeyCode::Left => Self::previous_column(&mut self.table_state),
            KeyCode::Right => Self::next_column(&mut self.table_state),
            KeyCode::Enter => {
                if self.table_state.selected_cell().is_some() {
                    self.current_screen = Screen::Expand;
                }
            }
            KeyCode::Char('s') => {
                let selected_row = self.table_state.selected();
                self.table_state = TableState::default().with_selected(selected_row.unwrap_or(0));
                self.current_screen = Screen::SearchEditor;
            }
            _ => {}
        }
        Ok(())
    }

    fn handle_expand_events(&mut self, key: KeyEvent) -> Result<(), Box<dyn Error>> {
        match key.code {
            KeyCode::Char('q') | KeyCode::Esc => {
                if self.query_records.is_empty() {
                    self.current_screen = Screen::Main;
                } else {
                    self.current_screen = Screen::SearchResults;
                }
                self.expand_scroll = 0;
            }
            KeyCode::Up => {
                if self.expand_scroll > 0 {
                    self.expand_scroll -= 1;
                }
            }
            KeyCode::Down => {
                let limit = self.get_scroll_limit();
                if self.expand_scroll < limit {
                    self.expand_scroll += 1;
                }
            }
            _ => {}
        }
        Ok(())
    }

    fn handle_query_editor_events(&mut self, key: KeyEvent) -> Result<(), Box<dyn Error>> {
        if key.modifiers.contains(KeyModifiers::SHIFT) && key.code == KeyCode::Char('C') {
            self.query_editor.clear_inputs();
            return Ok(());
        }

        match key.code {
            KeyCode::Esc => {
                self.query_records = vec![];
                self.query_table_state = TableState::default().with_selected(0);
                self.current_screen = Screen::Main;
            }
            KeyCode::Down | KeyCode::Tab => {
                self.query_editor.next_input();
            }
            KeyCode::Up | KeyCode::BackTab => {
                self.query_editor.prev_input();
            }
            KeyCode::Left => {
                self.query_editor.move_cursor_left();
            }
            KeyCode::Right => {
                self.query_editor.move_cursor_right();
            }
            KeyCode::Char(c) => {
                if c == ' ' {
                    self.query_editor.next_operator();
                }
                self.query_editor.handle_input(c);
            }
            KeyCode::Backspace => {
                self.query_editor.handle_input_delete();
            }
            KeyCode::Enter => {
                self.current_screen = Screen::SearchResults;
                self.loading = true;
            }
            _ => {}
        }
        Ok(())
    }

    fn handle_search_result_events(&mut self, key: KeyEvent) -> Result<(), Box<dyn Error>> {
        match key.code {
            KeyCode::Char('q') | KeyCode::Esc => {
                self.current_screen = Screen::Main;
                self.query_records = vec![];
            }
            KeyCode::Down => Self::next_row(&mut self.query_table_state, &self.query_records),
            KeyCode::Up => Self::previous_row(&mut self.query_table_state, &self.query_records),
            KeyCode::Left => Self::previous_column(&mut self.query_table_state),
            KeyCode::Right => Self::next_column(&mut self.query_table_state),
            KeyCode::Enter => {
                if self.query_table_state.selected_cell().is_some() {
                    self.current_screen = Screen::Expand;
                }
            }
            KeyCode::Char('s') => {
                self.current_screen = Screen::SearchEditor;
            }
            _ => {}
        }
        Ok(())
    }

    pub fn handle_events(&mut self) -> Result<(), Box<dyn Error>> {
        match event::read()? {
            Event::Key(key) => {
                // Process the current event
                match self.current_screen {
                    Screen::Main => self.handle_main_events(key)?,
                    Screen::Expand => self.handle_expand_events(key)?,
                    Screen::SearchEditor => self.handle_query_editor_events(key)?,
                    Screen::SearchResults => self.handle_search_result_events(key)?,
                };
            }
            // Handle paste events directly
            Event::Paste(text) => {
                if self.current_screen == Screen::SearchEditor {
                    self.query_editor.handle_paste(&text);
                }
            }
            // Ignore other events
            _ => {}
        }
        Ok(())
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

    async fn get_records_batch(
        collection: &Collection,
        offset: u32,
    ) -> Result<Vec<Record>, CollectionAPIError> {
        let limit = 100;

        let response = collection
            .get(
                None,
                None,
                None,
                Some(IncludeList::default_get()),
                Some(limit),
                Some(offset),
            )
            .await
            .map_err(|_| CollectionAPIError::Get(collection.name.clone()))?;

        Ok(Self::get_response_to_records(response))
    }

    pub fn init(app_arc: Arc<Mutex<Self>>) {
        let app_clone = Arc::clone(&app_arc);
        spawn(async move {
            let mut app = app_clone.lock().await;
            match app.collection.count().await {
                Ok(count) => {
                    app.collection_size = count;
                }
                Err(_) => {
                    app.error = Some(String::from("Failed to fetch collection size."));
                }
            }
            match Self::get_records_batch(&app.collection, 0).await {
                Ok(records) => {
                    app.records = records;
                    app.initialized = true;
                }
                Err(_) => {
                    app.error = Some(String::from("Failed to get records"));
                }
            }
            app.loading = false;
        });
    }

    pub fn load_records(app_arc: Arc<Mutex<Self>>) {
        let app_clone = Arc::clone(&app_arc);
        spawn(async move {
            let (collection, offset, collection_size, table_state) = {
                let mut app = app_clone.lock().await;
                app.loading = true;
                (
                    app.collection.clone(),
                    app.records.len() as u32,
                    app.collection_size,
                    app.table_state.clone(),
                )
            };

            let records = Self::get_records_batch(&collection, offset).await;
            let count = Self::update_collection_size(
                &collection,
                &table_state,
                offset as usize,
                collection_size as usize,
            )
            .await;

            let mut app = app_clone.lock().await;
            match count {
                Ok(count) => {
                    app.collection_size = count.unwrap_or(app.collection_size);
                }
                Err(_) => {
                    app.error = Some(String::from("Failed to fetch collection size."));
                }
            }
            match records {
                Ok(new_records) => {
                    app.records.extend(new_records);
                }
                Err(_) => {
                    app.error = Some(String::from("Failed to load collection records"));
                }
            }
            app.loading = false;
        });
    }

    async fn update_collection_size(
        collection: &Collection,
        table_state: &TableState,
        records_size: usize,
        collection_size: usize,
    ) -> Result<Option<u32>, CollectionAPIError> {
        let count_threshold = records_size * 98 / 100;
        if table_state.selected().unwrap_or(0) >= count_threshold && records_size == collection_size
        {
            return Ok(Some(collection.count().await.map_err(|_| {
                CollectionAPIError::Count(collection.name.clone())
            })?));
        }
        Ok(None)
    }

    pub fn load_more_records(&mut self) -> bool {
        if self.loading || self.error.is_some() {
            return false;
        }

        let threshold = self.records.len() * 3 / 4;
        if self.table_state.selected().unwrap_or(0) >= threshold
            && self.records.len() < self.collection_size as usize
        {
            return true;
        }

        false
    }

    pub fn submit_query(app_arc: Arc<Mutex<Self>>) {
        let app_clone = Arc::clone(&app_arc);
        spawn(async move {
            let (collection, ids, where_document, metadata) = {
                let mut app = app_clone.lock().await;
                app.loading = true;
                (
                    app.collection.clone(),
                    app.query_editor.parse_ids(),
                    app.query_editor.parse_where_document(),
                    app.query_editor.parse_metadata(),
                )
            };

            let request_metadata = metadata.as_deref();
            let request_where_document = where_document.as_deref();

            let response = collection
                .get(
                    ids,
                    request_metadata,
                    request_where_document,
                    Some(IncludeList::default_get()),
                    Some(100),
                    None,
                )
                .await
                .map_err(|_| CollectionAPIError::Get(collection.name.clone()));

            let mut app = app_clone.lock().await;
            match response {
                Ok(get_records) => {
                    app.query_records = Self::get_response_to_records(get_records);
                }
                Err(_) => {
                    app.error = Some(String::from("Failed to load collection records"));
                }
            }
            app.loading = false;
        });
    }

    // ======= Table Navigation ========

    pub fn next_row(table_state: &mut TableState, records: &[Record]) {
        let i = match table_state.selected() {
            Some(i) => {
                if i >= records.len() - 1 {
                    0
                } else {
                    i + 1
                }
            }
            None => 0,
        };
        table_state.select(Some(i));
    }

    pub fn previous_row(table_state: &mut TableState, records: &[Record]) {
        let i = match table_state.selected() {
            Some(i) => {
                if i == 0 {
                    records.len() - 1
                } else {
                    i - 1
                }
            }
            None => 0,
        };
        table_state.select(Some(i));
    }

    pub fn next_column(table_state: &mut TableState) {
        table_state.select_next_column();
    }

    pub fn previous_column(table_state: &mut TableState) {
        table_state.select_previous_column();
    }

    pub fn get_selected_record(&self) -> Option<(&Record, usize)> {
        let main_cell = self.table_state.selected_cell();
        if let Some(cell) = main_cell {
            let (row, column) = cell;
            return self.records.get(row).map(|record| (record, column));
        }

        let search_cell = self.query_table_state.selected_cell();
        if let Some(cell) = search_cell {
            let (row, column) = cell;
            return self.query_records.get(row).map(|record| (record, column));
        }

        None
    }

    pub fn get_record_content(record: &Record, column: usize) -> String {
        match column {
            0 => record.id.clone(),
            1 => record.document.clone().unwrap_or_default(),
            2 => {
                let metadata = record.metadata.clone().unwrap_or_default();
                let sorted: BTreeMap<_, _> = metadata.into_iter().collect();
                serde_json::to_string_pretty(&sorted).unwrap_or_else(|_| String::new())
            }
            _ => String::new(),
        }
    }

    fn get_scroll_limit(&self) -> u16 {
        let selected_record = self.get_selected_record();
        if selected_record.is_none() {
            return 0;
        }

        let (record, column) = selected_record.unwrap();
        let text = Self::get_record_content(record, column);

        Self::estimate_wrapped_lines(&text, self.width)
    }

    fn estimate_wrapped_lines(text: &str, width: u16) -> u16 {
        let mut line_count = 0;
        for raw_line in text.lines() {
            let mut current_line_len = 0;

            for word in raw_line.split_whitespace() {
                let word_len = word.chars().count();
                if current_line_len == 0 {
                    current_line_len = word_len;
                } else if current_line_len + 1 + word_len <= width as usize {
                    current_line_len += 1 + word_len; // 1 for space
                } else {
                    line_count += 1;
                    current_line_len = word_len;
                }
            }
        }

        line_count + 1
    }
}
