use crate::tui::collection_browser::app::{App, Record, Screen};
use crate::tui::collection_browser::query_editor::{Input, QueryEditor};
use chroma_types::Metadata;
use ratatui::layout::{Alignment, Constraint, Direction, Layout, Rect};
use ratatui::style::{Color, Style, Stylize};
use ratatui::text::{Line, Span, Text};
use ratatui::widgets::{
    Block, Borders, Cell, HighlightSpacing, Padding, Paragraph, Row, Table, TableState, Wrap,
};
use ratatui::Frame;
use std::collections::BTreeMap;
use std::error::Error;
use std::iter;
use std::time::{Duration, Instant};
use throbber_widgets_tui::{Throbber, ThrobberState};

#[derive(Debug)]
pub struct UserInput {
    field: String,
    cursor_position: usize,
    active: bool,
}

impl UserInput {
    pub fn new(field: String, cursor_position: usize, active: bool) -> Self {
        Self {
            field,
            cursor_position,
            active,
        }
    }

    pub fn text(&mut self) -> Text {
        if !self.active {
            return Text::from(Line::from(Span::raw(self.field.clone())));
        }

        let (before, after) = self.field.split_at(self.cursor_position);
        let mut spans: Vec<Span> = Vec::new();
        if !before.is_empty() {
            spans.push(Span::raw(before));
        }
        if !after.is_empty() {
            spans.push(Span::styled(
                &after[0..1],
                Style::default().fg(Color::Black).bg(Color::White),
            ));
            if after.len() > 1 {
                spans.push(Span::raw(&after[1..]));
            }
        } else {
            spans.push(Span::styled(" ", Style::default().bg(Color::White)));
        }
        Text::from(Line::from(spans))
    }
}

#[derive(Debug)]
pub struct UI {
    throbber_state: ThrobberState,
    last_tick: Instant,
    tick_rate: Duration,
}

impl UI {
    pub fn new() -> Self {
        Self {
            throbber_state: ThrobberState::default(),
            last_tick: Instant::now(),
            tick_rate: Duration::from_millis(100),
        }
    }

    pub fn tick(&mut self) {
        let now = Instant::now();
        if now.duration_since(self.last_tick) >= self.tick_rate {
            self.throbber_state.calc_next();
            self.last_tick = now;
        }
    }

    pub fn render(&mut self, frame: &mut Frame, app: &mut App) {
        let mut app_block = Block::default()
            .borders(Borders::ALL)
            .padding(Padding::new(1, 3, 1, 1))
            .title(self.title(app, frame).left_aligned());

        if !app.loading {
            app_block = app_block.title_bottom(
                self.instructions(&app.current_screen, &app.error)
                    .centered(),
            );
        }

        let inner_area = app_block.inner(frame.area());

        frame.render_widget(app_block, frame.area());

        match app.current_screen {
            Screen::Main => {
                if app.initialized {
                    self.render_table(frame, inner_area, &app.records, &mut app.table_state)
                }
            }
            Screen::Expand => self.render_expand_view(frame, inner_area, app),
            Screen::SearchEditor => self.render_query_editor(frame, inner_area, &app.query_editor),
            Screen::SearchResults => {
                if !app.loading {
                    self.render_table(
                        frame,
                        inner_area,
                        &app.query_records,
                        &mut app.query_table_state,
                    )
                }
            }
        }

        if app.loading {
            self.loader(frame, frame.area())
        }
    }

    fn title(&self, app: &App, frame: &Frame) -> Line {
        let title_text = match app.current_screen {
            Screen::Main => self.main_title(app, frame),
            Screen::Expand => self.expand_view_title(app),
            Screen::SearchEditor => {
                format!("Get Query Editor (collection: {})", app.collection.name)
            }
            Screen::SearchResults => {
                format!("Search results (collection: {})", app.collection.name)
            }
        };
        Line::from(Span::styled(
            format!(" {} ", title_text),
            Style::default().bold(),
        ))
    }

    fn instructions(&self, current_screen: &Screen, error: &Option<String>) -> Line {
        let padding = " ";
        let spacing = "  ";

        if let Some(error) = error {
            let message = format!("{}{}{}q (Quit){}", padding, error, spacing, padding);
            return Line::from(Span::styled(
                message,
                Style::default().bold().fg(Color::Red),
            ));
        }

        let instruction_texts = match current_screen {
            Screen::Main => self.main_instructions(),
            Screen::Expand => self.expand_view_instructions(),
            Screen::SearchEditor => self.query_editor_instructions(),
            Screen::SearchResults => self.search_results_instructions(),
        };

        let spans = instruction_texts
            .iter()
            .map(|(key, text)| {
                vec![
                    Span::styled(key.to_string(), Style::default().fg(Color::Cyan)),
                    Span::raw(format!(" {}{}", text, spacing)),
                ]
            })
            .collect::<Vec<Vec<Span<'_>>>>()
            .into_iter()
            .flatten()
            .collect::<Vec<Span>>();

        Line::from(
            iter::once(Span::raw(spacing))
                .chain(spans)
                .collect::<Vec<Span>>(),
        )
    }

    fn metadata_inline_json(metadata: Option<Metadata>) -> Result<String, Box<dyn Error>> {
        match metadata {
            Some(metadata) => {
                let sorted: BTreeMap<_, _> = metadata.into_iter().collect();
                Ok(serde_json::to_string(&sorted)?)
            }
            None => Ok(String::new()),
        }
    }

    fn loader(&mut self, frame: &mut Frame, area: Rect) {
        self.tick();

        let vertical_chunks = Layout::default()
            .direction(Direction::Vertical)
            .constraints([
                Constraint::Min(1),    // content
                Constraint::Length(1), // throbber row
            ])
            .split(area);

        let horizontal_chunks = Layout::default()
            .direction(Direction::Horizontal)
            .constraints([
                Constraint::Min(1),     // left padding
                Constraint::Length(1),  // center
                Constraint::Length(15), // center
                Constraint::Min(1),     // right padding
            ])
            .split(vertical_chunks[1]); // apply to the bottom row

        frame.render_widget(Span::raw(" "), horizontal_chunks[1]);

        let throbber = Throbber::default().label("Loading... ");
        frame.render_stateful_widget(throbber, horizontal_chunks[2], &mut self.throbber_state);
    }

    // ===== Main Screen =====

    fn main_title(&self, app: &App, frame: &Frame) -> String {
        let max_visible_rows = frame.area().height.saturating_sub(6) as usize;

        let title = format!("Browsing collection: {}", app.collection.name);

        if app.records.is_empty() {
            return title;
        }

        format!(
            "{} [{}-{} / {} ({} loaded)]",
            title,
            app.table_state.offset() + 1,
            max_visible_rows + app.table_state.offset() + 1,
            app.collection_size,
            app.records.len()
        )
    }

    fn main_instructions(&self) -> Vec<(&str, &str)> {
        vec![
            ("↑/↓", "Nav rows"),
            ("←/→", "Nav cols"),
            ("Enter", "Expand"),
            ("s", "Search"),
            ("q", "Quit"),
        ]
    }

    fn render_table(
        &mut self,
        frame: &mut Frame,
        area: Rect,
        records: &[Record],
        table_state: &mut TableState,
    ) {
        if records.is_empty() {
            let empty_message = Paragraph::new("No records found").centered();
            frame.render_widget(empty_message, area);
            return;
        }

        let selected_cell_style = Style::default().fg(Color::Black).bg(Color::Cyan);
        let header_style = Style::default().fg(Color::Yellow).bold();

        let header = ["ID", "Document", "Metadata"]
            .into_iter()
            .map(Cell::from)
            .collect::<Row>()
            .style(header_style)
            .height(1);

        let rows = records.iter().map(|record| {
            let contents = [
                record.id.clone(),
                record
                    .document
                    .clone()
                    .unwrap_or_default()
                    .replace("\n", "\\n"),
                Self::metadata_inline_json(record.metadata.clone()).unwrap_or_default(),
            ];

            contents
                .iter()
                .map(|content| Cell::from(Text::from(content.to_string())))
                .collect::<Row>()
                .height(1)
        });

        let bar = " █ ";
        let table = Table::new(
            rows,
            [
                Constraint::Percentage(20),
                Constraint::Percentage(40),
                Constraint::Percentage(40),
            ],
        )
        .header(header)
        .column_spacing(2)
        .cell_highlight_style(selected_cell_style)
        .highlight_symbol(Text::from(vec![bar.into()]).style(Style::default().fg(Color::LightRed)))
        .highlight_spacing(HighlightSpacing::Always);
        frame.render_stateful_widget(table, area, table_state);
    }

    // ===== Expand Screen =====

    fn expand_view_title(&self, app: &App) -> String {
        if let Some((record, column)) = app.get_selected_record() {
            return match column {
                0 => format!("Collection: {}, Record ID", app.collection.name),
                1 => format!(
                    "Collection: {}, Record Document {}",
                    app.collection.name, record.id
                ),
                2 => format!(
                    "Collection: {}, Record Metadata {}",
                    app.collection.name, record.id
                ),
                _ => String::new(),
            };
        }
        String::from("")
    }

    fn expand_view_instructions(&self) -> Vec<(&str, &str)> {
        vec![("↑/↓", "Scroll"), ("q/Esc", "Quit")]
    }

    fn render_expand_view(&mut self, frame: &mut Frame, area: Rect, app: &App) {
        if let Some((record, column)) = app.get_selected_record() {
            let content = App::get_record_content(record, column);
            frame.render_widget(
                Paragraph::new(content)
                    .wrap(Wrap { trim: true })
                    .scroll((app.expand_scroll, 0)),
                area,
            );
            return;
        }

        frame.render_widget(Paragraph::new(""), area);
    }

    // ===== Query Editor =====

    fn query_editor_instructions(&self) -> Vec<(&str, &str)> {
        vec![
            ("↑/↓/←/→", "Nav"),
            ("Shift+C", "Clear"),
            ("Enter", "Submit"),
            ("Esc", "Quit"),
        ]
    }

    fn render_query_editor(&mut self, frame: &mut Frame, area: Rect, editor: &QueryEditor) {
        let chunks = Layout::default()
            .direction(Direction::Vertical)
            .constraints([
                Constraint::Length(1), // IDs label
                Constraint::Length(3), // IDs input
                Constraint::Length(1), // Padding
                Constraint::Length(1), // Where Document label
                Constraint::Length(3), // Where Document input
                Constraint::Length(1), // Padding
                Constraint::Length(1), // Metadata label
                Constraint::Length(3), // Metadata input
            ])
            .split(area);

        let current: Input = editor
            .inputs
            .get(editor.current_input)
            .unwrap_or(&Input::IDs)
            .clone();

        // IDs label
        let ids_label = Paragraph::new("IDs (comma separated)")
            .style(Style::default().fg(Color::Yellow))
            .alignment(Alignment::Left);
        frame.render_widget(ids_label, chunks[0]);

        // IDs input
        let mut input_text = UserInput::new(
            editor.ids.clone(),
            editor.cursor_position,
            current == Input::IDs,
        );

        let ids_input = Paragraph::new(input_text.text()).block(
            Block::default()
                .borders(Borders::ALL)
                .border_style(if current == Input::IDs {
                    Style::default().fg(Color::Cyan)
                } else {
                    Style::default()
                }),
        );
        frame.render_widget(ids_input, chunks[1]);

        // Where Document label
        let where_label = Paragraph::new("Where Document")
            .style(Style::default().fg(Color::Yellow))
            .alignment(Alignment::Left);
        frame.render_widget(where_label, chunks[3]);

        // Where Document input
        let mut where_doc_text = UserInput::new(
            editor.where_document.clone(),
            editor.cursor_position,
            current == Input::WhereDocument,
        );

        let where_input = Paragraph::new(where_doc_text.text()).block(
            Block::default().borders(Borders::ALL).border_style(
                if current == Input::WhereDocument {
                    Style::default().fg(Color::Cyan)
                } else {
                    Style::default()
                },
            ),
        );
        frame.render_widget(where_input, chunks[4]);

        // Metadata label
        let metadata_label = Paragraph::new("Metadata")
            .style(Style::default().fg(Color::Yellow))
            .alignment(Alignment::Left);
        frame.render_widget(metadata_label, chunks[6]);

        // Metadata inputs
        let metadata_chunks = Layout::default()
            .direction(Direction::Horizontal)
            .constraints([
                Constraint::Percentage(30),
                Constraint::Length(10),
                Constraint::Min(10),
            ])
            .split(chunks[7]);

        // Key input
        let mut metadata_key_text = UserInput::new(
            editor.metadata_key.clone(),
            editor.cursor_position,
            current == Input::MetadataKey,
        );

        let key_input = Paragraph::new(metadata_key_text.text()).block(
            Block::default()
                .borders(Borders::ALL)
                .border_style(if current == Input::MetadataKey {
                    Style::default().fg(Color::Cyan)
                } else {
                    Style::default()
                })
                .title(String::from(" Key ")),
        );
        frame.render_widget(key_input, metadata_chunks[0]);

        // Operator
        let operator = Paragraph::new(Text::from(editor.operators[editor.operator].to_string()))
            .alignment(Alignment::Center)
            .block(Block::default().borders(Borders::ALL).border_style(
                if current == Input::MetadataOperator {
                    Style::default().fg(Color::Cyan)
                } else {
                    Style::default()
                },
            ));
        frame.render_widget(operator, metadata_chunks[1]);

        // Value input
        let mut metadata_value_text = UserInput::new(
            editor.metadata_value.clone(),
            editor.cursor_position,
            current == Input::MetadataValue,
        );

        let value_input = Paragraph::new(metadata_value_text.text()).block(
            Block::default()
                .borders(Borders::ALL)
                .border_style(if current == Input::MetadataValue {
                    Style::default().fg(Color::Cyan)
                } else {
                    Style::default()
                })
                .title(String::from(" Value ")),
        );
        frame.render_widget(value_input, metadata_chunks[2]);
    }

    // Search results

    fn search_results_instructions(&self) -> Vec<(&str, &str)> {
        vec![
            ("↑/↓", "Nav rows"),
            ("←/→", "Nav cols"),
            ("Enter", "Expand"),
            ("s", "Search"),
            ("q/Esc", "Back"),
        ]
    }
}
