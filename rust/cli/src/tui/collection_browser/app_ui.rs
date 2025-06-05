use crate::tui::collection_browser::app_state::{AppState, ExpandContext};
use crate::tui::collection_browser::query_editor::{Mode, QueryEditor, QueryEditorState};
use crate::tui::collection_browser::table::{AppTable, CurrentCell};
use crate::tui::collection_browser::{RecordField, Screen};
use crate::ui_utils::{ColorLevel, Theme};
use ratatui::layout::Rect;
use ratatui::style::{self, Color, Style, Stylize};
use ratatui::text::{Line, Span, Text};
use ratatui::widgets::{Block, Borders, Padding, Paragraph, Wrap};
use ratatui::Frame;
use style::palette::tailwind;
use supports_color::{on, Stream};

pub struct ColorPalette {
    pub table_header_font: Color,
    pub table_header_background: Color,
    pub current_cell_font: Color,
    pub current_cell_background: Color,
    pub current_row_indicator: Color,
    pub form_active_field: Color,
    pub form_title: Color,
    pub error: Color,
}

impl ColorPalette {
    pub fn default(color_level: ColorLevel, theme: Theme) -> Self {
        match (color_level, theme) {
            (ColorLevel::Ansi256, Theme::Dark) => Self::ansi256_dark(),
            (ColorLevel::Ansi256, Theme::Light) => Self::ansi256_dark(),
            (ColorLevel::TrueColor, Theme::Dark) => Self::true_color_dark(),
            (ColorLevel::TrueColor, Theme::Light) => Self::true_color_light(),
        }
    }

    pub fn true_color_dark() -> Self {
        Self {
            table_header_font: tailwind::SLATE.c200,
            table_header_background: tailwind::BLUE.c900,
            current_cell_font: tailwind::SLATE.c900,
            current_cell_background: tailwind::BLUE.c500,
            current_row_indicator: tailwind::ORANGE.c400,
            form_active_field: tailwind::ORANGE.c400,
            form_title: tailwind::BLUE.c900,
            error: tailwind::RED.c500,
        }
    }

    pub fn true_color_light() -> Self {
        Self {
            table_header_font: tailwind::SLATE.c200,
            table_header_background: tailwind::BLUE.c900,
            current_cell_font: tailwind::SLATE.c900,
            current_cell_background: tailwind::BLUE.c500,
            current_row_indicator: tailwind::ORANGE.c400,
            form_active_field: tailwind::ORANGE.c400,
            form_title: tailwind::BLUE.c400,
            error: tailwind::RED.c500,
        }
    }

    pub fn ansi256_dark() -> Self {
        Self {
            table_header_font: Color::White,
            table_header_background: Color::Blue,
            current_cell_font: Color::White,
            current_cell_background: Color::Cyan,
            current_row_indicator: Color::LightRed,
            form_active_field: Color::Yellow,
            form_title: Color::Blue,
            error: Color::Red,
        }
    }
}

pub struct AppUI {
    palette: ColorPalette,
}

impl AppUI {
    pub fn new(theme: Theme) -> Self {
        let color_level = match on(Stream::Stdout) {
            Some(info) if info.has_16m => ColorLevel::TrueColor,
            Some(info) if info.has_256 => ColorLevel::Ansi256,
            _ => ColorLevel::Ansi256,
        };

        Self {
            palette: ColorPalette::default(color_level, theme.clone()),
        }
    }

    pub fn render(&mut self, frame: &mut Frame, app_state: &mut AppState) {
        let app_block = Block::default()
            .borders(Borders::ALL)
            .padding(Padding::new(1, 3, 1, 1))
            .title(self.title(frame, app_state).left_aligned())
            .title_bottom(self.instructions(app_state).centered());

        let inner_area = app_block.inner(frame.area());

        match app_state.screen {
            Screen::Main => self.render_main(frame, inner_area, app_state),
            Screen::Expand => self.render_expand(frame, inner_area, app_state),
            Screen::Search => self.render_search(frame, inner_area, app_state),
            Screen::SearchResult => self.render_search_result(frame, inner_area, app_state),
        }

        frame.render_widget(app_block, frame.area());
    }

    fn title(&self, frame: &mut Frame, app_state: &mut AppState) -> Line {
        let content = match app_state.screen {
            Screen::Main => {
                let visible_rows = frame.area().height.saturating_sub(8) as usize;
                self.main_title(app_state, visible_rows)
            }
            Screen::Expand => self.expand_title(
                &app_state.collection_name,
                AppTable::current_cell_content(&app_state.records_table_state, &app_state.records),
            ),
            Screen::Search => self.search_title(&app_state.collection_name),
            Screen::SearchResult => self.search_result_title(&app_state.collection_name),
        };

        let title = format!(" {} ", content);
        Line::from(Span::styled(title, Style::default().bold()))
    }

    fn instructions(&self, app_state: &mut AppState) -> Line {
        let padding = " ";
        let spacing = "  ";

        let content = if let Some(error) = &app_state.error {
            vec![("Error", error.as_str()), ("Esc", "Quit")]
        } else {
            match &app_state.screen {
                Screen::Main => self.main_instructions(),
                Screen::Expand => self.expand_instructions(),
                Screen::Search => self.search_instructions(&app_state.query_editor_state),
                Screen::SearchResult => self.search_result_instructions(),
            }
        };

        let instructions_text = content
            .iter()
            .map(|(key, text)| format!("({}) {}", key, text))
            .collect::<Vec<String>>()
            .join(spacing);

        let content = format!(" {}{} ", padding, instructions_text);

        let mut style = Style::default();

        if app_state.error.is_some() {
            style = style.fg(self.palette.error);
        }

        Line::from(Span::styled(content, style))
    }

    // ======= Main Screen ========

    fn main_title(&self, app_state: &AppState, max_visible_rows: usize) -> String {
        let title = format!(" Browsing Chroma Collection: {}", app_state.collection_name);

        if app_state.records.is_empty() {
            return title;
        }

        format!(
            "{} [{}-{} / {} ({} loaded)]",
            title,
            app_state.records_table_state.offset() + 1,
            max_visible_rows + app_state.records_table_state.offset() + 1,
            app_state.collection_size,
            app_state.records.len()
        )
    }

    fn main_instructions(&self) -> Vec<(&str, &str)> {
        vec![
            ("↑/↓", "Nav rows"),
            ("←/→", "Nav cols"),
            ("Enter", "Expand"),
            ("s", "Search"),
            ("Esc", "Quit"),
        ]
    }

    pub fn render_main(&self, frame: &mut Frame, area: Rect, app_state: &mut AppState) {
        let headers = vec!["ID", "Document", "Metadata"];
        let table = AppTable::new(headers, &app_state.records, &self.palette);
        frame.render_stateful_widget(table, area, &mut app_state.records_table_state);
    }

    // ======= Expand Screen ========

    fn expand_title(&self, collection_name: &str, current_cell: Option<CurrentCell>) -> String {
        if let Some(current_cell) = current_cell {
            let content = match &current_cell.record_field {
                RecordField::ID => String::from("Record ID"),
                RecordField::Document => format!("Record Document (ID: {})", current_cell.id),
                RecordField::Metadata => format!("Record Metadata (ID: {})", current_cell.id),
            };
            return format!(" Collection: {} | {} ", collection_name, content);
        }
        String::new()
    }

    fn expand_instructions(&self) -> Vec<(&str, &str)> {
        vec![("↑/↓", "Scroll"), ("←/→", "Nav cols"), ("Esc", "Back")]
    }

    // ======= Search Screen ========

    fn search_title(&self, collection_name: &str) -> String {
        format!(" Collection: {} | Search Records", collection_name)
    }

    fn search_instructions(&self, query_editor_state: &QueryEditorState) -> Vec<(&str, &str)> {
        match query_editor_state.mode {
            Mode::Normal => {
                vec![
                    ("↑/↓/←/→", "Nav"),
                    ("e", "Edit"),
                    ("c", "Clear"),
                    ("Space", "Toggle Operator"),
                    ("Enter", "Submit"),
                    ("Esc", "Back"),
                ]
            }
            Mode::Editing => {
                vec![("Esc", "Exit Edit Mode")]
            }
        }
    }

    fn render_expand(&self, frame: &mut Frame, area: Rect, app_state: &mut AppState) {
        let current_cell = match app_state.expand_context {
            ExpandContext::Main => {
                AppTable::current_cell_content(&app_state.records_table_state, &app_state.records)
            }
            ExpandContext::Query => AppTable::current_cell_content(
                &app_state.query_table_state,
                &app_state.query_records,
            ),
        };
        if let Some(current_cell) = current_cell {
            let paragraph = Paragraph::new(Text::from(current_cell.content))
                .style(Style::default())
                .wrap(Wrap { trim: true })
                .scroll((app_state.expand_scroll, 0));
            frame.render_widget(paragraph, area);
        }
    }

    fn render_search(&self, frame: &mut Frame, area: Rect, app_state: &mut AppState) {
        let query_editor = QueryEditor::new(&self.palette);
        query_editor.render(frame, area, &mut app_state.query_editor_state);
    }

    // ======= Search Result Screen ========

    fn search_result_title(&self, collection_name: &str) -> String {
        format!(" Collection: {} | Search Result", collection_name)
    }

    fn search_result_instructions(&self) -> Vec<(&str, &str)> {
        vec![
            ("↑/↓", "Nav rows"),
            ("←/→", "Nav cols"),
            ("Enter", "Expand"),
            ("s", "Search"),
            ("Esc", "Back"),
        ]
    }

    pub fn render_search_result(&self, frame: &mut Frame, area: Rect, app_state: &mut AppState) {
        if app_state.loading {
            return;
        }
        let headers = vec!["ID", "Document", "Metadata"];
        let table = AppTable::new(headers, &app_state.query_records, &self.palette);
        frame.render_stateful_widget(table, area, &mut app_state.query_table_state);
    }
}
