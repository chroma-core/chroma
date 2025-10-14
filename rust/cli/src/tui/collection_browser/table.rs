use crate::tui::collection_browser::app_ui::ColorPalette;
use crate::tui::collection_browser::{Record, RecordField};
use chroma_types::MetadataValue;
use ratatui::buffer::Buffer;
use ratatui::layout::{Constraint, Rect};
use ratatui::style::Style;
use ratatui::text::Text;
use ratatui::widgets::{Cell, HighlightSpacing, Row, StatefulWidget, Table, TableState};
use std::collections::BTreeMap;

pub struct CurrentCell {
    pub id: String,
    pub record_field: RecordField,
    pub content: String,
}

impl CurrentCell {
    pub fn new(id: String, record_field: RecordField, content: String) -> Self {
        Self {
            id,
            record_field,
            content,
        }
    }
}

pub struct AppTable<'a> {
    header: Vec<&'a str>,
    records: &'a [Record],
    header_style: Style,
    current_cell_style: Style,
    current_row_indicator_style: Style,
    widths: Vec<Constraint>,
}

impl<'a> AppTable<'a> {
    pub fn new(header: Vec<&'a str>, records: &'a [Record], palette: &'a ColorPalette) -> Self {
        AppTable {
            header,
            records,
            header_style: Style::default()
                .fg(palette.table_header_font)
                .bg(palette.table_header_background),
            current_cell_style: Style::default()
                .fg(palette.current_cell_font)
                .bg(palette.current_cell_background),
            current_row_indicator_style: Style::default().fg(palette.current_row_indicator),
            widths: vec![
                Constraint::Percentage(20),
                Constraint::Percentage(40),
                Constraint::Percentage(40),
            ],
        }
    }

    fn record_row_contents(record: &Record) -> Vec<String> {
        let id = record.id.clone();

        let document = record
            .document
            .clone()
            .unwrap_or_default()
            .replace("\n", "\\n");

        let metadata = match record.metadata.clone() {
            Some(metadata) => {
                let sorted: BTreeMap<String, MetadataValue> = metadata.into_iter().collect();
                serde_json::to_string(&sorted).unwrap_or_default()
            }
            None => String::new(),
        };

        vec![id, document, metadata]
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
        // Only select next column if current column is less than max column index (2)
        if let Some((_, col)) = table_state.selected_cell() {
            if col < 2 {
                table_state.select_next_column();
            }
        } else {
            table_state.select_next_column();
        }
    }

    pub fn previous_column(table_state: &mut TableState) {
        table_state.select_previous_column();
    }

    pub fn current_cell_content(
        table_state: &TableState,
        records: &[Record],
    ) -> Option<CurrentCell> {
        if let Some((row, col)) = table_state.selected_cell() {
            let record = &records[row];
            let id = record.id.clone();
            match col {
                0 => Some(CurrentCell::new(id.clone(), RecordField::ID, id)),
                1 => Some(CurrentCell::new(
                    id,
                    RecordField::Document,
                    record.document.clone().unwrap_or_default(),
                )),
                2 => {
                    let content = match &record.metadata {
                        Some(metadata) => {
                            serde_json::to_string_pretty(&metadata).unwrap_or_default()
                        }
                        None => String::new(),
                    };
                    Some(CurrentCell::new(id, RecordField::Metadata, content))
                }
                _ => None,
            }
        } else {
            None
        }
    }
}

impl StatefulWidget for AppTable<'_> {
    type State = TableState;

    fn render(self, area: Rect, buf: &mut Buffer, state: &mut Self::State) {
        let header = self
            .header
            .into_iter()
            .map(Cell::from)
            .collect::<Row>()
            .style(self.header_style)
            .height(1);

        let rows = self.records.iter().map(|record| {
            Self::record_row_contents(record)
                .into_iter()
                .map(|content| Cell::from(Text::from(content)))
                .collect::<Row>()
                .height(1)
        });

        let bar = " █ ";
        let indicator = vec![bar.into()];

        let table = Table::new(rows, self.widths)
            .header(header)
            .column_spacing(2)
            .highlight_symbol(Text::from(indicator).style(self.current_row_indicator_style))
            .cell_highlight_style(self.current_cell_style)
            .highlight_spacing(HighlightSpacing::Always);

        table.render(area, buf, state);
    }
}
