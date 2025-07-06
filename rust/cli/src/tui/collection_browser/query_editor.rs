use crate::tui::collection_browser::app_ui::ColorPalette;
use crate::tui::collection_browser::input::{InputBox, ToggleButton, ToggleState};
use crate::utils::parse_value;
use ratatui::layout::{Constraint, Direction, Layout, Rect};
use ratatui::style::Style;
use ratatui::text::Text;
use ratatui::widgets::Paragraph;
use ratatui::Frame;
use serde_json::{Map, Value};
use tui_input::Input;

#[derive(Debug, Clone)]
pub enum MetadataOperator {
    LessThan,
    LessThanOrEqual,
    Equal,
    NotEqual,
    GreaterThanOrEqual,
    GreaterThan,
    In,
    NotIn,
}

impl MetadataOperator {
    pub fn for_query(&self) -> String {
        match self {
            MetadataOperator::Equal => "$eq".to_string(),
            MetadataOperator::NotEqual => "$ne".to_string(),
            MetadataOperator::GreaterThan => "$gt".to_string(),
            MetadataOperator::GreaterThanOrEqual => "$gte".to_string(),
            MetadataOperator::LessThan => "$lt".to_string(),
            MetadataOperator::LessThanOrEqual => "$lte".to_string(),
            MetadataOperator::In => "$in".to_string(),
            MetadataOperator::NotIn => "$nin".to_string(),
        }
    }
}

impl std::fmt::Display for MetadataOperator {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        let s = match self {
            MetadataOperator::LessThan => "<",
            MetadataOperator::LessThanOrEqual => "<=",
            MetadataOperator::Equal => "=",
            MetadataOperator::NotEqual => "!=",
            MetadataOperator::GreaterThanOrEqual => ">=",
            MetadataOperator::GreaterThan => ">",
            MetadataOperator::In => "$in",
            MetadataOperator::NotIn => "$nin",
        };
        write!(f, "{}", s)
    }
}

static OPERATORS: [MetadataOperator; 8] = [
    MetadataOperator::Equal,
    MetadataOperator::NotEqual,
    MetadataOperator::LessThan,
    MetadataOperator::LessThanOrEqual,
    MetadataOperator::GreaterThan,
    MetadataOperator::GreaterThanOrEqual,
    MetadataOperator::In,
    MetadataOperator::NotIn,
];

#[derive(Debug, Clone)]
pub enum InputType {
    IDs(Input),
    WhereDocument(Input),
    MetadataKey(Input),
    MetadataValue(Input),
    MetadataOperator(ToggleState),
}

impl std::fmt::Display for InputType {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        let s = match self {
            InputType::IDs(_) => "IDs",
            InputType::WhereDocument(_) => "Where Document",
            InputType::MetadataKey(_) => "Key",
            InputType::MetadataValue(_) => "Value",
            InputType::MetadataOperator(_) => "Operator",
        };
        write!(f, "{}", s)
    }
}

impl InputType {
    pub fn as_input_mut(&mut self) -> Option<&mut Input> {
        match self {
            InputType::IDs(i)
            | InputType::WhereDocument(i)
            | InputType::MetadataKey(i)
            | InputType::MetadataValue(i) => Some(i),
            _ => None,
        }
    }

    pub fn as_toggle_mut(&mut self) -> Option<&mut ToggleState> {
        match self {
            InputType::MetadataOperator(toggle_state) => Some(toggle_state),
            _ => None,
        }
    }

    pub fn reset(&mut self) {
        match self {
            InputType::MetadataOperator(toggle_state) => {
                toggle_state.selected = 0;
            }
            InputType::WhereDocument(i)
            | InputType::MetadataKey(i)
            | InputType::MetadataValue(i)
            | InputType::IDs(i) => {
                let _ = i.value_and_reset();
            }
        }
    }
}

#[derive(Debug, Eq, PartialEq)]
pub enum Mode {
    Normal,
    Editing,
}

pub struct QueryEditorState {
    pub inputs: Vec<InputType>,
    pub active: usize,
    pub mode: Mode,
}

impl Default for QueryEditorState {
    fn default() -> Self {
        Self {
            inputs: vec![
                InputType::IDs(Input::default()),
                InputType::WhereDocument(Input::default()),
                InputType::MetadataKey(Input::default()),
                InputType::MetadataOperator(ToggleState::new()),
                InputType::MetadataValue(Input::default()),
            ],
            active: 0,
            mode: Mode::Normal,
        }
    }
}

impl QueryEditorState {
    pub fn current_field(&mut self) -> &mut InputType {
        &mut self.inputs[self.active]
    }

    pub fn next_field(&mut self) {
        self.active += 1;
        if self.active >= self.inputs.len() {
            self.active = 0;
        }
    }

    pub fn previous_field(&mut self) {
        if self.active == 0 {
            self.active = self.inputs.len() - 1;
            return;
        }
        self.active -= 1;
    }

    pub fn toggle_operator(&mut self) {
        let field = self.current_field();
        if let Some(toggle_state) = field.as_toggle_mut() {
            toggle_state.selected += 1;
            if toggle_state.selected >= OPERATORS.len() {
                toggle_state.selected = 0;
            }
        }
    }

    pub fn reset(&mut self) {
        self.inputs.iter_mut().for_each(|i| i.reset());
    }

    pub fn ids(&self) -> Option<Vec<String>> {
        let ids_input = self.inputs.iter().find_map(|i| {
            if let InputType::IDs(input_state) = i {
                Some(input_state.value())
            } else {
                None
            }
        })?;

        if ids_input.is_empty() {
            return None;
        }
        let ids: Vec<String> = ids_input
            .split(',')
            .map(|id| id.trim().to_string())
            .filter(|id| !id.is_empty())
            .collect();
        if ids.is_empty() {
            return None;
        }
        Some(ids)
    }

    pub fn where_document(&self) -> Option<String> {
        let where_document_input = self.inputs.iter().find_map(|i| {
            if let InputType::WhereDocument(input_state) = i {
                Some(input_state.value())
            } else {
                None
            }
        })?;

        if where_document_input.is_empty() {
            return None;
        }
        let mut map = Map::new();
        map.insert(
            "$contains".to_string(),
            Value::String(where_document_input.trim().to_string()),
        );
        Some(serde_json::to_string(&Value::Object(map)).unwrap_or_default())
    }

    pub fn metadata(&self) -> Option<String> {
        let metadata_key_input = self.inputs.iter().find_map(|i| {
            if let InputType::MetadataKey(input_state) = i {
                Some(input_state.value())
            } else {
                None
            }
        })?;

        let metadata_value_input = self.inputs.iter().find_map(|i| {
            if let InputType::MetadataValue(input_state) = i {
                Some(input_state.value())
            } else {
                None
            }
        })?;

        if metadata_key_input.is_empty() || metadata_value_input.is_empty() {
            return None;
        }

        let metadata_operator = self.inputs.iter().find_map(|i| {
            if let InputType::MetadataOperator(input_state) = i {
                Some(OPERATORS[input_state.selected].clone())
            } else {
                None
            }
        })?;

        let mut operator_map = Map::new();
        operator_map.insert(
            metadata_operator.for_query(),
            // Value::String(metadata_value_input.trim().to_string()),
            parse_value(metadata_value_input.trim()),
        );

        let mut map = Map::new();
        map.insert(
            metadata_key_input.trim().to_string(),
            Value::Object(operator_map),
        );
        Some(serde_json::to_string(&Value::Object(map)).unwrap_or_default())
    }
}

pub struct QueryEditor {
    active_style: Style,
    title_style: Style,
}

impl QueryEditor {
    pub fn new(palette: &ColorPalette) -> Self {
        Self {
            active_style: Style::default().fg(palette.form_active_field),
            title_style: Style::default().bg(palette.form_title),
        }
    }

    pub fn render(self, frame: &mut Frame, area: Rect, state: &mut QueryEditorState) {
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
                Constraint::Length(1), // Padding
                Constraint::Length(3), // Metadata input
            ])
            .split(area);

        let labels = ["IDs", "Where Document", "Metadata"];
        let label_chunks = [chunks[0], chunks[3], chunks[6]];
        labels.iter().enumerate().for_each(|(i, label)| {
            let content = format!(" {}", label);
            let p = Paragraph::new(Text::from(content)).style(self.title_style);
            frame.render_widget(p, label_chunks[i]);
        });

        let metadata_chunks = Layout::default()
            .direction(Direction::Horizontal)
            .constraints([
                Constraint::Percentage(30),
                Constraint::Length(10),
                Constraint::Min(10),
            ])
            .split(chunks[8]);

        let mut input_chunks = vec![chunks[1], chunks[4]];
        input_chunks.extend(metadata_chunks.to_vec());

        input_chunks.into_iter().enumerate().for_each(|(i, chunk)| {
            let is_active = state.active == i;
            let input = &mut state.inputs[i];

            let title = match input {
                InputType::MetadataKey(_) | InputType::MetadataValue(_) => Some(input.to_string()),
                _ => None,
            };

            if let Some(input_state) = input.as_input_mut() {
                let mut input_box = InputBox::new(self.active_style);

                if let Some(title) = title {
                    input_box = input_box.title(title);
                }

                if is_active {
                    input_box = input_box.active();
                    if state.mode == Mode::Editing {
                        let width = chunk.width.saturating_sub(3) as usize;
                        let scroll = input_state.visual_scroll(width);
                        let cursor_x = input_state.visual_cursor().saturating_sub(scroll) + 1;
                        frame.set_cursor_position((chunk.x + cursor_x as u16, chunk.y + 1))
                    }
                }
                frame.render_stateful_widget(input_box, chunk, input_state);
            }

            if let Some(toggle_state) = input.as_toggle_mut() {
                let mut toggle_button = ToggleButton::new(&OPERATORS, self.active_style);
                if is_active {
                    toggle_button = toggle_button.active()
                }
                frame.render_stateful_widget(toggle_button, chunk, toggle_state);
            }
        });
    }
}
