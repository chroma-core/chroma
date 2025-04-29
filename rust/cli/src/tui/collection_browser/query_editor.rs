use serde_json::{Map, Value};
use std::fmt;

#[derive(Debug, PartialEq, Clone)]
pub enum Operator {
    Equal,
    NotEqual,
    GreaterThan,
    GreaterThanOrEqual,
    LessThan,
    LessThanOrEqual,
    In,
    NotIn,
}

impl Operator {
    pub fn for_query(&self) -> String {
        match self {
            Operator::Equal => "$eq".to_string(),
            Operator::NotEqual => "$ne".to_string(),
            Operator::GreaterThan => "$gt".to_string(),
            Operator::GreaterThanOrEqual => "$gte".to_string(),
            Operator::LessThan => "$lt".to_string(),
            Operator::LessThanOrEqual => "$lte".to_string(),
            Operator::In => "$in".to_string(),
            Operator::NotIn => "$nin".to_string(),
        }
    }
}

impl fmt::Display for Operator {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        let op_str = match self {
            Operator::Equal => "=",
            Operator::NotEqual => "!=",
            Operator::GreaterThan => ">",
            Operator::GreaterThanOrEqual => ">=",
            Operator::LessThan => "<",
            Operator::LessThanOrEqual => "<=",
            Operator::In => "in",
            Operator::NotIn => "not in",
        };
        write!(f, "{}", op_str)
    }
}

#[derive(Debug, PartialEq, Clone)]
pub enum Input {
    IDs,
    WhereDocument,
    MetadataKey,
    MetadataOperator,
    MetadataValue,
}

#[derive(Debug, Default)]
pub struct QueryEditor {
    pub operator: usize,
    pub operators: Vec<Operator>,
    pub current_input: usize,
    pub inputs: Vec<Input>,
    pub ids: String,
    pub where_document: String,
    pub metadata_key: String,
    pub metadata_value: String,
    pub cursor_position: usize,
}

impl QueryEditor {
    pub fn new() -> QueryEditor {
        QueryEditor {
            operator: 0,
            operators: vec![
                Operator::Equal,
                Operator::NotEqual,
                Operator::GreaterThan,
                Operator::GreaterThanOrEqual,
                Operator::LessThan,
                Operator::LessThanOrEqual,
                Operator::In,
                Operator::NotIn,
            ],
            current_input: 0,
            inputs: vec![
                Input::IDs,
                Input::WhereDocument,
                Input::MetadataKey,
                Input::MetadataOperator,
                Input::MetadataValue,
            ],
            ids: String::new(),
            where_document: String::new(),
            metadata_key: String::new(),
            metadata_value: String::new(),
            cursor_position: 0,
        }
    }

    pub fn next_input(&mut self) {
        self.current_input += 1;
        if self.current_input >= self.inputs.len() {
            self.current_input = 0;
        }
        self.cursor_position = self.get_current_input_value().len();
    }

    pub fn prev_input(&mut self) {
        if self.current_input == 0 {
            self.current_input = self.inputs.len() - 1;
        } else {
            self.current_input -= 1;
        }
        self.cursor_position = self.get_current_input_value().len();
    }

    pub fn next_operator(&mut self) {
        if self.inputs[self.current_input] != Input::MetadataOperator {
            return;
        }
        self.operator += 1;
        if self.operator >= self.operators.len() {
            self.operator = 0;
        }
    }

    fn get_current_input_value(&self) -> &String {
        match self.inputs[self.current_input] {
            Input::IDs => &self.ids,
            Input::WhereDocument => &self.where_document,
            Input::MetadataKey => &self.metadata_key,
            Input::MetadataValue => &self.metadata_value,
            _ => &self.ids, // Fallback
        }
    }

    fn get_current_input_value_mut(&mut self) -> &mut String {
        match self.inputs[self.current_input] {
            Input::IDs => &mut self.ids,
            Input::WhereDocument => &mut self.where_document,
            Input::MetadataKey => &mut self.metadata_key,
            Input::MetadataValue => &mut self.metadata_value,
            _ => &mut self.ids, // Fallback
        }
    }

    pub fn handle_input(&mut self, c: char) {
        if self.inputs[self.current_input] == Input::MetadataOperator {
            return;
        }

        let current_len = self.get_current_input_value().len();
        let cursor_pos = self.cursor_position;

        let input = self.get_current_input_value_mut();
        if cursor_pos == current_len {
            input.push(c);
        } else {
            input.insert(cursor_pos, c);
        }
        self.cursor_position += 1;
    }

    pub fn handle_paste(&mut self, text: &str) {
        if self.inputs[self.current_input] == Input::MetadataOperator {
            return;
        }

        let current_len = self.get_current_input_value().len();
        let cursor_pos = self.cursor_position;

        let input = self.get_current_input_value_mut();
        if cursor_pos == current_len {
            input.push_str(text);
        } else {
            // Insert character by character to maintain proper cursor position
            for (i, c) in text.chars().enumerate() {
                input.insert(cursor_pos + i, c);
            }
        }
        self.cursor_position += text.chars().count();
    }

    pub fn handle_input_delete(&mut self) {
        if self.inputs[self.current_input] == Input::MetadataOperator {
            return;
        }

        let current_len = self.get_current_input_value().len();
        let cursor_pos = self.cursor_position;

        if !current_len.eq(&0) && cursor_pos > 0 {
            let input = self.get_current_input_value_mut();
            input.remove(cursor_pos - 1);
            self.cursor_position -= 1;
        }
    }

    pub fn move_cursor_left(&mut self) {
        if self.cursor_position > 0 {
            self.cursor_position -= 1;
        } else {
            self.prev_input();
        }
    }

    pub fn move_cursor_right(&mut self) {
        let max_pos = self.get_current_input_value().len();
        if self.cursor_position < max_pos {
            self.cursor_position += 1;
        } else {
            self.next_input();
        }
    }

    pub fn clear_inputs(&mut self) {
        self.ids.clear();
        self.where_document.clear();
        self.metadata_key.clear();
        self.metadata_value.clear();
        self.cursor_position = 0;
        self.operator = 0;
    }

    fn list_from_string(s: &str) -> Vec<String> {
        if s.is_empty() {
            return vec![];
        }
        s.split(",").map(|s| s.trim().to_string()).collect()
    }

    pub fn parse_ids(&mut self) -> Option<Vec<String>> {
        let ids: Vec<String> = Self::list_from_string(&self.ids);
        if ids.is_empty() {
            None
        } else {
            Some(ids)
        }
    }

    pub fn parse_where_document(&mut self) -> Option<String> {
        let where_document = self.where_document.trim().to_string();
        if where_document.is_empty() {
            None
        } else {
            let mut query_obj = Map::new();
            query_obj.insert("$contains".to_string(), Value::String(where_document));
            Some(serde_json::to_string(&Value::Object(query_obj)).unwrap_or_default())
        }
    }

    pub fn parse_metadata(&mut self) -> Option<String> {
        let key = self.metadata_key.trim().to_string();
        if key.is_empty() {
            return None;
        }

        let value = self.metadata_value.trim().to_string();
        if value.is_empty() {
            return None;
        }

        let operator = self.operators[self.operator].clone();

        let mut operator_value_obj = Map::new();
        let json_value = match operator {
            Operator::In | Operator::NotIn => Value::Array(
                Self::list_from_string(&value)
                    .iter()
                    .map(|s| Value::String(s.to_string()))
                    .collect(),
            ),
            _ => Value::String(value),
        };
        operator_value_obj.insert(operator.for_query(), json_value);

        let mut query_obj = Map::new();
        query_obj.insert(key, Value::Object(operator_value_obj));

        Some(serde_json::to_string(&Value::Object(query_obj)).unwrap_or_default())
    }
}
