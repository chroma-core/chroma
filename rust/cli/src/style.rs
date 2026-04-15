use colored::{Color, ColoredString, Colorize};

pub const ACCENT_COLOR: Color = Color::Blue;
pub const COMMAND_COLOR: Color = Color::Yellow;
pub const SUCCESS_COLOR: Color = Color::Green;
pub const WARNING_COLOR: Color = Color::Yellow;
pub const ERROR_COLOR: Color = Color::Red;

pub const SECTION_HEADER_COLOR: Color = ACCENT_COLOR;
pub const STATUS_LABEL_COLOR: Color = ACCENT_COLOR;
pub const PROMPT_COLOR: Color = ACCENT_COLOR;
pub const COMMAND_HINT_COLOR: Color = COMMAND_COLOR;
pub const LIST_MARKER_COLOR: Color = COMMAND_COLOR;
pub const PANEL_BORDER_COLOR: Color = ACCENT_COLOR;
pub const LINK_COLOR: Color = ACCENT_COLOR;

fn color(text: impl Into<String>, color: Color) -> ColoredString {
    text.into().color(color)
}

pub fn accent(text: impl Into<String>) -> ColoredString {
    color(text, ACCENT_COLOR)
}

pub fn accent_bold(text: impl Into<String>) -> ColoredString {
    accent(text).bold()
}

pub fn section_header(text: impl Into<String>) -> ColoredString {
    color(text, SECTION_HEADER_COLOR).bold()
}

pub fn status_label(text: impl Into<String>) -> ColoredString {
    color(text, STATUS_LABEL_COLOR).bold()
}

pub fn prompt(text: impl Into<String>) -> ColoredString {
    color(text, PROMPT_COLOR)
}

pub fn prompt_bold(text: impl Into<String>) -> ColoredString {
    prompt(text).bold()
}

pub fn command(text: impl Into<String>) -> ColoredString {
    color(text, COMMAND_HINT_COLOR)
}

pub fn command_bold(text: impl Into<String>) -> ColoredString {
    command(text).bold()
}

pub fn list_marker() -> ColoredString {
    color(">", LIST_MARKER_COLOR)
}

pub fn success(text: impl Into<String>) -> ColoredString {
    color(text, SUCCESS_COLOR)
}

pub fn success_bold(text: impl Into<String>) -> ColoredString {
    success(text).bold()
}

pub fn warning(text: impl Into<String>) -> ColoredString {
    color(text, WARNING_COLOR)
}

pub fn warning_bold(text: impl Into<String>) -> ColoredString {
    warning(text).bold()
}

pub fn error(text: impl Into<String>) -> ColoredString {
    color(text, ERROR_COLOR)
}

pub fn error_bold(text: impl Into<String>) -> ColoredString {
    error(text).bold()
}

pub fn link(text: impl Into<String>) -> ColoredString {
    color(text, LINK_COLOR).underline()
}

pub fn panel_border(text: impl Into<String>) -> ColoredString {
    color(text, PANEL_BORDER_COLOR)
}
