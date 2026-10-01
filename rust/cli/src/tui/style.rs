use colored::{Color, ColoredString, Colorize};

const ACCENT_COLOR: Color = Color::Blue;
const COMMAND_COLOR: Color = Color::Yellow;

fn color(text: impl Into<String>, color: Color) -> ColoredString {
    text.into().color(color)
}

pub fn accent_bold(text: impl Into<String>) -> ColoredString {
    color(text, ACCENT_COLOR).bold()
}

pub fn section_header(text: impl Into<String>) -> ColoredString {
    accent_bold(text)
}

pub fn status_label(text: impl Into<String>) -> ColoredString {
    accent_bold(text)
}

pub fn command(text: impl Into<String>) -> ColoredString {
    color(text, COMMAND_COLOR)
}

pub fn list_marker() -> ColoredString {
    color(">", COMMAND_COLOR)
}

pub fn panel_border(text: impl Into<String>) -> ColoredString {
    color(text, ACCENT_COLOR)
}
