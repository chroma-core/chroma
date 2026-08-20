use crate::terminal::Terminal;
use crate::tui::style;
use crate::utils::{CliError, UtilsError};
use crossterm::cursor::{Hide, MoveTo, Show};
use crossterm::event::{KeyCode, KeyEvent, KeyModifiers};
use crossterm::terminal::{self, ClearType};
use crossterm::ExecutableCommand;
use std::io::Write;
use textwrap::wrap;

pub(crate) const DEFAULT_MAX_VISIBLE_ITEMS: usize = 8;
pub(crate) const PANEL_MAX_WIDTH: usize = 92;

pub fn print_section_header(term: &mut dyn Terminal, title: &str) {
    term.println(&format!("{}", style::section_header(title)));
}

pub fn print_command_hint(term: &mut dyn Terminal, label: &str, command: &str) {
    term.println(&format!(
        "{} {}",
        style::accent_bold(label),
        style::command(command)
    ));
}

pub fn print_status_line(term: &mut dyn Terminal, action: &str, detail: &str) {
    term.println(&format!("{} {}", style::status_label(action), detail));
}

pub fn print_success_banner(term: &mut dyn Terminal, message: &str) {
    term.println(&format!("\n{}", style::accent_bold(message)));
}

pub(crate) fn is_interrupt(key: &KeyEvent) -> bool {
    key.modifiers == KeyModifiers::CONTROL && key.code == KeyCode::Char('c')
}

pub(crate) fn enable_raw_prompt(stdout: &mut std::io::Stdout) -> Result<(), CliError> {
    terminal::enable_raw_mode().map_err(|_| UtilsError::UserInputFailed)?;
    stdout
        .execute(Hide)
        .map_err(|_| UtilsError::UserInputFailed)?;
    Ok(())
}

pub(crate) fn disable_raw_prompt(
    stdout: &mut std::io::Stdout,
    origin: (u16, u16),
    rendered_rows: usize,
) -> Result<(), CliError> {
    clear_panel(stdout, origin, rendered_rows)?;
    stdout
        .execute(Show)
        .map_err(|_| UtilsError::UserInputFailed)?;
    terminal::disable_raw_mode().map_err(|_| UtilsError::UserInputFailed)?;
    Ok(())
}

pub(crate) fn clear_panel(
    stdout: &mut std::io::Stdout,
    origin: (u16, u16),
    rows_to_clear: usize,
) -> Result<(), CliError> {
    let (_, terminal_rows) = terminal::size().map_err(|_| UtilsError::UserInputFailed)?;
    let max_rows = usize::from(terminal_rows).saturating_sub(usize::from(origin.1));

    for row in 0..rows_to_clear.min(max_rows) {
        stdout
            .execute(MoveTo(0, origin.1 + row as u16))
            .map_err(|_| UtilsError::UserInputFailed)?;
        stdout
            .execute(terminal::Clear(ClearType::CurrentLine))
            .map_err(|_| UtilsError::UserInputFailed)?;
    }

    stdout
        .execute(MoveTo(origin.0, origin.1))
        .map_err(|_| UtilsError::UserInputFailed)?;
    stdout.flush().map_err(|_| UtilsError::UserInputFailed)?;
    Ok(())
}

pub(crate) fn truncate_line_for_width(line: &str, width: usize) -> String {
    let char_count = line.chars().count();
    if char_count <= width {
        return line.to_string();
    }

    if width <= 3 {
        return line.chars().take(width).collect();
    }

    let mut truncated = line.chars().take(width - 3).collect::<String>();
    truncated.push_str("...");
    truncated
}

pub(crate) fn pad_line_to_width(line: &str, width: usize) -> String {
    let line_width = line.chars().count();
    if line_width >= width {
        return line.to_string();
    }

    format!("{}{}", line, " ".repeat(width - line_width))
}

pub(crate) fn push_wrapped_lines(lines: &mut Vec<String>, value: &str, inner_width: usize) {
    let wrapped = wrap(value, inner_width.max(1));
    if wrapped.is_empty() {
        lines.push(String::new());
        return;
    }

    for line in wrapped {
        lines.push(line.into_owned());
    }
}

pub(crate) fn wrapped_line_count(value: &str, width: usize) -> usize {
    wrap(value, width.max(1)).len().max(1)
}

pub(crate) fn resolve_panel_width(terminal_cols: usize) -> usize {
    terminal_cols.clamp(4, PANEL_MAX_WIDTH)
}

pub(crate) fn resolve_panel_origin(
    cursor_origin: (u16, u16),
    terminal_size: (u16, u16),
    desired_rows: usize,
) -> (u16, u16) {
    let panel_width = resolve_panel_width(usize::from(terminal_size.0));
    let desired_rows = desired_rows.min(usize::from(terminal_size.1).max(1));
    let max_origin_y = usize::from(terminal_size.1).saturating_sub(desired_rows) as u16;
    let max_origin_x = usize::from(terminal_size.0).saturating_sub(panel_width) as u16;
    (
        cursor_origin.0.min(max_origin_x),
        cursor_origin.1.min(max_origin_y),
    )
}

pub(crate) fn visible_item_capacity(
    filtered_len: usize,
    terminal_rows: usize,
    static_rows: usize,
    footer_rows: usize,
    default_max_visible_items: usize,
) -> usize {
    if filtered_len == 0 {
        return 1;
    }

    let mut visible = filtered_len.min(default_max_visible_items).max(1);
    loop {
        let indicator_rows = if filtered_len > visible { 2 } else { 0 };
        let total_rows = static_rows + visible + indicator_rows + footer_rows;
        if total_rows <= terminal_rows || visible == 1 {
            return visible;
        }
        visible -= 1;
    }
}

pub(crate) fn visible_window_start(total: usize, highlighted: usize, max_visible: usize) -> usize {
    if total <= max_visible {
        return 0;
    }

    let half = max_visible / 2;
    let max_start = total - max_visible;
    highlighted.saturating_sub(half).min(max_start)
}

pub(crate) fn frame_panel_lines(tag: &str, lines: &[String], panel_width: usize) -> Vec<String> {
    let inner_width = panel_width.saturating_sub(4).max(1);
    let mut framed = Vec::with_capacity(lines.len() + 2);
    framed.push(format!(
        "{}",
        style::panel_border(build_panel_top_border(tag, panel_width))
    ));
    for line in lines {
        framed.push(format!(
            "{} {} {}",
            style::panel_border("│"),
            pad_line_to_width(&truncate_line_for_width(line, inner_width), inner_width),
            style::panel_border("│")
        ));
    }
    framed.push(format!(
        "{}",
        style::panel_border(format!("╰{}╯", "─".repeat(panel_width.saturating_sub(2))))
    ));
    framed
}

fn build_panel_top_border(tag: &str, panel_width: usize) -> String {
    let available = panel_width.saturating_sub(2);
    let tag = truncate_line_for_width(&format!("[ {} ]", tag), available.saturating_sub(1));
    let tag_width = tag.chars().count();
    if available <= tag_width + 1 {
        return format!("╭{}╮", "─".repeat(available));
    }

    let remaining = available - 1 - tag_width;
    format!("╭─{}{}╮", tag, "─".repeat(remaining))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::terminal::test_terminal::TestTerminal;

    #[test]
    fn shared_text_helpers_keep_command_output_styles_consistent() {
        let mut term = TestTerminal::new();

        print_section_header(&mut term, "Available skills");
        print_command_hint(&mut term, "Install with:", "chroma skills install foo");
        print_status_line(&mut term, "Installing", "foo into codex");
        print_success_banner(&mut term, "Installed foo successfully.");

        assert_eq!(term.output.len(), 4);
        assert!(term.output[0].contains("Available skills"));
        assert!(term.output[1].contains("Install with:"));
        assert!(term.output[2].contains("Installing foo into codex"));
        assert!(term.output[3].contains("Installed foo successfully."));
    }

    #[test]
    fn ctrl_c_is_the_only_interrupt_key() {
        assert!(is_interrupt(&KeyEvent::new(
            KeyCode::Char('c'),
            KeyModifiers::CONTROL
        )));
        assert!(!is_interrupt(&KeyEvent::new(
            KeyCode::Char('z'),
            KeyModifiers::CONTROL
        )));
        assert!(!is_interrupt(&KeyEvent::new(
            KeyCode::Char('c'),
            KeyModifiers::NONE
        )));
    }
}
