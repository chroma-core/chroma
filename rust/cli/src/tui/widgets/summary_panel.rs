use super::{truncate_line_for_width, PANEL_MAX_WIDTH};
use crate::terminal::Terminal;
use crate::tui::style;
use crossterm::terminal;
use textwrap::wrap;

const SUMMARY_PANEL_MIN_INNER_WIDTH: usize = 20;

pub fn print_summary_panel(term: &mut dyn Terminal, label: &str, value: &str) {
    term.println("");
    for line in build_summary_panel_lines(label, value, resolve_summary_panel_width()) {
        term.println(&line);
    }
}

fn resolve_summary_panel_width() -> usize {
    terminal::size()
        .map(|(cols, _)| usize::from(cols).clamp(5, PANEL_MAX_WIDTH))
        .unwrap_or(PANEL_MAX_WIDTH)
}

fn build_summary_panel_lines(label: &str, value: &str, panel_width: usize) -> Vec<String> {
    let panel_width = panel_width.max(5);
    let inner_limit = panel_width.saturating_sub(4).max(1);
    let content = wrap_summary_content(value, inner_limit);
    let widest_line = content
        .iter()
        .map(|line| line.chars().count())
        .max()
        .unwrap_or(0);
    let inner_width = widest_line.max(SUMMARY_PANEL_MIN_INNER_WIDTH.min(inner_limit));
    let panel_width = inner_width + 4;
    let top_border = build_summary_top_border(label, panel_width);
    let bottom_border = format!("└{}┘", "─".repeat(panel_width.saturating_sub(2)));

    let mut lines = Vec::with_capacity(content.len() + 2);
    lines.push(format!("{}", style::panel_border(top_border)));
    for line in content {
        let padding = inner_width.saturating_sub(line.chars().count());
        lines.push(format!(
            "{} {}{} {}",
            style::panel_border("│"),
            line,
            " ".repeat(padding),
            style::panel_border("│")
        ));
    }
    lines.push(format!("{}", style::panel_border(bottom_border)));
    lines
}

fn wrap_summary_content(value: &str, inner_width: usize) -> Vec<String> {
    let mut lines = Vec::new();

    for raw_line in value.lines() {
        let wrapped = wrap(raw_line, inner_width.max(1));
        if wrapped.is_empty() {
            lines.push(String::new());
            continue;
        }

        lines.extend(wrapped.into_iter().map(|line| line.into_owned()));
    }

    if lines.is_empty() {
        lines.push(String::new());
    }

    lines
}

fn build_summary_top_border(label: &str, panel_width: usize) -> String {
    let available = panel_width.saturating_sub(2);
    if available <= 3 {
        return format!("┌{}┐", "─".repeat(available));
    }

    let label = truncate_line_for_width(label, available.saturating_sub(3));
    let remaining = available.saturating_sub(label.chars().count() + 3);
    format!("┌─ {} {}┐", label, "─".repeat(remaining))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::terminal::test_terminal::TestTerminal;

    #[test]
    fn summary_panel_draws_a_box_for_multiline_content() {
        let mut term = TestTerminal::new();

        print_summary_panel(&mut term, "Agents", "Codex\nCursor");

        assert_eq!(term.output[0], "");
        assert!(term.output[1].starts_with("┌─ Agents "));
        assert!(term.output[2].starts_with("│ Codex"));
        assert!(term.output[2].ends_with("│"));
        assert!(term.output[3].starts_with("│ Cursor"));
        assert!(term.output[3].ends_with("│"));
        assert!(term.output[4].starts_with("└"));
        assert!(term.output[4].ends_with("┘"));
        assert_eq!(
            term.output[1].chars().count(),
            term.output[4].chars().count()
        );
    }

    #[test]
    fn summary_panel_wraps_long_values_to_fit_available_width() {
        let lines = build_summary_panel_lines(
            "Agents",
            "Amp, Antigravity, Cline, Codex +8 more [25 selected; additional: Claude Code, OpenClaw, CodeBuddy, Command Code +9 more]",
            40,
        );

        assert!(lines.len() > 4);
        assert!(lines.iter().all(|line| line.chars().count() <= 40));
        assert!(lines.iter().any(|line| line.contains("additional:")));
    }
}
