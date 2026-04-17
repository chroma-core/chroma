use crate::style;
use crate::terminal::Terminal;
use crossterm::terminal;
use textwrap::wrap;

const SUMMARY_PANEL_MAX_WIDTH: usize = 92;
const SUMMARY_PANEL_MIN_INNER_WIDTH: usize = 20;

#[derive(Debug, Clone)]
pub struct FilterableSelectItem {
    pub label: String,
    pub summary: String,
}

pub struct PanelSelectPrompt<'a> {
    pub tag: &'a str,
    pub title: &'a str,
    pub context_lines: &'a [String],
    pub items: &'a [FilterableSelectItem],
    pub default_selected_index: usize,
    pub empty_message: &'a str,
}

pub struct FilterableMultiSelectPrompt<'a> {
    pub tag: &'a str,
    pub title: &'a str,
    pub preface_lines: &'a [String],
    pub prompt: &'a str,
    pub included_heading: Option<&'a str>,
    pub included_items: &'a [FilterableSelectItem],
    pub selectable_heading: &'a str,
    pub selectable_items: &'a [FilterableSelectItem],
    pub default_selected_indices: &'a [usize],
    pub empty_message: &'a str,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SharedUiElementKind {
    SectionHeader,
    CommandHint,
    SummaryPanel,
    PanelSelect,
    FilterableMultiSelect,
    StatusLine,
    SuccessBanner,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SharedUiElement {
    pub kind: SharedUiElementKind,
    pub name: &'static str,
    pub description: &'static str,
}

pub const SHARED_UI_ELEMENTS: &[SharedUiElement] = &[
    SharedUiElement {
        kind: SharedUiElementKind::SectionHeader,
        name: "section_header",
        description: "Blue bold section title for command output.",
    },
    SharedUiElement {
        kind: SharedUiElementKind::CommandHint,
        name: "command_hint",
        description: "Instruction line with a highlighted command example.",
    },
    SharedUiElement {
        kind: SharedUiElementKind::SummaryPanel,
        name: "summary_panel",
        description: "Boxed key/value confirmation panel for resolved selections.",
    },
    SharedUiElement {
        kind: SharedUiElementKind::PanelSelect,
        name: "panel_select",
        description: "Tagged single-choice panel rendered by the terminal backend.",
    },
    SharedUiElement {
        kind: SharedUiElementKind::FilterableMultiSelect,
        name: "filterable_multi_select",
        description: "Tagged searchable multi-select checklist rendered by the terminal backend.",
    },
    SharedUiElement {
        kind: SharedUiElementKind::StatusLine,
        name: "status_line",
        description: "Action-oriented banner used before long-running work starts.",
    },
    SharedUiElement {
        kind: SharedUiElementKind::SuccessBanner,
        name: "success_banner",
        description: "Blue bold completion message shown after successful work.",
    },
];

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

pub fn print_summary_panel(term: &mut dyn Terminal, label: &str, value: &str) {
    term.println("");
    for line in build_summary_panel_lines(label, value, resolve_summary_panel_width()) {
        term.println(&line);
    }
}

fn resolve_summary_panel_width() -> usize {
    terminal::size()
        .map(|(cols, _)| usize::from(cols).clamp(5, SUMMARY_PANEL_MAX_WIDTH))
        .unwrap_or(SUMMARY_PANEL_MAX_WIDTH)
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

    let label = truncate_summary_line(label, available.saturating_sub(3));
    let remaining = available.saturating_sub(label.chars().count() + 3);
    format!("┌─ {} {}┐", label, "─".repeat(remaining))
}

fn truncate_summary_line(line: &str, width: usize) -> String {
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

#[cfg(test)]
mod tests {
    use super::{
        build_summary_panel_lines, print_command_hint, print_section_header, print_status_line,
        print_success_banner, print_summary_panel, SHARED_UI_ELEMENTS,
    };
    use crate::terminal::test_terminal::TestTerminal;

    #[test]
    fn shared_ui_catalog_lists_expected_elements() {
        let names = SHARED_UI_ELEMENTS
            .iter()
            .map(|element| element.name)
            .collect::<Vec<_>>();

        assert_eq!(
            names,
            vec![
                "section_header",
                "command_hint",
                "summary_panel",
                "panel_select",
                "filterable_multi_select",
                "status_line",
                "success_banner",
            ]
        );
    }

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
}
