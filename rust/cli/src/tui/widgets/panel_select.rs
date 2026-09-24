use super::{
    clear_panel, disable_raw_prompt, enable_raw_prompt, frame_panel_lines, is_interrupt,
    push_wrapped_lines, resolve_panel_origin, resolve_panel_width, truncate_line_for_width,
    visible_item_capacity, visible_window_start, wrapped_line_count, PanelSelectPrompt,
    DEFAULT_MAX_VISIBLE_ITEMS, PANEL_MAX_WIDTH,
};
use crate::utils::{CliError, UtilsError};
use crossterm::cursor::{self, MoveTo};
use crossterm::event::{self, Event, KeyCode, KeyEventKind};
use crossterm::terminal;
use crossterm::ExecutableCommand;
use std::io::{stdout, Write};

pub fn run_panel_select(prompt: &PanelSelectPrompt<'_>) -> Result<usize, CliError> {
    let mut stdout = stdout();
    let cursor_origin = cursor::position().map_err(|_| UtilsError::UserInputFailed)?;
    let terminal_size = terminal::size().map_err(|_| UtilsError::UserInputFailed)?;
    let origin = resolve_panel_origin(
        cursor_origin,
        terminal_size,
        preferred_panel_select_rows(prompt),
    );
    enable_raw_prompt(&mut stdout)?;
    let mut rendered_rows = 0usize;

    let result = (|| -> Result<usize, CliError> {
        if prompt.items.is_empty() {
            return Err(UtilsError::UserInputFailed.into());
        }

        let mut highlighted = prompt
            .default_selected_index
            .min(prompt.items.len().saturating_sub(1));

        loop {
            rendered_rows =
                render_panel_select(&mut stdout, origin, prompt, highlighted, rendered_rows)?;

            let event = event::read().map_err(|_| UtilsError::UserInputFailed)?;
            let Event::Key(key) = event else {
                continue;
            };

            if key.kind != KeyEventKind::Press {
                continue;
            }

            if is_interrupt(&key) {
                break Err(UtilsError::UserInputFailed.into());
            }

            match key.code {
                KeyCode::Up => {
                    highlighted = highlighted.saturating_sub(1);
                }
                KeyCode::Down => {
                    if highlighted + 1 < prompt.items.len() {
                        highlighted += 1;
                    }
                }
                KeyCode::Enter => break Ok(highlighted),
                KeyCode::Esc => break Err(UtilsError::UserInputFailed.into()),
                _ => {}
            }
        }
    })();

    let _ = disable_raw_prompt(&mut stdout, origin, rendered_rows);
    result
}

fn render_panel_select(
    stdout: &mut std::io::Stdout,
    origin: (u16, u16),
    prompt: &PanelSelectPrompt<'_>,
    highlighted: usize,
    previous_frame_rows: usize,
) -> Result<usize, CliError> {
    let (terminal_cols, terminal_rows) =
        terminal::size().map_err(|_| UtilsError::UserInputFailed)?;
    let available_rows = usize::from(terminal_rows).saturating_sub(usize::from(origin.1));
    let available_cols = usize::from(terminal_cols);
    let lines = build_panel_select_lines(
        prompt,
        highlighted,
        available_rows.max(3),
        available_cols.max(12),
    );

    clear_panel(stdout, origin, previous_frame_rows.max(lines.len()))?;

    for (row, line) in lines.iter().enumerate() {
        stdout
            .execute(MoveTo(origin.0, origin.1 + row as u16))
            .map_err(|_| UtilsError::UserInputFailed)?;
        stdout
            .write_all(line.as_bytes())
            .map_err(|_| UtilsError::UserInputFailed)?;
    }
    stdout.flush().map_err(|_| UtilsError::UserInputFailed)?;
    Ok(lines.len())
}

pub(crate) fn build_panel_select_lines(
    prompt: &PanelSelectPrompt<'_>,
    highlighted: usize,
    terminal_rows: usize,
    terminal_cols: usize,
) -> Vec<String> {
    let panel_width = resolve_panel_width(terminal_cols);
    let inner_width = panel_width.saturating_sub(4).max(1);
    let content_rows = terminal_rows.saturating_sub(2).max(1);
    let compact = content_rows <= estimate_compact_threshold(prompt, inner_width);
    let static_rows = estimate_static_rows(prompt, compact, inner_width);
    let footer_rows = if compact { 1 } else { 2 };
    let max_visible_items = visible_item_capacity(
        prompt.items.len(),
        content_rows,
        static_rows,
        footer_rows,
        DEFAULT_MAX_VISIBLE_ITEMS,
    );
    let start = visible_window_start(prompt.items.len(), highlighted, max_visible_items);
    let end = usize::min(start + max_visible_items, prompt.items.len());

    let mut content_lines = Vec::new();
    push_wrapped_lines(&mut content_lines, prompt.title, inner_width);
    if !compact {
        content_lines.push(String::new());
    }

    for line in prompt.context_lines {
        push_wrapped_lines(&mut content_lines, line, inner_width);
    }
    if !prompt.context_lines.is_empty() && !compact {
        content_lines.push(String::new());
    }

    if prompt.items.is_empty() {
        push_wrapped_lines(&mut content_lines, prompt.empty_message, inner_width);
    } else {
        if start > 0 {
            content_lines.push(truncate_line_for_width(
                &format!("↑ {} more", start),
                inner_width,
            ));
        }

        for (visible_index, item) in prompt.items[start..end].iter().enumerate() {
            let absolute_index = start + visible_index;
            let marker = if absolute_index == highlighted {
                "◉"
            } else {
                "○"
            };
            content_lines.push(truncate_line_for_width(
                &format!("{} {}", marker, item.label),
                inner_width,
            ));
        }

        if end < prompt.items.len() {
            content_lines.push(truncate_line_for_width(
                &format!("↓ {} more", prompt.items.len() - end),
                inner_width,
            ));
        }
    }

    if !compact {
        content_lines.push(String::new());
    }
    content_lines.push(truncate_line_for_width(
        "↑/↓ move, enter confirm, esc cancel",
        inner_width,
    ));

    if content_lines.len() > content_rows {
        let footer_line = content_lines
            .pop()
            .unwrap_or_else(|| "↑/↓ move, enter confirm".to_string());
        content_lines.truncate(content_rows.saturating_sub(1));
        if content_rows > 0 {
            content_lines.push(footer_line);
        }
    }

    frame_panel_lines(prompt.tag, &content_lines, panel_width)
}

fn preferred_panel_select_rows(prompt: &PanelSelectPrompt<'_>) -> usize {
    estimate_static_rows(prompt, false, PANEL_MAX_WIDTH.saturating_sub(4))
        + DEFAULT_MAX_VISIBLE_ITEMS
        + 4
}

fn estimate_compact_threshold(prompt: &PanelSelectPrompt<'_>, inner_width: usize) -> usize {
    estimate_static_rows(prompt, false, inner_width) + 1
}

fn estimate_static_rows(
    prompt: &PanelSelectPrompt<'_>,
    compact: bool,
    inner_width: usize,
) -> usize {
    let mut rows = wrapped_line_count(prompt.title, inner_width);
    if !compact {
        rows += 1;
    }
    rows += prompt
        .context_lines
        .iter()
        .map(|line| wrapped_line_count(line, inner_width))
        .sum::<usize>();
    if !prompt.context_lines.is_empty() && !compact {
        rows += 1;
    }
    rows
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::tui::widgets::FilterableSelectItem;

    #[test]
    fn panel_select_renders_bordered_prompt() {
        let context_lines = vec!["Selected skill: chroma-cloud".to_string()];
        let prompt = PanelSelectPrompt {
            tag: "skills",
            title: "Choose install scope",
            context_lines: &context_lines,
            items: &[
                FilterableSelectItem {
                    label: "Project".to_string(),
                    summary: "Project".to_string(),
                },
                FilterableSelectItem {
                    label: "Global".to_string(),
                    summary: "Global".to_string(),
                },
            ],
            default_selected_index: 0,
            empty_message: "No options available.",
        };

        let lines = build_panel_select_lines(&prompt, 0, 12, 40);

        assert!(lines[0].starts_with("╭"));
        assert!(lines
            .iter()
            .any(|line| line.contains("Choose install scope")));
        assert!(lines.iter().any(|line| line.contains("Project")));
        assert!(lines.iter().all(|line| line.chars().count() <= 40));
    }
}
