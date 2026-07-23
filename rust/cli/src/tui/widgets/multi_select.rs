use super::{
    clear_panel, disable_raw_prompt, enable_raw_prompt, frame_panel_lines, is_interrupt,
    push_wrapped_lines, resolve_panel_origin, resolve_panel_width, truncate_line_for_width,
    visible_item_capacity, visible_window_start, wrapped_line_count, FilterableMultiSelectPrompt,
    FilterableSelectItem, DEFAULT_MAX_VISIBLE_ITEMS, PANEL_MAX_WIDTH,
};
use crate::utils::{CliError, UtilsError};
use crossterm::cursor::{self, MoveTo};
use crossterm::event::{self, Event, KeyCode, KeyEventKind, KeyModifiers};
use crossterm::terminal;
use crossterm::ExecutableCommand;
use std::collections::BTreeSet;
use std::io::{stdout, Write};

pub fn run_filterable_multi_select(
    prompt: &FilterableMultiSelectPrompt<'_>,
) -> Result<Vec<usize>, CliError> {
    let mut stdout = stdout();
    let cursor_origin = cursor::position().map_err(|_| UtilsError::UserInputFailed)?;
    let terminal_size = terminal::size().map_err(|_| UtilsError::UserInputFailed)?;
    let origin = resolve_filterable_prompt_origin(cursor_origin, terminal_size, prompt);
    enable_raw_prompt(&mut stdout)?;
    let mut rendered_rows = 0usize;

    let result = (|| -> Result<Vec<usize>, CliError> {
        let mut query = String::new();
        let mut selected = prompt
            .default_selected_indices
            .iter()
            .copied()
            .filter(|index| *index < prompt.selectable_items.len())
            .collect::<BTreeSet<_>>();
        let mut highlighted = 0usize;

        loop {
            let filtered = filtered_indices(prompt.selectable_items, &query);
            if filtered.is_empty() {
                highlighted = 0;
            } else if highlighted >= filtered.len() {
                highlighted = filtered.len() - 1;
            }

            rendered_rows = render_filterable_multi_select(
                &mut stdout,
                origin,
                prompt,
                &query,
                &filtered,
                highlighted,
                &selected,
                rendered_rows,
            )?;

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
                    if highlighted + 1 < filtered.len() {
                        highlighted += 1;
                    }
                }
                KeyCode::Char(' ') => {
                    if let Some(index) = filtered.get(highlighted) {
                        if !selected.insert(*index) {
                            selected.remove(index);
                        }
                    }
                }
                KeyCode::Backspace => {
                    query.pop();
                    highlighted = 0;
                }
                KeyCode::Enter => {
                    break Ok(selected.into_iter().collect());
                }
                KeyCode::Esc => {
                    break Err(UtilsError::UserInputFailed.into());
                }
                KeyCode::Char(c)
                    if key.modifiers.is_empty() || key.modifiers == KeyModifiers::SHIFT =>
                {
                    query.push(c);
                    highlighted = 0;
                }
                _ => {}
            }
        }
    })();

    let _ = disable_raw_prompt(&mut stdout, origin, rendered_rows);
    result
}

fn render_filterable_multi_select(
    stdout: &mut std::io::Stdout,
    origin: (u16, u16),
    prompt: &FilterableMultiSelectPrompt<'_>,
    query: &str,
    filtered: &[usize],
    highlighted: usize,
    selected: &BTreeSet<usize>,
    previous_frame_rows: usize,
) -> Result<usize, CliError> {
    let (terminal_cols, terminal_rows) =
        terminal::size().map_err(|_| UtilsError::UserInputFailed)?;
    let available_rows = usize::from(terminal_rows).saturating_sub(usize::from(origin.1));
    let available_cols = usize::from(terminal_cols).saturating_sub(usize::from(origin.0));
    let ctx = RenderContext {
        query,
        filtered,
        highlighted,
        selected,
        terminal_rows: available_rows.max(1),
        terminal_cols: available_cols.max(1),
    };
    let lines = build_filterable_multi_select_lines(prompt, &ctx);

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

fn filtered_indices(items: &[FilterableSelectItem], query: &str) -> Vec<usize> {
    let normalized_query = query.trim().to_lowercase();
    items
        .iter()
        .enumerate()
        .filter(|(_, item)| {
            normalized_query.is_empty()
                || item.label.to_lowercase().contains(&normalized_query)
                || item.summary.to_lowercase().contains(&normalized_query)
        })
        .map(|(index, _)| index)
        .collect()
}

#[derive(Clone, Copy)]
pub(crate) struct RenderContext<'a> {
    pub query: &'a str,
    pub filtered: &'a [usize],
    pub highlighted: usize,
    pub selected: &'a BTreeSet<usize>,
    pub terminal_rows: usize,
    pub terminal_cols: usize,
}

pub(crate) fn build_filterable_multi_select_lines(
    prompt: &FilterableMultiSelectPrompt<'_>,
    ctx: &RenderContext<'_>,
) -> Vec<String> {
    let RenderContext {
        query,
        filtered,
        highlighted,
        selected,
        terminal_rows,
        terminal_cols,
    } = *ctx;
    let panel_width = resolve_panel_width(terminal_cols);
    let inner_width = panel_width.saturating_sub(4).max(1);
    let content_rows = terminal_rows.saturating_sub(2).max(1);
    let compact = content_rows <= estimate_compact_threshold(prompt);
    let static_rows = estimate_static_rows(prompt, compact, inner_width);
    let footer_rows = if compact { 1 } else { 2 };
    let max_visible_items = visible_item_capacity(
        filtered.len(),
        content_rows,
        static_rows,
        footer_rows,
        DEFAULT_MAX_VISIBLE_ITEMS,
    );

    let mut content_lines = Vec::new();
    push_wrapped_lines(&mut content_lines, prompt.title, inner_width);
    if !compact {
        content_lines.push(String::new());
    }

    for line in prompt.preface_lines {
        push_wrapped_lines(&mut content_lines, line, inner_width);
    }
    if !prompt.preface_lines.is_empty() && !compact {
        content_lines.push(String::new());
    }

    push_wrapped_lines(&mut content_lines, prompt.prompt, inner_width);
    if let Some(heading) = prompt.included_heading {
        if !compact {
            content_lines.push(String::new());
        }
        push_wrapped_lines(&mut content_lines, heading, inner_width);
        for item in prompt.included_items {
            content_lines.push(truncate_line_for_width(
                &format!("  • {}", item.label),
                inner_width,
            ));
        }
    }

    if !compact {
        content_lines.push(String::new());
    }
    push_wrapped_lines(&mut content_lines, prompt.selectable_heading, inner_width);
    content_lines.push(truncate_line_for_width(
        &format!("Search: {}", query),
        inner_width,
    ));
    if !compact {
        content_lines.push(truncate_line_for_width(
            "↑/↓ move, space select, enter confirm",
            inner_width,
        ));
    }

    if filtered.is_empty() {
        push_wrapped_lines(&mut content_lines, prompt.empty_message, inner_width);
    } else {
        let start = visible_window_start(filtered.len(), highlighted, max_visible_items);
        let end = usize::min(start + max_visible_items, filtered.len());

        if start > 0 {
            content_lines.push(truncate_line_for_width(
                &format!("↑ {} more", start),
                inner_width,
            ));
        }

        for (visible_index, item_index) in filtered[start..end].iter().enumerate() {
            let absolute_index = start + visible_index;
            let cursor = if absolute_index == highlighted {
                "›"
            } else {
                " "
            };
            let marker = if selected.contains(item_index) {
                "◉"
            } else {
                "○"
            };
            content_lines.push(truncate_line_for_width(
                &format!(
                    "{} {} {}",
                    cursor, marker, prompt.selectable_items[*item_index].label
                ),
                inner_width,
            ));
        }

        if end < filtered.len() {
            content_lines.push(truncate_line_for_width(
                &format!("↓ {} more", filtered.len() - end),
                inner_width,
            ));
        }
    }

    if !compact {
        content_lines.push(String::new());
    }
    content_lines.push(truncate_line_for_width(
        &format!("Selected: {}", summarize_selection(prompt, selected)),
        inner_width,
    ));

    if content_lines.len() > content_rows {
        let selected_line = content_lines
            .pop()
            .unwrap_or_else(|| "Selected: none".to_string());
        content_lines.truncate(content_rows.saturating_sub(1));
        if content_rows > 0 {
            content_lines.push(selected_line);
        }
    }

    frame_panel_lines(prompt.tag, &content_lines, panel_width)
}

pub(crate) fn resolve_filterable_prompt_origin(
    cursor_origin: (u16, u16),
    terminal_size: (u16, u16),
    prompt: &FilterableMultiSelectPrompt<'_>,
) -> (u16, u16) {
    resolve_panel_origin(cursor_origin, terminal_size, preferred_render_rows(prompt))
}

fn preferred_render_rows(prompt: &FilterableMultiSelectPrompt<'_>) -> usize {
    estimate_static_rows(prompt, false, PANEL_MAX_WIDTH.saturating_sub(4))
        + DEFAULT_MAX_VISIBLE_ITEMS
        + 6
}

fn estimate_compact_threshold(prompt: &FilterableMultiSelectPrompt<'_>) -> usize {
    estimate_static_rows(prompt, false, PANEL_MAX_WIDTH.saturating_sub(4)) + 1
}

fn estimate_static_rows(
    prompt: &FilterableMultiSelectPrompt<'_>,
    compact: bool,
    inner_width: usize,
) -> usize {
    let mut rows = wrapped_line_count(prompt.title, inner_width);
    if !compact {
        rows += 1;
    }
    rows += prompt
        .preface_lines
        .iter()
        .map(|line| wrapped_line_count(line, inner_width))
        .sum::<usize>();
    if !prompt.preface_lines.is_empty() && !compact {
        rows += 1;
    }
    rows += wrapped_line_count(prompt.prompt, inner_width);
    if prompt.included_heading.is_some() {
        rows += 1 + prompt.included_items.len();
    }
    rows += 2;
    if !compact {
        rows += 2;
    }
    rows
}

fn summarize_selection(
    prompt: &FilterableMultiSelectPrompt<'_>,
    selected: &BTreeSet<usize>,
) -> String {
    let names = prompt
        .included_items
        .iter()
        .map(|item| item.summary.clone())
        .chain(
            selected
                .iter()
                .map(|index| prompt.selectable_items[*index].summary.clone()),
        )
        .collect::<Vec<_>>();

    if names.is_empty() {
        return "none".to_string();
    }

    const MAX_NAMES: usize = 3;
    if names.len() <= MAX_NAMES {
        return names.join(", ");
    }

    format!(
        "{}, {}, {} +{} more",
        names[0],
        names[1],
        names[2],
        names.len() - MAX_NAMES
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn rendered_lines_fit_small_terminal_height() {
        let preface = vec!["43 supported agents".to_string()];
        let included = vec![
            FilterableSelectItem {
                label: "Amp".to_string(),
                summary: "Amp".to_string(),
            },
            FilterableSelectItem {
                label: "Antigravity".to_string(),
                summary: "Antigravity".to_string(),
            },
        ];
        let selectable = (0..10)
            .map(|index| FilterableSelectItem {
                label: format!("Agent {}", index),
                summary: format!("Agent {}", index),
            })
            .collect::<Vec<_>>();
        let prompt = FilterableMultiSelectPrompt {
            tag: "skills",
            title: "Install skill into agents",
            preface_lines: &preface,
            prompt: "Which agents do you want to install to?",
            included_heading: Some("Universal (.agents/skills) - always included"),
            included_items: &included,
            selectable_heading: "Additional agents",
            selectable_items: &selectable,
            default_selected_indices: &[],
            empty_message: "No matching agents.",
        };

        let filtered = (0..selectable.len()).collect::<Vec<_>>();
        let selected = BTreeSet::new();
        let ctx = RenderContext {
            query: "",
            filtered: &filtered,
            highlighted: 0,
            selected: &selected,
            terminal_rows: 8,
            terminal_cols: 20,
        };
        let lines = build_filterable_multi_select_lines(&prompt, &ctx);

        assert_eq!(lines.len(), 8);
        assert!(lines[0].starts_with("╭"));
        assert!(lines.iter().all(|line| line.chars().count() <= 20));
        assert!(lines.iter().any(|line| line.contains("Selected: ")));
    }

    #[test]
    fn prompt_origin_shifts_up_when_cursor_is_near_bottom() {
        let prompt = FilterableMultiSelectPrompt {
            tag: "skills",
            title: "Install skill into agents",
            preface_lines: &[],
            prompt: "Which agents do you want to install to?",
            included_heading: Some("Universal (.agents/skills) - always included"),
            included_items: &[],
            selectable_heading: "Additional agents",
            selectable_items: &[],
            default_selected_indices: &[],
            empty_message: "No matching agents.",
        };

        let origin = resolve_filterable_prompt_origin((0, 20), (80, 24), &prompt);

        assert!(origin.1 < 20);
    }

    #[test]
    fn prompt_origin_stays_left_justified() {
        let prompt = FilterableMultiSelectPrompt {
            tag: "skills",
            title: "Install skill into agents",
            preface_lines: &[],
            prompt: "Which agents do you want to install to?",
            included_heading: None,
            included_items: &[],
            selectable_heading: "Additional agents",
            selectable_items: &[],
            default_selected_indices: &[],
            empty_message: "No matching agents.",
        };

        let origin = resolve_filterable_prompt_origin((0, 4), (120, 24), &prompt);

        assert_eq!(origin.0, 0);
    }
}
