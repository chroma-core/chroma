use crate::style;
use crate::ui::{FilterableMultiSelectPrompt, FilterableSelectItem, PanelSelectPrompt};
use crate::utils::{CliError, UtilsError};
use crossterm::cursor::{self, Hide, MoveTo, Show};
use crossterm::event::{self, Event, KeyCode, KeyEventKind, KeyModifiers};
use crossterm::terminal::{self, ClearType};
use crossterm::ExecutableCommand;
use dialoguer::theme::ColorfulTheme;
use dialoguer::{Confirm, Input, Select};
use std::collections::BTreeSet;
use std::io::{stdout, Write};
use textwrap::wrap;

pub trait Terminal {
    fn println(&mut self, msg: &str);
    fn prompt_input(&mut self) -> Result<String, CliError>;
    fn prompt_select(&mut self, items: &[String]) -> Result<usize, CliError>;
    fn prompt_panel_select(&mut self, prompt: &PanelSelectPrompt<'_>) -> Result<usize, CliError>;
    fn prompt_multi_select(
        &mut self,
        prompt: &FilterableMultiSelectPrompt<'_>,
    ) -> Result<Vec<usize>, CliError>;
    fn prompt_confirm(&mut self, prompt: &str) -> Result<bool, CliError>;
}

pub struct SystemTerminal;
const DEFAULT_MAX_VISIBLE_ITEMS: usize = 8;
const PANEL_MAX_WIDTH: usize = 92;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum RawPromptControl {
    Interrupt,
    Suspend,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum RawPromptControlResult {
    NotHandled,
    ContinuePrompt,
    AbortPrompt,
}

impl Terminal for SystemTerminal {
    fn println(&mut self, msg: &str) {
        println!("{}", msg);
    }

    fn prompt_input(&mut self) -> Result<String, CliError> {
        let input: String = Input::with_theme(&ColorfulTheme::default())
            .interact_text()
            .map_err(|_| UtilsError::UserInputFailed)?;
        Ok(input)
    }

    fn prompt_select(&mut self, items: &[String]) -> Result<usize, CliError> {
        let selection = Select::with_theme(&ColorfulTheme::default())
            .items(items)
            .default(0)
            .interact()
            .map_err(|_| UtilsError::UserInputFailed)?;
        Ok(selection)
    }

    fn prompt_panel_select(&mut self, prompt: &PanelSelectPrompt<'_>) -> Result<usize, CliError> {
        run_panel_select(prompt)
    }

    fn prompt_multi_select(
        &mut self,
        prompt: &FilterableMultiSelectPrompt<'_>,
    ) -> Result<Vec<usize>, CliError> {
        run_filterable_multi_select(prompt)
    }

    fn prompt_confirm(&mut self, prompt: &str) -> Result<bool, CliError> {
        let confirmed = Confirm::new()
            .with_prompt(prompt)
            .interact()
            .map_err(|_| UtilsError::UserInputFailed)?;
        Ok(confirmed)
    }
}

fn control_action_for_key(key: &crossterm::event::KeyEvent) -> Option<RawPromptControl> {
    if key.modifiers != KeyModifiers::CONTROL {
        return None;
    }

    match key.code {
        KeyCode::Char('c') => Some(RawPromptControl::Interrupt),
        KeyCode::Char('z') => Some(RawPromptControl::Suspend),
        _ => None,
    }
}

fn enable_raw_prompt(stdout: &mut std::io::Stdout) -> Result<(), CliError> {
    terminal::enable_raw_mode().map_err(|_| UtilsError::UserInputFailed)?;
    stdout
        .execute(Hide)
        .map_err(|_| UtilsError::UserInputFailed)?;
    Ok(())
}

fn disable_raw_prompt(
    stdout: &mut std::io::Stdout,
    origin: (u16, u16),
    rendered_rows: usize,
) -> Result<(), CliError> {
    clear_filterable_multi_select(stdout, origin, rendered_rows)?;
    stdout
        .execute(Show)
        .map_err(|_| UtilsError::UserInputFailed)?;
    terminal::disable_raw_mode().map_err(|_| UtilsError::UserInputFailed)?;
    Ok(())
}

#[cfg(unix)]
fn raise_interrupt_signal() {
    unsafe {
        libc::raise(libc::SIGINT);
    }
}

#[cfg(not(unix))]
fn raise_interrupt_signal() {}

#[cfg(unix)]
fn suspend_process() -> Result<(), CliError> {
    let result = unsafe { libc::raise(libc::SIGTSTP) };
    if result == 0 {
        Ok(())
    } else {
        Err(UtilsError::UserInputFailed.into())
    }
}

#[cfg(not(unix))]
fn suspend_process() -> Result<(), CliError> {
    Err(UtilsError::UserInputFailed.into())
}

fn handle_raw_prompt_control<F>(
    stdout: &mut std::io::Stdout,
    key: &crossterm::event::KeyEvent,
    origin: &mut (u16, u16),
    rendered_rows: &mut usize,
    resolve_origin: F,
) -> Result<RawPromptControlResult, CliError>
where
    F: Fn((u16, u16), (u16, u16)) -> (u16, u16),
{
    let Some(action) = control_action_for_key(key) else {
        return Ok(RawPromptControlResult::NotHandled);
    };

    disable_raw_prompt(stdout, *origin, *rendered_rows)?;

    match action {
        RawPromptControl::Interrupt => {
            raise_interrupt_signal();
            Ok(RawPromptControlResult::AbortPrompt)
        }
        RawPromptControl::Suspend => {
            suspend_process()?;
            let cursor_origin = cursor::position().map_err(|_| UtilsError::UserInputFailed)?;
            let terminal_size = terminal::size().map_err(|_| UtilsError::UserInputFailed)?;
            *origin = resolve_origin(cursor_origin, terminal_size);
            *rendered_rows = 0;
            enable_raw_prompt(stdout)?;
            Ok(RawPromptControlResult::ContinuePrompt)
        }
    }
}

fn run_filterable_multi_select(
    prompt: &FilterableMultiSelectPrompt<'_>,
) -> Result<Vec<usize>, CliError> {
    let mut stdout = stdout();
    let cursor_origin = cursor::position().map_err(|_| UtilsError::UserInputFailed)?;
    let terminal_size = terminal::size().map_err(|_| UtilsError::UserInputFailed)?;
    let mut origin = resolve_filterable_prompt_origin(cursor_origin, terminal_size, prompt);
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

            match handle_raw_prompt_control(
                &mut stdout,
                &key,
                &mut origin,
                &mut rendered_rows,
                |cursor_origin, terminal_size| {
                    resolve_filterable_prompt_origin(cursor_origin, terminal_size, prompt)
                },
            )? {
                RawPromptControlResult::NotHandled => {}
                RawPromptControlResult::ContinuePrompt => continue,
                RawPromptControlResult::AbortPrompt => {
                    break Err(UtilsError::UserInputFailed.into());
                }
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

fn run_panel_select(prompt: &PanelSelectPrompt<'_>) -> Result<usize, CliError> {
    let mut stdout = stdout();
    let cursor_origin = cursor::position().map_err(|_| UtilsError::UserInputFailed)?;
    let terminal_size = terminal::size().map_err(|_| UtilsError::UserInputFailed)?;
    let mut origin = resolve_panel_origin(
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

            match handle_raw_prompt_control(
                &mut stdout,
                &key,
                &mut origin,
                &mut rendered_rows,
                |cursor_origin, terminal_size| {
                    resolve_panel_origin(
                        cursor_origin,
                        terminal_size,
                        preferred_panel_select_rows(prompt),
                    )
                },
            )? {
                RawPromptControlResult::NotHandled => {}
                RawPromptControlResult::ContinuePrompt => continue,
                RawPromptControlResult::AbortPrompt => {
                    break Err(UtilsError::UserInputFailed.into());
                }
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

    clear_filterable_multi_select(stdout, origin, previous_frame_rows.max(lines.len()))?;

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

#[allow(clippy::too_many_arguments)]
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
    let lines = build_filterable_multi_select_lines(
        prompt,
        query,
        filtered,
        highlighted,
        selected,
        available_rows.max(1),
        available_cols.max(1),
    );

    clear_filterable_multi_select(stdout, origin, previous_frame_rows.max(lines.len()))?;

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

fn clear_filterable_multi_select(
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

fn build_filterable_multi_select_lines(
    prompt: &FilterableMultiSelectPrompt<'_>,
    query: &str,
    filtered: &[usize],
    highlighted: usize,
    selected: &BTreeSet<usize>,
    terminal_rows: usize,
    terminal_cols: usize,
) -> Vec<String> {
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

fn build_panel_select_lines(
    prompt: &PanelSelectPrompt<'_>,
    highlighted: usize,
    terminal_rows: usize,
    terminal_cols: usize,
) -> Vec<String> {
    let panel_width = resolve_panel_width(terminal_cols);
    let inner_width = panel_width.saturating_sub(4).max(1);
    let content_rows = terminal_rows.saturating_sub(2).max(1);
    let compact = content_rows <= estimate_panel_select_compact_threshold(prompt, inner_width);
    let static_rows = estimate_panel_select_static_rows(prompt, compact, inner_width);
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

fn resolve_filterable_prompt_origin(
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

fn preferred_panel_select_rows(prompt: &PanelSelectPrompt<'_>) -> usize {
    estimate_panel_select_static_rows(prompt, false, PANEL_MAX_WIDTH.saturating_sub(4))
        + DEFAULT_MAX_VISIBLE_ITEMS
        + 4
}

fn truncate_line_for_width(line: &str, width: usize) -> String {
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

fn estimate_panel_select_compact_threshold(
    prompt: &PanelSelectPrompt<'_>,
    inner_width: usize,
) -> usize {
    estimate_panel_select_static_rows(prompt, false, inner_width) + 1
}

fn estimate_panel_select_static_rows(
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

fn visible_item_capacity(
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

fn visible_window_start(total: usize, highlighted: usize, max_visible: usize) -> usize {
    if total <= max_visible {
        return 0;
    }

    let half = max_visible / 2;
    let max_start = total - max_visible;
    highlighted.saturating_sub(half).min(max_start)
}

fn resolve_panel_origin(
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

fn resolve_panel_width(terminal_cols: usize) -> usize {
    terminal_cols.clamp(4, PANEL_MAX_WIDTH)
}

fn frame_panel_lines(tag: &str, lines: &[String], panel_width: usize) -> Vec<String> {
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

fn push_wrapped_lines(lines: &mut Vec<String>, value: &str, inner_width: usize) {
    let wrapped = wrap(value, inner_width.max(1));
    if wrapped.is_empty() {
        lines.push(String::new());
        return;
    }

    for line in wrapped {
        lines.push(line.into_owned());
    }
}

fn wrapped_line_count(value: &str, width: usize) -> usize {
    wrap(value, width.max(1)).len().max(1)
}

fn pad_line_to_width(line: &str, width: usize) -> String {
    let line_width = line.chars().count();
    if line_width >= width {
        return line.to_string();
    }

    format!("{}{}", line, " ".repeat(width - line_width))
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
pub mod test_terminal {
    use super::{
        build_filterable_multi_select_lines, build_panel_select_lines, control_action_for_key,
        resolve_filterable_prompt_origin, RawPromptControl, Terminal,
    };
    use crate::ui::{FilterableMultiSelectPrompt, FilterableSelectItem, PanelSelectPrompt};
    use crate::utils::{CliError, UtilsError};
    use crossterm::event::{KeyCode, KeyEvent, KeyModifiers};
    use std::collections::BTreeSet;

    pub struct TestTerminal {
        pub output: Vec<String>,
        inputs: Vec<String>,
        input_index: usize,
    }

    impl TestTerminal {
        pub fn new() -> Self {
            Self {
                output: Vec::new(),
                inputs: Vec::new(),
                input_index: 0,
            }
        }

        pub fn with_inputs(mut self, inputs: Vec<&str>) -> Self {
            self.inputs = inputs.into_iter().map(|s| s.to_string()).collect();
            self
        }

        fn next_input(&mut self) -> Result<String, CliError> {
            if self.input_index < self.inputs.len() {
                let input = self.inputs[self.input_index].clone();
                self.input_index += 1;
                Ok(input)
            } else {
                Err(UtilsError::UserInputFailed.into())
            }
        }
    }

    impl Terminal for TestTerminal {
        fn println(&mut self, msg: &str) {
            self.output.push(msg.to_string());
        }

        fn prompt_input(&mut self) -> Result<String, CliError> {
            self.next_input()
        }

        fn prompt_select(&mut self, _items: &[String]) -> Result<usize, CliError> {
            let input = self.next_input()?;
            input
                .parse::<usize>()
                .map_err(|_| UtilsError::UserInputFailed.into())
        }

        fn prompt_panel_select(
            &mut self,
            prompt: &PanelSelectPrompt<'_>,
        ) -> Result<usize, CliError> {
            self.output.push(prompt.title.to_string());
            self.output.extend(prompt.context_lines.iter().cloned());
            let input = self.next_input()?;
            input
                .parse::<usize>()
                .map_err(|_| UtilsError::UserInputFailed.into())
        }

        fn prompt_multi_select(
            &mut self,
            prompt: &FilterableMultiSelectPrompt<'_>,
        ) -> Result<Vec<usize>, CliError> {
            self.output.push(prompt.title.to_string());
            self.output.extend(prompt.preface_lines.iter().cloned());
            let input = self.next_input()?;
            if input.trim().is_empty() {
                return Ok(prompt.default_selected_indices.to_vec());
            }

            input
                .split(',')
                .map(|value| {
                    value
                        .trim()
                        .parse::<usize>()
                        .map_err(|_| UtilsError::UserInputFailed.into())
                })
                .collect()
        }

        fn prompt_confirm(&mut self, _prompt: &str) -> Result<bool, CliError> {
            let input = self.next_input()?;
            Ok(input.to_lowercase() == "y" || input.to_lowercase() == "yes")
        }
    }

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

        let lines = build_filterable_multi_select_lines(
            &prompt,
            "",
            &(0..selectable.len()).collect::<Vec<_>>(),
            0,
            &BTreeSet::new(),
            8,
            20,
        );

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

    #[test]
    fn raw_prompt_control_keys_include_interrupt_and_suspend() {
        assert_eq!(
            control_action_for_key(&KeyEvent::new(KeyCode::Char('c'), KeyModifiers::CONTROL)),
            Some(RawPromptControl::Interrupt)
        );
        assert_eq!(
            control_action_for_key(&KeyEvent::new(KeyCode::Char('z'), KeyModifiers::CONTROL)),
            Some(RawPromptControl::Suspend)
        );
        assert_eq!(
            control_action_for_key(&KeyEvent::new(KeyCode::Char('c'), KeyModifiers::NONE)),
            None
        );
    }
}
