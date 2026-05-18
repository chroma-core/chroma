mod shared;

pub mod multi_select;
pub mod panel_select;
pub mod summary_panel;

pub(crate) use shared::{
    clear_panel, disable_raw_prompt, enable_raw_prompt, frame_panel_lines, is_interrupt,
    push_wrapped_lines, resolve_panel_origin, resolve_panel_width, truncate_line_for_width,
    visible_item_capacity, visible_window_start, wrapped_line_count, DEFAULT_MAX_VISIBLE_ITEMS,
    PANEL_MAX_WIDTH,
};
pub use shared::{
    print_command_hint, print_section_header, print_status_line, print_success_banner,
};

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
