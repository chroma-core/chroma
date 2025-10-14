use ratatui::buffer::Buffer;
use ratatui::layout::Rect;
use ratatui::style::Style;
use ratatui::widgets::{Block, Borders, Paragraph, StatefulWidget, Widget};
use tui_input::Input;

pub struct InputBox {
    block: Block<'static>,
    active_style: Style,
    active: bool,
}

impl InputBox {
    pub fn new(active_style: Style) -> InputBox {
        Self {
            block: Block::default().borders(Borders::ALL),
            active_style,
            active: false,
        }
    }

    pub fn active(mut self) -> Self {
        self.active = true;
        self.block = self.block.border_style(self.active_style);
        self
    }

    pub fn title(mut self, title: String) -> Self {
        self.block = self.block.title(title);
        self
    }
}

impl StatefulWidget for InputBox {
    type State = Input;

    fn render(self, area: Rect, buf: &mut Buffer, state: &mut Self::State) {
        let width = area.width.saturating_sub(3) as usize;
        let scroll = state.visual_scroll(width);
        let p = Paragraph::new(state.value())
            .block(self.block)
            .scroll((0, scroll as u16));
        p.render(area, buf);
    }
}

#[derive(Debug, Clone)]
pub struct ToggleState {
    pub selected: usize,
}

impl ToggleState {
    pub fn new() -> ToggleState {
        Self { selected: 0 }
    }
}

pub struct ToggleButton<'a, T: ToString> {
    block: Block<'static>,
    active: bool,
    active_style: Style,
    options: &'a [T],
}

impl<'a, T: ToString> ToggleButton<'a, T> {
    pub fn new(options: &'a [T], active_style: Style) -> Self {
        Self {
            options,
            block: Block::default().borders(Borders::ALL),
            active_style,
            active: false,
        }
    }

    pub fn active(mut self) -> Self {
        self.block = self.block.border_style(self.active_style);
        self.active = true;
        self
    }
}

impl<T: ToString> StatefulWidget for ToggleButton<'_, T> {
    type State = ToggleState;

    fn render(self, area: Rect, buf: &mut Buffer, state: &mut Self::State) {
        let content = self.options[state.selected].to_string();
        let p = Paragraph::new(content).block(self.block).centered();
        p.render(area, buf);
    }
}
