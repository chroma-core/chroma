package views

import (
	"chroma-core/code-search-tui/renderer"

	tea "github.com/charmbracelet/bubbletea"
)

const (
	contents = `About
---

I'll fill this out later.
In the meantime, you can
drag this window around.

Please try it! It's fun!`
)

type AboutModel struct {
}

func (m AboutModel) Init() tea.Cmd {
	return nil
}

func (m AboutModel) Update(msg tea.Msg) (renderer.RendererChildModel, tea.Cmd) {
	return m, nil
}

func (m AboutModel) View(buffer [][]renderer.ASCIIPixel) {
	renderer.RenderString(buffer, 0, 0, contents, renderer.White)
}
