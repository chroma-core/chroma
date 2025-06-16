/*
This is the main page of the TUI.
It's supposed to resemble google.com where there is a logo and a search bar.
The background is a 3D scene renderer with a raycaster, using the model in
background.go.
In the foreground is a search bar.
Additionally, there are "windows" the user can open, drag to move, and exit.
*/

package views

import (
	"chroma-core/code-search-tui/renderer"
	"chroma-core/code-search-tui/util"

	tea "github.com/charmbracelet/bubbletea"
	"github.com/charmbracelet/lipgloss"
)

type WindowingSystemComponent interface {
	renderer.RendererChildModel
}

const (
	defaultState = iota
	draggingState
)

type dragEvent struct {
	window         *window
	initialMouseX  int
	initialMouseY  int
	initialWindowX int
	initialWindowY int
}

const (
	AboutWindow = iota
)

type borderStyle struct {
	Top         rune
	TopLeft     rune
	TopRight    rune
	Bottom      rune
	BottomLeft  rune
	BottomRight rune
	Left        rune
	Right       rune
}

var (
	chromaWindowBorderStyle = borderStyle{
		Top:         '═',
		TopLeft:     '╒',
		TopRight:    '╕',
		Bottom:      '─',
		BottomLeft:  '└',
		BottomRight: '┘',
		Left:        '│',
		Right:       '│',
	}
)

func stringToRune(s string) rune {
	if len(s) == 0 {
		return ' '
	}
	return []rune(s)[0]
}

func newBorderStyle(style lipgloss.Border) borderStyle {
	return borderStyle{
		Top:         stringToRune(style.Top),
		TopLeft:     stringToRune(style.TopLeft),
		TopRight:    stringToRune(style.TopRight),
		Bottom:      stringToRune(style.Bottom),
		BottomLeft:  stringToRune(style.BottomLeft),
		BottomRight: stringToRune(style.BottomRight),
		Left:        stringToRune(style.Left),
		Right:       stringToRune(style.Right),
	}
}

type window struct {
	model       WindowingSystemComponent
	borderStyle borderStyle
	x           int
	y           int
	width       int
	height      int
	focused     bool
}

func newWindow(model WindowingSystemComponent, x int, y int, width int, height int) window {
	borderStyle := chromaWindowBorderStyle
	return window{
		model:       model,
		borderStyle: borderStyle,
		x:           x,
		y:           y,
		width:       width,
		height:      height,
		focused:     false,
	}
}

func (w window) withBorderStyle(style lipgloss.Border) window {
	w.borderStyle = newBorderStyle(style)
	return w
}

func (w window) Init() tea.Cmd {
	return nil
}

func (w window) View(buffer [][]renderer.ASCIIPixel) {
	var color string
	if w.focused {
		color = renderer.White
	} else {
		color = renderer.Gray
	}
	for i := range w.height - 1 {
		renderer.SetPixel(buffer, w.x+1, w.y+i+1, renderer.ASCIIPixel{Color: color, Char: ' '})
		renderer.SetPixel(buffer, w.x+w.width-2, w.y+i+1, renderer.ASCIIPixel{Color: color, Char: ' '})
	}
	for i := range w.width {
		renderer.SetPixel(buffer, w.x+i, w.y, renderer.ASCIIPixel{Color: color, Char: w.borderStyle.Top})
		renderer.SetPixel(buffer, w.x+i, w.y+w.height-1, renderer.ASCIIPixel{Color: color, Char: w.borderStyle.Bottom})
	}
	for i := range w.height - 1 {
		renderer.SetPixel(buffer, w.x, w.y+i, renderer.ASCIIPixel{Color: color, Char: w.borderStyle.Left})
		renderer.SetPixel(buffer, w.x+w.width-1, w.y+i, renderer.ASCIIPixel{Color: color, Char: w.borderStyle.Right})
	}
	renderer.SetPixel(buffer, w.x, w.y, renderer.ASCIIPixel{Color: color, Char: w.borderStyle.TopLeft})
	renderer.SetPixel(buffer, w.x+w.width-1, w.y, renderer.ASCIIPixel{Color: color, Char: w.borderStyle.TopRight})
	renderer.SetPixel(buffer, w.x, w.y+w.height-1, renderer.ASCIIPixel{Color: color, Char: w.borderStyle.BottomLeft})
	renderer.SetPixel(buffer, w.x+w.width-1, w.y+w.height-1, renderer.ASCIIPixel{Color: color, Char: w.borderStyle.BottomRight})

	modelBuffer := renderer.NewBuffer(w.width-4, w.height-2)
	w.model.View(modelBuffer)
	for i := range modelBuffer {
		for j := range modelBuffer[i] {
			renderer.SetPixel(buffer, w.x+j+2, w.y+i+1, modelBuffer[i][j])
		}
	}
}

func (w window) Update(msg tea.Msg) (window, tea.Cmd) {
	model, cmd := w.model.Update(msg)
	w.model = model.(WindowingSystemComponent)
	return w, cmd
}

type WMModel struct {
	Width      int
	Height     int
	background renderer.RaycastSceneModel
	windows    []window
	focused    int
	state      int
	dragEvent  *dragEvent
}

func NewWMModel() WMModel {
	return WMModel{
		Width:      1,
		Height:     1,
		background: NewBackgroundModel(),
		windows: []window{
			newWindow(SearchBarModel{}, 30, 30, 50, 3).withBorderStyle(lipgloss.NormalBorder()),
		},
	}
}

func (m WMModel) Init() tea.Cmd {
	return nil
}

func (m *WMModel) mouseUpdate(msg tea.MouseMsg) tea.Cmd {
	focusedOnWindow := false
	for i := range m.windows {
		window := &m.windows[i]
		if msg.X >= window.x && msg.X < window.x+window.width && msg.Y >= window.y && msg.Y < window.y+window.height {
			m.focused = i
			window.focused = true
			focusedOnWindow = true
		} else {
			window.focused = false
		}
	}
	// Focus on search bar
	if !focusedOnWindow {
		m.focused = 0
		m.windows[0].focused = true
	}

	switch msg.Action {
	case tea.MouseActionPress:
		if msg.Button == tea.MouseButtonLeft {
			m.state = draggingState
			m.dragEvent = &dragEvent{window: &m.windows[m.focused], initialMouseX: msg.X, initialMouseY: msg.Y, initialWindowX: m.windows[m.focused].x, initialWindowY: m.windows[m.focused].y}
		}
	case tea.MouseActionRelease:
		if msg.Button == tea.MouseButtonLeft {
			m.state = defaultState
			m.dragEvent = nil
		}
	}

	if m.state == draggingState {
		m.dragEvent.window.x = m.dragEvent.initialWindowX + msg.X - m.dragEvent.initialMouseX
		m.dragEvent.window.y = m.dragEvent.initialWindowY + msg.Y - m.dragEvent.initialMouseY
	}
	return nil
}

func (m WMModel) Update(msg tea.Msg) (renderer.RendererChildModel, tea.Cmd) {
	var cmds []tea.Cmd
	switch msg := msg.(type) {
	case util.OpenWindowMsg:
		switch msg.WindowId {
		case AboutWindow:
			m.windows = append(m.windows, newWindow(AboutModel{}, 20, 20, 29, 11))
		}
	case tea.KeyMsg:
		window, cmd := m.windows[m.focused].Update(msg)
		m.windows[m.focused] = window
		cmds = append(cmds, cmd)
	case tea.WindowSizeMsg:
		m.Width = msg.Width
		m.Height = msg.Height
		searchBar := &m.windows[0]
		searchBar.x = (msg.Width - searchBar.width) / 2
		searchBar.y = (msg.Height-searchBar.height)/2 + 5
	case tea.MouseMsg:
		background, cmd := m.background.Update(msg)
		cmds = append(cmds, cmd)
		m.background = background
		cmds = append(cmds, m.mouseUpdate(msg))
	}
	return m, tea.Batch(cmds...)
}

func (m WMModel) View(buffer [][]renderer.ASCIIPixel) {
	m.background.View(buffer)
	for _, window := range m.windows {
		window.View(buffer)
	}
}
