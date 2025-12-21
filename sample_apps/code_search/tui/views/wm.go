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
	resizingState
)

type dragEvent struct {
	window              *window
	initialMouseX       int
	initialMouseY       int
	initialWindowX      int
	initialWindowY      int
	initialWindowWidth  int
	initialWindowHeight int
	isResizing          bool
}

const (
	AboutWindow = iota
	StateWindow
)

const (
	minWindowWidth   = 20
	minWindowHeight  = 5
	resizeHandleSize = 1 // Size of the resize handle area in the bottom right corner
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
	closable    bool // whether this window can be closed
}

func newWindow(model WindowingSystemComponent, x int, y int, width int, height int, closable bool) window {
	borderStyle := chromaWindowBorderStyle
	return window{
		model:       model,
		borderStyle: borderStyle,
		x:           x,
		y:           y,
		width:       width,
		height:      height,
		focused:     false,
		closable:    closable,
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

	// Draw close button in top left corner for closable windows
	if w.closable && w.width > 3 { // Only show close button if window is closable and wide enough
		closeButtonColor := color
		if w.focused {
			closeButtonColor = renderer.Red // Make close button red when focused
		}
		renderer.SetPixel(buffer, w.x+1, w.y, renderer.ASCIIPixel{Color: closeButtonColor, Char: '×'})
	}

	// Draw resize handle in bottom right corner (use different character to indicate resize)
	resizeHandleColor := color
	if w.focused {
		resizeHandleColor = renderer.Yellow // Make resize handle more visible when focused
	}
	renderer.SetPixel(buffer, w.x+w.width-1, w.y+w.height-1, renderer.ASCIIPixel{Color: resizeHandleColor, Char: '◢'})

	// Ensure minimum buffer size for the window content
	bufferWidth := w.width - 4
	bufferHeight := w.height - 2
	if bufferWidth < 1 {
		bufferWidth = 1
	}
	if bufferHeight < 1 {
		bufferHeight = 1
	}

	modelBuffer := renderer.NewBuffer(bufferWidth, bufferHeight)
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
			newWindow(SearchBarModel{}, 30, 30, 50, 3, false).withBorderStyle(lipgloss.NormalBorder()), // Search bar is not closable
		},
	}
}

func (m WMModel) Init() tea.Cmd {
	cmds := []tea.Cmd{}
	cmds = append(cmds, m.background.Init())
	cmds = append(cmds, m.windows[0].Init())
	return tea.Batch(cmds...)
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
			focusedWindow := &m.windows[m.focused]

			// Check if clicking on close button for closable windows
			if focusedWindow.closable && isMouseOverCloseButton(focusedWindow, msg.X, msg.Y) {
				// Remove the window from the slice
				if m.focused > 0 { // Never remove the search bar (index 0)
					m.windows = append(m.windows[:m.focused], m.windows[m.focused+1:]...)
					// Adjust focused index if necessary
					if m.focused >= len(m.windows) {
						m.focused = len(m.windows) - 1
					}
					// Ensure we focus on a valid window
					if m.focused >= 0 && m.focused < len(m.windows) {
						for i := range m.windows {
							m.windows[i].focused = (i == m.focused)
						}
					}
				}
			} else if isMouseOverResizeHandle(focusedWindow, msg.X, msg.Y) {
				m.state = resizingState
				m.dragEvent = &dragEvent{
					window:              focusedWindow,
					initialMouseX:       msg.X,
					initialMouseY:       msg.Y,
					initialWindowX:      focusedWindow.x,
					initialWindowY:      focusedWindow.y,
					initialWindowWidth:  focusedWindow.width,
					initialWindowHeight: focusedWindow.height,
					isResizing:          true,
				}
			} else {
				m.state = draggingState
				m.dragEvent = &dragEvent{
					window:              focusedWindow,
					initialMouseX:       msg.X,
					initialMouseY:       msg.Y,
					initialWindowX:      focusedWindow.x,
					initialWindowY:      focusedWindow.y,
					initialWindowWidth:  focusedWindow.width,
					initialWindowHeight: focusedWindow.height,
					isResizing:          false,
				}
			}
		}
	case tea.MouseActionRelease:
		if msg.Button == tea.MouseButtonLeft {
			m.state = defaultState
			m.dragEvent = nil
		}
	}

	if m.state == draggingState && m.dragEvent != nil && !m.dragEvent.isResizing {
		m.dragEvent.window.x = m.dragEvent.initialWindowX + msg.X - m.dragEvent.initialMouseX
		m.dragEvent.window.y = m.dragEvent.initialWindowY + msg.Y - m.dragEvent.initialMouseY
	} else if m.state == resizingState && m.dragEvent != nil && m.dragEvent.isResizing {
		// Calculate new size based on mouse movement
		newWidth := m.dragEvent.initialWindowWidth + msg.X - m.dragEvent.initialMouseX
		newHeight := m.dragEvent.initialWindowHeight + msg.Y - m.dragEvent.initialMouseY

		// Apply minimum size constraints
		if newWidth < minWindowWidth {
			newWidth = minWindowWidth
		}
		if newHeight < minWindowHeight {
			newHeight = minWindowHeight
		}

		m.dragEvent.window.width = newWidth
		m.dragEvent.window.height = newHeight
	}
	return nil
}

func (m WMModel) Update(msg tea.Msg) (renderer.RendererChildModel, tea.Cmd) {
	var cmds []tea.Cmd
	switch msg := msg.(type) {
	case util.OpenWindowMsg:
		var newWindowModel WindowingSystemComponent
		var newWindowWidth int
		var newWindowHeight int
		switch msg.WindowId {
		case AboutWindow:
			newWindowModel = AboutModel{}
			newWindowWidth = 29
			newWindowHeight = 11
		case StateWindow:
			newWindowModel = NewStateModel()
			newWindowWidth = 60
			newWindowHeight = 40
		}
		m.windows = append(m.windows, newWindow(newWindowModel, 20, 20, newWindowWidth, newWindowHeight, true)) // New windows are closable
		cmds = append(cmds, newWindowModel.Init())
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
		cmds = append(cmds, m.mouseUpdate(msg))
	case util.AsyncFetchResultMsg:
		var cmd tea.Cmd
		m.background, cmd = m.background.Update(msg)
		cmds = append(cmds, cmd)
		for i := range m.windows {
			m.windows[i], cmd = m.windows[i].Update(msg)
			cmds = append(cmds, cmd)
		}
		return m, tea.Batch(cmds...)
	}

	background, cmd := m.background.Update(msg)
	cmds = append(cmds, cmd)
	m.background = background

	return m, tea.Batch(cmds...)
}

func (m WMModel) View(buffer [][]renderer.ASCIIPixel) {
	m.background.View(buffer)
	for _, window := range m.windows {
		window.View(buffer)
	}
}

// isMouseOverResizeHandle checks if the mouse is over the resize handle (bottom right corner)
func isMouseOverResizeHandle(window *window, mouseX, mouseY int) bool {
	return mouseX >= window.x+window.width-resizeHandleSize-1 &&
		mouseX < window.x+window.width &&
		mouseY >= window.y+window.height-resizeHandleSize-1 &&
		mouseY < window.y+window.height
}

// isMouseOverCloseButton checks if the mouse is over the close button (top left corner)
func isMouseOverCloseButton(window *window, mouseX, mouseY int) bool {
	return mouseX >= window.x &&
		mouseX <= window.x+2 &&
		mouseY == window.y
}
