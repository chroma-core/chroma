/*
This is a Bubble-Tea-compatible TUI framework that gives you more control over
the layout.

Instead of models rendering directly to text, they are first rendered to a
buffer, then the buffer is rendered to a string.

This is useful if you want to render models over each other, or if you want
to position them absolutely within a viewport.
*/

package renderer

import (
	"strings"

	tea "github.com/charmbracelet/bubbletea"
)

type ASCIIPixel struct {
	Color string
	Char  rune
}

func SetPixel(buffer [][]ASCIIPixel, x, y int, pixel ASCIIPixel) {
	if x < 0 || y < 0 || x >= len(buffer[0]) || y >= len(buffer) {
		return
	}
	buffer[y][x] = pixel
}

type RendererChildModel interface {
	Init() tea.Cmd
	Update(msg tea.Msg) (RendererChildModel, tea.Cmd)
	View(buffer [][]ASCIIPixel)
}

type RendererModel struct {
	Width  int
	Height int
	model  RendererChildModel
}

func NewRendererModel(model RendererChildModel) RendererModel {
	return RendererModel{
		Width:  1,
		Height: 1,
		model:  model,
	}
}

func (rm RendererModel) Init() tea.Cmd {
	return rm.model.Init()
}

func (rm RendererModel) Update(msg tea.Msg) (tea.Model, tea.Cmd) {
	switch msg := msg.(type) {
	case tea.WindowSizeMsg:
		rm.Width = msg.Width
		rm.Height = msg.Height
	}
	m, cmd := rm.model.Update(msg)
	rm.model = m
	return rm, cmd
}

func (rm RendererModel) View() string {
	buffer := NewBuffer(rm.Width, rm.Height)

	rm.model.View(buffer)

	var sb strings.Builder
	sb.Grow(rm.Width * rm.Height * 4) // Pre-allocate approximate capacity
	for _, row := range buffer {
		for c, pixel := range row {
			if pixel.Char != ' ' && (c == 0 || row[c-1].Color != pixel.Color) {
				sb.WriteString(pixel.Color)
			}
			sb.WriteRune(pixel.Char)
		}
		sb.WriteString("\x1b[0m\n")
	}
	return sb.String()
}

func RenderString(buffer [][]ASCIIPixel, x, y int, str string, color string) {
	cx, cy := x, y
	for _, char := range str {
		if char == '\n' {
			cy++
			cx = x
		} else {
			SetPixel(buffer, cx, cy, ASCIIPixel{Color: color, Char: char})
			cx++
		}
	}
}
