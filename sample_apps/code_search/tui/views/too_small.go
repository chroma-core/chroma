package views

import (
	tea "github.com/charmbracelet/bubbletea"
	"github.com/charmbracelet/lipgloss"
)

type TooSmallModel struct {
	Width  int
	Height int
}

func NewTooSmallModel() TooSmallModel {
	return TooSmallModel{Width: 0, Height: 0}
}

func (m *TooSmallModel) SetSize(width, height int) {
	m.Width = width
	m.Height = height
}

func (m *TooSmallModel) GetSize() (int, int) {
	return m.Width, m.Height
}

var style = lipgloss.NewStyle().
	Align(lipgloss.Center).
	BorderStyle(lipgloss.DoubleBorder())

func (m TooSmallModel) View() string {
	return style.
		Width(m.Width - 2).
		Height(m.Height - 2).
		Render("Your window is too small!")
}

func (m TooSmallModel) Update(msg tea.Msg) (TooSmallModel, tea.Cmd) {
	return m, nil
}
