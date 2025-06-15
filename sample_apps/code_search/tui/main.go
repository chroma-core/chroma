package main

import (
	"fmt"
	"os"

	"chroma-core/code-search-tui/views"

	"github.com/charmbracelet/bubbles/help"
	"github.com/charmbracelet/bubbles/key"
	"github.com/charmbracelet/bubbles/textarea"
	tea "github.com/charmbracelet/bubbletea"
)

type keymap = struct {
	quit key.Binding
}

type rootModel struct {
	width  int
	height int
	keymap keymap
	help   help.Model
	logo   views.LogoModel
}

func newModel() rootModel {
	m := rootModel{
		help: help.New(),
		keymap: keymap{
			quit: key.NewBinding(
				key.WithKeys("esc", "ctrl+c"),
				key.WithHelp("esc", "quit"),
			),
		},
		logo: views.NewLogoModel(),
	}
	return m
}

func (m rootModel) Init() tea.Cmd {
	return textarea.Blink
}

func (m rootModel) Update(msg tea.Msg) (tea.Model, tea.Cmd) {
	var cmds []tea.Cmd

	switch msg := msg.(type) {
	case tea.KeyMsg:
		switch {
		case key.Matches(msg, m.keymap.quit):
			return m, tea.Quit
		}
	case tea.WindowSizeMsg:
		m.height = msg.Height
		m.width = msg.Width
		m.logo.Width = msg.Width
		m.logo.Height = msg.Height
	}

	var cmd tea.Cmd
	m.logo, cmd = m.logo.Update(msg)
	if cmd != nil {
		cmds = append(cmds, cmd)
	}

	return m, tea.Batch(cmds...)
}

func (m rootModel) View() string {
	help := m.help.ShortHelpView([]key.Binding{
		m.keymap.quit,
	})

	logo := m.logo.View()

	return logo + "\n\n" + help
}

func main() {
	if _, err := tea.NewProgram(newModel(), tea.WithAltScreen(), tea.WithMouseAllMotion()).Run(); err != nil {
		fmt.Println("Error while running program:", err)
		os.Exit(1)
	}
}
