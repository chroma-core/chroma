package main

import (
	"fmt"
	"os"

	"chroma-core/code-search-tui/renderer"
	"chroma-core/code-search-tui/util"
	"chroma-core/code-search-tui/views"

	"github.com/charmbracelet/bubbles/help"
	"github.com/charmbracelet/bubbles/key"
	"github.com/charmbracelet/bubbles/textarea"
	tea "github.com/charmbracelet/bubbletea"
)

const (
	SearchBarScreen = iota
	SearchResultsScreen
)

type keymap = struct {
	quit  key.Binding
	about key.Binding
}

type ScreenModel interface {
	tea.Model
}

type rootModel struct {
	width    int
	height   int
	keymap   keymap
	help     help.Model
	tooSmall views.TooSmallModel
	screens  []tea.Model
	active   int
	debugMsg string
}

func newModel() rootModel {
	m := rootModel{
		help: help.New(),
		keymap: keymap{
			quit: key.NewBinding(
				key.WithKeys("esc", "ctrl+c"),
				key.WithHelp("esc", "quit"),
			),
			about: key.NewBinding(
				key.WithKeys("ctrl+a"),
				key.WithHelp("ctrl+a", "about"),
			),
		},
		tooSmall: views.NewTooSmallModel(),
		screens: []tea.Model{
			renderer.NewRendererModel(views.NewWMModel()),
			views.NewSearchResultsModel(),
		},
		active: SearchBarScreen,
	}
	return m
}

func (m rootModel) Init() tea.Cmd {
	return textarea.Blink
}

func (m rootModel) Update(msg tea.Msg) (tea.Model, tea.Cmd) {
	var cmds []tea.Cmd
	var cmd tea.Cmd

	switch msg := msg.(type) {
	case util.DoSearchMsg:
		m.active = SearchResultsScreen
	case util.DebugMsg:
		m.debugMsg = msg.Msg
		return m, nil
	case tea.KeyMsg:
		switch {
		case key.Matches(msg, m.keymap.quit):
			if m.active == SearchBarScreen {
				return m, tea.Quit
			} else {
				m.active = SearchBarScreen
			}
		case key.Matches(msg, m.keymap.about):
			cmds = append(cmds, func() tea.Msg {
				return util.OpenWindowMsg{WindowId: views.AboutWindow}
			})
		}
	case tea.WindowSizeMsg:
		m.height = msg.Height
		m.width = msg.Width
		newCmd := tea.WindowSizeMsg{
			Width:  msg.Width,
			Height: msg.Height - 3,
		}
		for i := range m.screens {
			m.screens[i], cmd = m.screens[i].Update(newCmd)
			cmds = append(cmds, cmd)
		}
		m.tooSmall.SetSize(msg.Width, msg.Height-2)
		return m, nil
	case util.OpenWindowMsg:
		newModel, cmd := m.screens[0].Update(msg)
		m.screens[0] = newModel.(ScreenModel)
		cmds = append(cmds, cmd)
		return m, tea.Batch(cmds...)
	}

	focusedScreen := m.screens[m.active]
	newModel, cmd := focusedScreen.Update(msg)
	m.screens[m.active] = newModel.(ScreenModel)
	cmds = append(cmds, cmd)

	return m, tea.Batch(cmds...)
}

func (m rootModel) isTooSmall() bool {
	return m.width < 80 || m.height < 20
}

func (m rootModel) View() string {
	help := m.help.ShortHelpView([]key.Binding{
		m.keymap.quit,
		m.keymap.about,
	})

	if m.isTooSmall() {
		return m.tooSmall.View() + "\n\n " + help
	}

	focusedScreen := m.screens[m.active]
	return focusedScreen.View() + "\n\n " + help + " " + m.debugMsg
}

func main() {
	if _, err := tea.NewProgram(newModel(), tea.WithAltScreen(), tea.WithMouseAllMotion()).Run(); err != nil {
		fmt.Println("Error while running program:", err)
		os.Exit(1)
	}
}
