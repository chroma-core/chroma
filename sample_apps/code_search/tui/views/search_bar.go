package views

import (
	"chroma-core/code-search-tui/renderer"
	"chroma-core/code-search-tui/util"
	"time"

	tea "github.com/charmbracelet/bubbletea"
)

type SearchBarModel struct {
	searchQuery string
}

func (m SearchBarModel) Init() tea.Cmd {
	return nil
}

func (m SearchBarModel) Update(msg tea.Msg) (renderer.RendererChildModel, tea.Cmd) {
	switch msg := msg.(type) {
	case tea.KeyMsg:
		switch msg.String() {
		case "backspace":
			if len(m.searchQuery) > 0 {
				m.searchQuery = m.searchQuery[:len(m.searchQuery)-1]
			}
		case "enter":
			return m, func() tea.Msg {
				return util.DoSearchMsg{SearchQuery: m.searchQuery}
			}
		default:
			if len(msg.String()) == 1 {
				m.searchQuery += msg.String()
			}
		}
	}
	return m, nil
}

func (m SearchBarModel) View(buffer [][]renderer.ASCIIPixel) {
	width := 46
	var color string
	var searchBarContents []rune
	if len(m.searchQuery) == 0 {
		color = renderer.Gray
		searchBarContents = []rune("Search your codebase...")
	} else {
		color = renderer.White
		searchBarContents = []rune(m.searchQuery)
		if time.Now().Unix()%2 == 0 {
			searchBarContents = append(searchBarContents, '█')
		}
		if len(searchBarContents) > width {
			searchBarContents = searchBarContents[len(searchBarContents)-width:]
		}
	}
	for i, c := range searchBarContents {
		renderer.SetPixel(buffer, i, 0, renderer.ASCIIPixel{Color: color, Char: c})
	}
}
