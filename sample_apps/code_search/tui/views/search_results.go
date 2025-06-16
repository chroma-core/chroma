package views

import (
	"chroma-core/code-search-tui/util"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"

	"github.com/charmbracelet/bubbles/list"
	tea "github.com/charmbracelet/bubbletea"
	"github.com/charmbracelet/lipgloss"
)

const (
	Loading = iota
	Loaded
	Error
)

type SearchResultsModel struct {
	width        int
	height       int
	searchQuery  string
	State        int
	ResultsModel tea.Model
	Error        string
}

func NewSearchResultsModel() SearchResultsModel {
	return SearchResultsModel{
		State:        Loading,
		ResultsModel: NewActualSearchResultsModel(),
	}
}

func (m SearchResultsModel) Init() tea.Cmd {
	return nil
}

func (m SearchResultsModel) Update(msg tea.Msg) (tea.Model, tea.Cmd) {
	var cmds []tea.Cmd
	switch msg := msg.(type) {
	case tea.WindowSizeMsg:
		m.width = msg.Width
		m.height = msg.Height
	case util.DoSearchMsg:
		m.State = Loading
		cmds = append(cmds, fetchSearchResults(msg.SearchQuery))
	case util.SearchResultsReceivedMsg:
		m.State = Loaded
	case util.SearchErrorMsg:
		m.State = Error
		m.Error = msg.Error
		m.searchQuery = msg.Query
	}
	var cmd tea.Cmd
	m.ResultsModel, cmd = m.ResultsModel.Update(msg)
	cmds = append(cmds, cmd)
	return m, tea.Batch(cmds...)
}

func fetchSearchResults(searchQuery string) tea.Cmd {
	var cmds []tea.Cmd
	url := fmt.Sprintf("http://localhost:3001/api/query?q=%s", url.QueryEscape(searchQuery))
	resp, err := http.Get(url)
	if err != nil {
		return func() tea.Msg {
			return util.SearchErrorMsg{Error: err.Error(), Query: searchQuery}
		}
	} else {
		body, err := io.ReadAll(resp.Body)
		if err != nil {
			return func() tea.Msg {
				return util.SearchErrorMsg{Error: err.Error(), Query: searchQuery}
			}
		}
		var response util.SearchResultsResponse
		err = json.Unmarshal(body, &response)
		if err != nil {
			return func() tea.Msg {
				return util.SearchErrorMsg{Error: err.Error(), Query: searchQuery}
			}
		}
		cmds = append(cmds, func() tea.Msg {
			return util.SearchResultsReceivedMsg{Results: response.Results, Query: searchQuery}
		})
		return tea.Batch(cmds...)
	}
}

func (m SearchResultsModel) View() string {
	if m.State == Loading {
		return "Loading..."
	}
	if m.State == Error {
		return fmt.Sprintf("Error: %s", m.Error)
	}
	return m.ResultsModel.View()
}

type ActualSearchResultsModel struct {
	width         int
	height        int
	SearchResults []util.SearchResult
	SelectedIndex int
	list          list.Model
}

func NewActualSearchResultsModel() ActualSearchResultsModel {
	return ActualSearchResultsModel{
		width:         1,
		height:        1,
		SearchResults: []util.SearchResult{},
		SelectedIndex: 0,
		list:          list.New([]list.Item{}, list.NewDefaultDelegate(), 1, 1),
	}
}

func (m ActualSearchResultsModel) Init() tea.Cmd {
	return nil
}

func (m ActualSearchResultsModel) Update(msg tea.Msg) (tea.Model, tea.Cmd) {
	var cmds []tea.Cmd
	switch msg := msg.(type) {
	case tea.WindowSizeMsg:
		m.width = msg.Width
		m.height = msg.Height
		m.list.SetSize(m.width/2, m.height)
	case tea.KeyMsg:
		if msg.String() == "up" {
			if m.SelectedIndex > 0 {
				m.SelectedIndex--
			}
		}
		if msg.String() == "down" {
			if m.SelectedIndex < len(m.SearchResults)-1 {
				m.SelectedIndex++
			}
		}
	case util.SearchResultsReceivedMsg:
		m.SearchResults = msg.Results
		items := make([]list.Item, len(m.SearchResults))
		for i, result := range m.SearchResults {
			items[i] = result
		}
		m.list.SetItems(items)
	}
	var cmd tea.Cmd
	m.list, cmd = m.list.Update(msg)
	cmds = append(cmds, cmd)
	return m, tea.Batch(cmds...)
}

func (m ActualSearchResultsModel) View() string {
	if len(m.SearchResults) == 0 {
		return ""
	}
	style := lipgloss.NewStyle().
		Width(m.width).
		Height(m.height)
	gap := 8

	selectedItem := m.list.SelectedItem()
	var selected util.SearchResult
	if searchResult, ok := selectedItem.(util.SearchResult); ok {
		selected = searchResult
	} else {
		selected = m.SearchResults[0]
	}

	padding := 4
	halfStyle := lipgloss.NewStyle().
		Width(m.width/2 - padding - gap/2).
		MaxWidth(m.width/2 - gap/2).
		Height(m.height - padding).
		MaxHeight(m.height)

	list := halfStyle.Render(m.list.View())

	code := halfStyle.
		Padding(padding).
		BorderStyle(lipgloss.NormalBorder()).
		BorderForeground(lipgloss.Color("240")).
		Render(selected.SourceCode)

	content := lipgloss.JoinHorizontal(lipgloss.Center, list, strings.Repeat(" ", gap), code)
	return style.Render(content)
}
