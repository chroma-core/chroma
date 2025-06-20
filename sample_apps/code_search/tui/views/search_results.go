package views

import (
	"chroma-core/code-search-tui/util"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os/exec"
	"strings"

	"github.com/charmbracelet/bubbles/list"
	tea "github.com/charmbracelet/bubbletea"
	"github.com/charmbracelet/lipgloss"
	"github.com/google/uuid"
)

type fileRequestResult struct {
	RepoName   string `json:"repo"`
	CommitHash string `json:"commit"`
	FilePath   string `json:"path"`
	Content    string `json:"content"`
}

type setCodePreviewMsg struct {
	result util.SearchResult
}

type codePreviewModel struct {
	stateId        string
	fileId         string
	loading        bool
	currentResult  util.SearchResult
	fileResult     fileRequestResult
	scrollPosition int
	height         int
}

func newCodePreviewModel() codePreviewModel {
	return codePreviewModel{
		stateId: uuid.New().String(),
		fileId:  uuid.New().String(),
		loading: true,
	}
}

func (m codePreviewModel) Init() tea.Cmd {
	return nil
}

func (m codePreviewModel) Update(msg tea.Msg) (tea.Model, tea.Cmd) {
	var cmds []tea.Cmd
	switch msg := msg.(type) {
	case setCodePreviewMsg:
		m.currentResult = msg.result
		m.loading = true
		cmds = append(cmds, util.AsyncFetchCmd(m.fileId, fmt.Sprintf("http://localhost:3001/api/file?&path=%s", msg.result.FilePath)))
	case util.AsyncFetchResultMsg:
		if msg.RecipientId != m.fileId || msg.Result != util.AsyncFetchSuccess {
			return m, tea.Batch(cmds...)
		}
		body, err := io.ReadAll(msg.Response.Body)
		if err != nil {
			return m, tea.Batch(cmds...)
		}
		var response fileRequestResult
		err = json.Unmarshal(body, &response)
		if err != nil {
			return m, tea.Batch(cmds...)
		}
		m.fileResult = response
		m.loading = false

		// Auto-scroll to center the highlighted code snippet
		if m.height > 0 {
			startLine := m.currentResult.StartLine - 1 // Convert to 0-based
			visibleLines := m.height - 2               // Account for padding/borders
			if visibleLines > 0 {
				// Center the start line in the view
				m.scrollPosition = startLine - visibleLines/2
				if m.scrollPosition < 0 {
					m.scrollPosition = 0
				}
			}
		}
	}
	return m, tea.Batch(cmds...)
}

func (m codePreviewModel) View() string {
	if m.loading {
		return "Loading..."
	}

	// Split content into lines
	lines := strings.Split(m.fileResult.Content, "\n")

	// Calculate how many lines to highlight based on SourceCode
	sourceLines := strings.Split(strings.TrimSpace(m.currentResult.SourceCode), "\n")
	numLinesToHighlight := len(sourceLines)

	// Create style for highlighted lines with green background
	highlightStyle := lipgloss.NewStyle().Background(lipgloss.Color("#1E883E"))

	// Apply highlighting to the relevant lines
	startIdx := m.currentResult.StartLine
	if startIdx >= 0 && startIdx < len(lines) {
		for i := 0; i < numLinesToHighlight && startIdx+i < len(lines); i++ {
			lines[startIdx+i] = highlightStyle.Render(lines[startIdx+i])
		}
	}

	// Apply scrolling - show only the visible portion
	if m.height > 0 {
		visibleLines := m.height - 2 // Account for padding/borders
		endIdx := m.scrollPosition + visibleLines

		// Ensure we don't go out of bounds
		if m.scrollPosition >= len(lines) {
			m.scrollPosition = len(lines) - visibleLines
		}
		if m.scrollPosition < 0 {
			m.scrollPosition = 0
		}
		if endIdx > len(lines) {
			endIdx = len(lines)
		}

		// Return only the visible lines
		if m.scrollPosition < len(lines) {
			visibleLines := lines[m.scrollPosition:endIdx]
			return strings.Join(visibleLines, "\n")
		}
	}

	return strings.Join(lines, "\n")
}

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
	return m.ResultsModel.Init()
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
	id            string
	width         int
	height        int
	SearchResults []util.SearchResult
	SelectedIndex int
	list          list.Model
	State         util.State
	codePreview   tea.Model
}

func NewActualSearchResultsModel() ActualSearchResultsModel {
	return ActualSearchResultsModel{
		id:            uuid.New().String(),
		width:         1,
		height:        1,
		SearchResults: []util.SearchResult{},
		SelectedIndex: 0,
		list:          list.New([]list.Item{}, list.NewDefaultDelegate(), 1, 1),
		codePreview:   newCodePreviewModel(),
	}
}

func (m ActualSearchResultsModel) Init() tea.Cmd {
	cmds := []tea.Cmd{
		m.codePreview.Init(),
		util.AsyncFetchCmd(m.id, "http://localhost:3001/api/state"),
	}
	return tea.Batch(cmds...)
}

func (m ActualSearchResultsModel) Update(msg tea.Msg) (tea.Model, tea.Cmd) {
	var cmds []tea.Cmd
	selectedItemChanged := false
	switch msg := msg.(type) {
	case util.AsyncFetchResultMsg:
		if msg.RecipientId != m.id || msg.Result != util.AsyncFetchSuccess {
			var cmd tea.Cmd
			m.codePreview, cmd = m.codePreview.Update(msg)
			cmds = append(cmds, cmd)
			return m, tea.Batch(cmds...)
		}
		body, err := io.ReadAll(msg.Response.Body)
		if err != nil {
			return m, nil
		}
		var response util.StateResponse
		err = json.Unmarshal(body, &response)
		if err != nil {
			return m, nil
		}
		m.State = response.Result
	case tea.WindowSizeMsg:
		m.width = msg.Width
		m.height = msg.Height
		m.list.SetSize(m.width/2, m.height)
		if codePreview, ok := m.codePreview.(codePreviewModel); ok {
			codePreview.height = m.height - 5
			m.codePreview = codePreview
		}
	case tea.KeyMsg:
		if msg.String() == "up" {
			if m.SelectedIndex > 0 {
				m.SelectedIndex--
				selectedItemChanged = true
			}
		}
		if msg.String() == "down" {
			if m.SelectedIndex < len(m.SearchResults)-1 {
				m.SelectedIndex++
				selectedItemChanged = true
			}
		}
		if msg.String() == "enter" {
			selectedItem := m.list.SelectedItem()
			if searchResult, ok := selectedItem.(util.SearchResult); ok {
				cmds = append(cmds, openGithubRepo(m.State.RepoName, m.State.CommitHash, searchResult.FilePath, searchResult.StartLine))
			}
		}
	case util.SearchResultsReceivedMsg:
		m.SearchResults = msg.Results
		items := make([]list.Item, len(m.SearchResults))
		for i, result := range m.SearchResults {
			items[i] = result
		}
		m.list.SetItems(items)
		selectedItemChanged = true
	}

	if selectedItemChanged {
		newModel, cmd := m.codePreview.Update(setCodePreviewMsg{result: m.SearchResults[m.SelectedIndex]})
		cmds = append(cmds, cmd)
		m.codePreview = newModel.(codePreviewModel)
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

	padding := 4
	leftStyle := lipgloss.NewStyle().
		Width(m.width/2 - 20 - padding - gap/2).
		MaxWidth(m.width/2 - 20 - gap/2).
		Height(m.height - padding).
		MaxHeight(m.height)

	rightStyle := lipgloss.NewStyle().
		Width(m.width/2 + 20 - padding - gap/2).
		MaxWidth(m.width/2 + 20 - gap/2).
		Height(m.height - padding).
		MaxHeight(m.height)

	list := leftStyle.Render(m.list.View())

	code := rightStyle.
		Padding(padding).
		BorderStyle(lipgloss.NormalBorder()).
		BorderForeground(lipgloss.Color("240")).
		Render(m.codePreview.View())

	content := lipgloss.JoinHorizontal(lipgloss.Center, list, strings.Repeat(" ", gap), code)
	return style.Render(content)
}

func openGithubRepo(repoName string, commitHash string, file string, lineNumber int) tea.Cmd {
	url := fmt.Sprintf("https://github.com/%s/blob/%s/%s#L%d", repoName, commitHash, file, lineNumber)
	return func() tea.Msg {
		exec.Command("open", url).Run()
		return nil
	}
}
