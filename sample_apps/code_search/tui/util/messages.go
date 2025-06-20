package util

import (
	"net/http"

	tea "github.com/charmbracelet/bubbletea"
)

type DebugMsg struct {
	Msg string
}

type TickMsg struct {
	Id string
}

type OpenWindowMsg struct {
	WindowId int
}

type DoSearchMsg struct {
	SearchQuery string
}

type SearchResultsReceivedMsg struct {
	Results []SearchResult
	Query   string
}

type SearchErrorMsg struct {
	Error string
	Query string
}

const (
	AsyncFetchSuccess = iota
	AsyncFetchError
)

type AsyncFetchResultMsg struct {
	RecipientId string
	Result      int
	Error       error
	Response    *http.Response
}

func AsyncFetchCmd(recipientId string, url string) tea.Cmd {
	return func() tea.Msg {
		resp, err := http.Get(url)
		if err != nil {
			return AsyncFetchResultMsg{RecipientId: recipientId, Result: AsyncFetchError, Error: err}
		}
		return AsyncFetchResultMsg{RecipientId: recipientId, Result: AsyncFetchSuccess, Response: resp}
	}
}
