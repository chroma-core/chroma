package util

type DebugMsg struct {
	Msg string
}

type TickMsg struct {
	Id int
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
