package util

type SearchResult struct {
	FilePath      string      `json:"file_path"`
	IndexDocument interface{} `json:"index_document"`
	Language      string      `json:"language"`
	Name          string      `json:"name"`
	SourceCode    string      `json:"source_code"`
	StartLine     int         `json:"start_line"`
}

func (i SearchResult) Title() string       { return i.Name }
func (i SearchResult) Description() string { return i.FilePath }
func (i SearchResult) FilterValue() string { return i.SourceCode }

type SearchResultsResponse struct {
	Results []SearchResult `json:"result"`
}
