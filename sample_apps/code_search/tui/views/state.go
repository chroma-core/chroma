package views

import (
	"chroma-core/code-search-tui/renderer"
	"chroma-core/code-search-tui/util"
	"encoding/json"
	"fmt"
	"io"

	tea "github.com/charmbracelet/bubbletea"
	"github.com/google/uuid"
)

type stateModel struct {
	id             string
	sourceCode     string
	chunkCount     int
	collectionName string
	repoName       string
	commitHash     string
}

func NewStateModel() stateModel {
	return stateModel{id: uuid.New().String()}
}

func (m stateModel) Init() tea.Cmd {
	return util.AsyncFetchCmd(m.id, "http://localhost:3001/api/state")
}

func (m stateModel) Update(msg tea.Msg) (renderer.RendererChildModel, tea.Cmd) {
	var cmds []tea.Cmd
	switch msg := msg.(type) {
	case util.AsyncFetchResultMsg:
		if msg.RecipientId != m.id || msg.Result != util.AsyncFetchSuccess {
			return m, nil
		}
		body, err := io.ReadAll(msg.Response.Body)
		if err != nil {
			return m, nil
		}
		var response util.StateResponse
		err = json.Unmarshal(body, &response)
		if err != nil {
			m.sourceCode = err.Error()
			return m, nil
		}
		m.sourceCode = response.Result.SourceCode
		m.chunkCount = response.Result.ChunkCount
		m.collectionName = response.Result.CollectionName
		m.repoName = response.Result.RepoName
		m.commitHash = response.Result.CommitHash
	}
	return m, tea.Batch(cmds...)
}

func (m stateModel) View(buffer [][]renderer.ASCIIPixel) {
	output := fmt.Sprintf("Repo name: %s\nCommit hash: %s\nChunk count: %d\n\n\nSource Code: \n\n%s", m.repoName, m.commitHash, m.chunkCount, m.sourceCode)
	renderer.RenderString(buffer, 0, 0, output, renderer.White)
}
