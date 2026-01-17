package ollama

import (
	"net/url"

	"github.com/pkg/errors"

	"github.com/chroma-core/chroma/clients/go/pkg/embeddings"
)

type Option func(p *OllamaClient) error

func WithBaseURL(baseURL string) Option {
	return func(p *OllamaClient) error {
		if baseURL == "" {
			return errors.New("base URL cannot be empty")
		}
		if _, err := url.ParseRequestURI(baseURL); err != nil {
			return errors.Wrap(err, "invalid base URL")
		}
		p.BaseURL = baseURL
		return nil
	}
}
func WithModel(model embeddings.EmbeddingModel) Option {
	return func(p *OllamaClient) error {
		if model == "" {
			return errors.New("model cannot be empty")
		}
		p.Model = model
		return nil
	}
}
