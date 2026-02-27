package http

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net/http"
)

// ChromaError represents an error returned by the Chroma API. It contains the ID of the error, the error message and the status code from the HTTP call.
// Example:
//
//	{
//	 "error": "NotFoundError",
//	 "message": "Tenant default_tenant2 not found"
//	}
type ChromaError struct {
	ErrorID   string `json:"error"`
	ErrorCode int    `json:"error_code"`
	Message   string `json:"message"`
}

func ChromaErrorFromHTTPResponse(resp *http.Response, err error) *ChromaError {
	chromaAPIError := &ChromaError{
		ErrorID: "unknown",
		Message: "unknown",
	}
	if err != nil {
		chromaAPIError.Message = err.Error()
	}
	if resp == nil {
		return chromaAPIError
	}
	defer func() { _ = resp.Body.Close() }()
	chromaAPIError.ErrorCode = resp.StatusCode

	// Read body into buffer first to allow fallback if JSON decode fails
	bodyBytes, readErr := ReadLimitedBody(resp.Body)
	if readErr != nil {
		return chromaAPIError
	}

	if err := json.NewDecoder(bytes.NewReader(bodyBytes)).Decode(chromaAPIError); err != nil {
		chromaAPIError.Message = string(bodyBytes)
	}
	return chromaAPIError
}

func (e *ChromaError) Error() string {
	return fmt.Sprintf("Error (%d) %s: %s", e.ErrorCode, e.ErrorID, e.Message)
}
