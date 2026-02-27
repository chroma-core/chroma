package http

import (
	"fmt"
	"io"
)

// MaxResponseBodySize is the maximum allowed response body size (200 MB).
const MaxResponseBodySize = 200 * 1024 * 1024

// ReadLimitedBody reads up to MaxResponseBodySize bytes from r.
// Returns an error if the response exceeds the limit.
func ReadLimitedBody(r io.Reader) ([]byte, error) {
	limitedReader := io.LimitReader(r, int64(MaxResponseBodySize)+1)
	data, err := io.ReadAll(limitedReader)
	if err != nil {
		return nil, err
	}
	if len(data) > MaxResponseBodySize {
		return nil, fmt.Errorf("response body exceeds maximum size of %d bytes", MaxResponseBodySize)
	}
	return data, nil
}

func ReadRespBody(resp io.Reader) (string, error) {
	if resp == nil {
		return "", nil
	}
	body, err := io.ReadAll(resp)
	if err != nil {
		return "", err
	}
	return string(body), nil
}
