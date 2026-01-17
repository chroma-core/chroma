package http

import "io"

func ReadRespBody(resp io.Reader) string {
	if resp == nil {
		return ""
	}
	body, err := io.ReadAll(resp)
	if err != nil {
		return ""
	}
	return string(body)
}
