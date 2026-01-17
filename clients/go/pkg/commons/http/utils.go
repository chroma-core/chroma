package http

import "io"

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
