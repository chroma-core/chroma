package http

import "net/http"

type RetryStrategy interface {
	DoWithRetry(client *http.Client, req *http.Request) (*http.Response, error)
}
