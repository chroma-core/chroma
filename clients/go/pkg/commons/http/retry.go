package http

import (
	"bytes"
	"errors"
	"io"
	"math"
	"net/http"
	"slices"
	"time"
)

type Option func(*SimpleRetryStrategy) error

func WithMaxRetries(retries int) Option {
	return func(r *SimpleRetryStrategy) error {
		if retries <= 0 {
			return errors.New("retries must be a positive integer")
		}
		r.MaxRetries = retries
		return nil
	}
}

func WithFixedDelay(delay time.Duration) Option {
	return func(r *SimpleRetryStrategy) error {
		if delay <= 0 {
			return errors.New("delay must be a positive integer")
		}
		r.FixedDelay = delay
		return nil
	}
}

func WithRetryableStatusCodes(statusCodes ...int) Option {
	return func(r *SimpleRetryStrategy) error {
		r.RetryableStatusCodes = statusCodes
		return nil
	}
}

func WithExponentialBackOff() Option {
	return func(r *SimpleRetryStrategy) error {
		r.ExponentialBackOff = true
		return nil
	}
}

type SimpleRetryStrategy struct {
	MaxRetries           int
	FixedDelay           time.Duration
	ExponentialBackOff   bool
	RetryableStatusCodes []int
}

func NewSimpleRetryStrategy(opts ...Option) (*SimpleRetryStrategy, error) {
	var strategy = &SimpleRetryStrategy{
		MaxRetries:           3,
		FixedDelay:           time.Duration(1000) * time.Millisecond,
		RetryableStatusCodes: []int{},
	}
	for _, opt := range opts {
		if err := opt(strategy); err != nil {
			return nil, err
		}
	}
	return strategy, nil
}

func (r *SimpleRetryStrategy) DoWithRetry(client *http.Client, req *http.Request) (*http.Response, error) {
	var bodyBytes []byte
	if req.Body != nil {
		var readErr error
		bodyBytes, readErr = io.ReadAll(req.Body)
		closeErr := req.Body.Close()
		if err := errors.Join(readErr, closeErr); err != nil {
			return nil, err
		}
		req.Body = io.NopCloser(bytes.NewReader(bodyBytes))
		req.GetBody = func() (io.ReadCloser, error) {
			return io.NopCloser(bytes.NewReader(bodyBytes)), nil
		}
	}

	var resp *http.Response
	var err error
	for i := 0; i < r.MaxRetries; i++ {
		if i > 0 && bodyBytes != nil {
			req.Body = io.NopCloser(bytes.NewReader(bodyBytes))
		}
		resp, err = client.Do(req)
		if err != nil {
			break
		}
		if resp.StatusCode >= 200 && resp.StatusCode < 400 {
			break
		}
		if r.isRetryable(resp.StatusCode) {
			if resp.Body != nil {
				_ = resp.Body.Close()
			}
			if r.ExponentialBackOff {
				time.Sleep(r.FixedDelay * time.Duration(math.Pow(2, float64(i))))
			} else {
				time.Sleep(r.FixedDelay)
			}
		} else {
			break
		}
	}
	return resp, err
}

func (r *SimpleRetryStrategy) isRetryable(code int) bool {
	return slices.Contains(r.RetryableStatusCodes, code)
}
