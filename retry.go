package konsumer

import (
	"context"
	"fmt"
	"math"
	"math/rand"
	"time"
)

const (
	defaultMaxRetries = 3
	defaultBaseDelay  = time.Second
	defaultMaxDelay   = 30 * time.Second
	defaultJitter     = 0.25
)

var _ Handler = &RetryHandler{}

type RetryHandler struct {
	next       Handler
	maxRetries int
	baseDelay  time.Duration
	maxDelay   time.Duration
	jitter     float64
}

func HandlerWithRetries() func(next Handler) Handler {
	return func(next Handler) Handler {
		return &RetryHandler{
			next:       next,
			maxRetries: defaultMaxRetries,
			baseDelay:  defaultBaseDelay,
			maxDelay:   defaultMaxDelay,
			jitter:     defaultJitter,
		}
	}
}

func (r *RetryHandler) Handle(ctx context.Context, msg Message) error {
	for attempt := 1; attempt <= r.maxRetries; attempt++ {
		msg.SetHeader(attemptMessageHeader, fmt.Appendf(nil, "%d", attempt))
		err := r.next.Handle(ctx, msg)
		if err == nil {
			return nil
		}

		if _, ok := err.(*PermanentError); ok {
			return err
		}

		if attempt >= r.maxRetries {
			return err
		}

		delay := time.Duration(float64(r.baseDelay) * math.Pow(2, float64(attempt-1)))
		if delay > r.maxDelay {
			delay = r.maxDelay
		}

		delay += jitterDelta(delay, r.jitter)
		time.Sleep(delay)
	}

	return nil
}

func jitterDelta(base time.Duration, perc float64) time.Duration {
	if base <= 0 {
		return 0
	}
	delta := float64(base) * perc
	return time.Duration(rand.Float64()*2*delta - delta)
}

const (
	attemptMessageHeader = "x-retry-count"
)
