package konsumer

import (
	"context"
	"log/slog"

	"go.opentelemetry.io/otel/trace"
)

type LoggingHandler struct {
	next   Handler
	logger *slog.Logger
}

func HandlerWithLogging(logger *slog.Logger) func(next Handler) Handler {
	return func(next Handler) Handler {
		return &LoggingHandler{
			next:   next,
			logger: logger,
		}
	}
}

func (l *LoggingHandler) Handle(ctx context.Context, msg Message) error {
	logger := l.logger.With(
		"topic", msg.Topic,
		"partition", msg.Partition,
		"offset", msg.Offset,
	)
	if sc := trace.SpanContextFromContext(ctx); sc.IsValid() {
		logger = logger.With("trace_id", sc.TraceID().String())
	}

	logger.Debug("handle message", "headers", msg.Headers)
	if err := l.next.Handle(ctx, msg); err != nil {
		logger.Warn("failed to handle message", "error", err)
		return err
	}

	logger.Debug("handled message successfully")
	return nil
}

type LoggingClient struct {
	next   Client
	logger *slog.Logger
}

func ClientWithLogging(logger *slog.Logger) func(next Client) Client {
	return func(next Client) Client {
		return &LoggingClient{
			next:   next,
			logger: logger,
		}
	}
}

func (l *LoggingClient) Receive(ctx context.Context) ([]Message, error) {
	logger := l.logger
	if sc := trace.SpanContextFromContext(ctx); sc.IsValid() {
		logger = logger.With("trace_id", sc.TraceID().String())
	}

	logger.Debug("receiving messages")
	msgs, err := l.next.Receive(ctx)
	if err != nil {
		logger.Warn("failed to receive messages", "error", err)
	}

	return msgs, err
}

func (l *LoggingClient) Commit(ctx context.Context, msg Message) error {
	logger := l.logger.With(
		"topic", msg.Topic,
		"partition", msg.Partition,
		"offset", msg.Offset,
	)
	if sc := trace.SpanContextFromContext(ctx); sc.IsValid() {
		logger = logger.With("trace_id", sc.TraceID().String())
	}

	logger.Debug("committing message")
	err := l.next.Commit(ctx, msg)
	if err != nil {
		logger.Warn("failed to commit message", "error", err)
	}
	return err
}
