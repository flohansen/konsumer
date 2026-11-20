package main

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/flohansen/konsumer"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/exporters/stdout/stdouttrace"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
)

func main() {
	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelDebug}))
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	exporter, err := stdouttrace.New(stdouttrace.WithPrettyPrint())
	if err != nil {
		panic(err)
	}
	tp := sdktrace.NewTracerProvider(
		sdktrace.WithBatcher(exporter),
	)
	defer tp.Shutdown(context.Background())
	otel.SetTracerProvider(tp)

	client := konsumer.Decorate(
		konsumer.Client(newFakeClientWithDummyData()),
		konsumer.ClientWithTracing(),
		konsumer.ClientWithMetrics("kafka"),
		konsumer.ClientWithLogging(logger),
	)

	handler := konsumer.Decorate(
		konsumer.Handler(&myHandler{}),
		konsumer.HandlerWithTracing(),
		konsumer.HandlerWithMetrics("kafka"),
		konsumer.HandlerWithRetries(),
		konsumer.HandlerWithLogging(logger),
	)

	loop := konsumer.NewConsumerLoop(client, handler)
	if err := loop.Start(ctx); err != nil {
		logger.Error("failed to run consumer loop", "error", err)
	}
}

type myHandler struct {
	failed int
}

func (m *myHandler) Handle(ctx context.Context, msg konsumer.Message) error {
	if m.failed < 2 && msg.Partition == 1 && msg.Offset == 1 {
		m.failed++
		return fmt.Errorf("i don't like this message")
	}

	return nil
}

type receiveResult struct {
	messages []konsumer.Message
	duration time.Duration
	err      error
}

type fakeClient struct {
	results []receiveResult
	calls   int
}

func newFakeClientWithDummyData() *fakeClient {
	return &fakeClient{
		results: []receiveResult{
			{
				messages: []konsumer.Message{
					{Topic: "test.topic", Partition: 0, Offset: 0},
					{Topic: "test.topic", Partition: 1, Offset: 0},
					{Topic: "test.topic", Partition: 2, Offset: 0},
				},
				duration: time.Second,
				err:      nil,
			},
			{
				messages: []konsumer.Message{
					{Topic: "test.topic", Partition: 0, Offset: 1},
					{Topic: "test.topic", Partition: 1, Offset: 1},
					{Topic: "test.topic", Partition: 2, Offset: 1},
				},
				duration: time.Second,
				err:      nil,
			},
		},
	}
}

func (f *fakeClient) Receive(ctx context.Context) ([]konsumer.Message, error) {
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	default:
		if f.calls > len(f.results)-1 {
			return nil, context.Canceled
		}

		result := f.results[f.calls]
		f.calls++

		time.Sleep(result.duration)
		return result.messages, result.err
	}
}

func (f *fakeClient) Commit(ctx context.Context, msg konsumer.Message) error {
	return nil
}
