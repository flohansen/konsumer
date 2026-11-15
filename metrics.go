package konsumer

import (
	"context"
	"fmt"
	"time"

	"github.com/flohansen/semconvmetric"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
	semconv "go.opentelemetry.io/otel/semconv/v1.37.0"
)

const meterName = "github.com/flohansen/konsumer"

type MetricHandler struct {
	next    Handler
	metrics semconvmetric.MessagingConsumer
}

func HandlerWithMetrics() func(next Handler) Handler {
	meter := otel.Meter(meterName)
	metrics := Must(semconvmetric.NewMessagingConsumer(meter))

	return func(next Handler) Handler {
		return &MetricHandler{
			next:    next,
			metrics: metrics,
		}
	}
}

func (m *MetricHandler) Handle(ctx context.Context, msg Message) error {
	start := time.Now()
	err := m.next.Handle(ctx, msg)
	elapsed := time.Since(start).Seconds()

	attrs := attrsWithErr([]attribute.KeyValue{
		semconv.MessagingOperationName("process"),
	}, err)

	m.metrics.ProcessDuration.Record(ctx, elapsed, metric.WithAttributes(attrs...))
	return err
}

type MetricClient struct {
	next    Client
	metrics semconvmetric.MessagingConsumer
}

func ClientWithMetrics() func(next Client) Client {
	meter := otel.Meter(meterName)
	metrics := Must(semconvmetric.NewMessagingConsumer(meter))

	return func(next Client) Client {
		return &MetricClient{
			next:    next,
			metrics: metrics,
		}
	}
}

func (m *MetricClient) Receive(ctx context.Context) ([]Message, error) {
	start := time.Now()
	msgs, err := m.next.Receive(ctx)
	elapsed := time.Since(start).Seconds()

	attrs := attrsWithErr([]attribute.KeyValue{
		semconv.MessagingOperationName("receive"),
	}, err)

	m.metrics.ClientConsumedMessages.Add(ctx, int64(len(msgs)), metric.WithAttributes(attrs...))
	m.metrics.ClientOperationDuration.Record(ctx, elapsed, metric.WithAttributes(attrs...))
	return msgs, err
}

func (m *MetricClient) Commit(ctx context.Context, msg Message) error {
	start := time.Now()
	err := m.next.Commit(ctx, msg)
	elapsed := time.Since(start).Seconds()

	attrs := attrsWithErr([]attribute.KeyValue{
		semconv.MessagingOperationName("settle"),
	}, err)

	m.metrics.ClientOperationDuration.Record(ctx, elapsed, metric.WithAttributes(attrs...))
	return err
}

func attrsWithErr(attrs []attribute.KeyValue, err error) []attribute.KeyValue {
	if err != nil {
		return append(attrs, semconv.ErrorType(fmt.Errorf("%T", err)))
	}
	return attrs
}
