package konsumer

import (
	"context"
	"fmt"
	"time"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/semconv/v1.43.0/messagingconv"
)

const meterName = "github.com/flohansen/konsumer"

type MetricHandler struct {
	next            Handler
	system          messagingconv.SystemAttr
	processDuration messagingconv.ProcessDuration
}

// HandlerWithMetrics returns a decorator that records the
// "messaging.process.duration" metric for each handled message.
//
// The system parameter is reported as the "messaging.system" attribute and
// should be the OpenTelemetry semantic convention value for the messaging
// system in use, e.g. "kafka", "rabbitmq" or "aws_sqs". See
// https://opentelemetry.io/docs/specs/semconv/registry/attributes/messaging/
func HandlerWithMetrics(system string) func(next Handler) Handler {
	meter := otel.Meter(meterName)
	processDuration := Must(messagingconv.NewProcessDuration(meter))

	return func(next Handler) Handler {
		return &MetricHandler{
			next:            next,
			system:          messagingconv.SystemAttr(system),
			processDuration: processDuration,
		}
	}
}

func (m *MetricHandler) Handle(ctx context.Context, msg Message) error {
	start := time.Now()
	err := m.next.Handle(ctx, msg)
	elapsed := time.Since(start).Seconds()

	m.processDuration.Record(ctx, elapsed, "process", m.system, errAttrs(err)...)
	return err
}

type MetricClient struct {
	next              Client
	system            messagingconv.SystemAttr
	consumedMessages  messagingconv.ClientConsumedMessages
	operationDuration messagingconv.ClientOperationDuration
}

// ClientWithMetrics returns a decorator that records the
// "messaging.client.consumed.messages" and "messaging.client.operation.duration"
// metrics for receive and commit operations.
//
// The system parameter is reported as the "messaging.system" attribute and
// should be the OpenTelemetry semantic convention value for the messaging
// system in use, e.g. "kafka", "rabbitmq" or "aws_sqs". See
// https://opentelemetry.io/docs/specs/semconv/registry/attributes/messaging/
func ClientWithMetrics(system string) func(next Client) Client {
	meter := otel.Meter(meterName)
	consumedMessages := Must(messagingconv.NewClientConsumedMessages(meter))
	operationDuration := Must(messagingconv.NewClientOperationDuration(meter))

	return func(next Client) Client {
		return &MetricClient{
			next:              next,
			system:            messagingconv.SystemAttr(system),
			consumedMessages:  consumedMessages,
			operationDuration: operationDuration,
		}
	}
}

func (m *MetricClient) Receive(ctx context.Context) ([]Message, error) {
	start := time.Now()
	msgs, err := m.next.Receive(ctx)
	elapsed := time.Since(start).Seconds()

	attrs := errAttrs(err)
	m.consumedMessages.Add(ctx, int64(len(msgs)), "receive", m.system, attrs...)
	m.operationDuration.Record(ctx, elapsed, "receive", m.system, attrs...)
	return msgs, err
}

func (m *MetricClient) Commit(ctx context.Context, msg Message) error {
	start := time.Now()
	err := m.next.Commit(ctx, msg)
	elapsed := time.Since(start).Seconds()

	m.operationDuration.Record(ctx, elapsed, "settle", m.system, errAttrs(err)...)
	return err
}

func errAttrs(err error) []attribute.KeyValue {
	if err == nil {
		return nil
	}
	return []attribute.KeyValue{
		attribute.String("error.type", fmt.Sprintf("%T", err)),
	}
}
