package konsumer

import (
	"context"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/trace"
)

const tracerName = "github.com/flohansen/konsumer"

type TracingHandler struct {
	next   Handler
	tracer trace.Tracer
}

func HandlerWithTracing() func(next Handler) Handler {
	tp := otel.GetTracerProvider()
	tracer := tp.Tracer(tracerName)

	return func(next Handler) Handler {
		return &TracingHandler{
			next:   next,
			tracer: tracer,
		}
	}
}

func (t *TracingHandler) Handle(ctx context.Context, msg Message) error {
	ctx, span := t.tracer.Start(ctx, "Handler.Handle")
	defer span.End()

	if err := t.next.Handle(ctx, msg); err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
		return err
	}

	return nil
}

type TracingClient struct {
	next   Client
	tracer trace.Tracer
}

func ClientWithTracing() func(next Client) Client {
	tp := otel.GetTracerProvider()
	tracer := tp.Tracer(tracerName)

	return func(next Client) Client {
		return &TracingClient{
			next:   next,
			tracer: tracer,
		}
	}
}

func (t *TracingClient) Commit(ctx context.Context, msg Message) error {
	ctx, span := t.tracer.Start(ctx, "Client.Commit")
	defer span.End()

	err := t.next.Commit(ctx, msg)
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
	}
	return err
}

func (t *TracingClient) Receive(ctx context.Context) ([]Message, error) {
	ctx, span := t.tracer.Start(ctx, "Client.Receive")
	defer span.End()

	msgs, err := t.next.Receive(ctx)
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
	}
	return msgs, err
}
