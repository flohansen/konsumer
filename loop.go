package konsumer

import (
	"context"
	"errors"
	"fmt"
)

type Message struct {
	Topic     string
	Partition int32
	Offset    int64
	Value     []byte
	Headers   []MessageHeader
}

func (msg *Message) SetHeader(key string, value []byte) {
	for i, hdr := range msg.Headers {
		if hdr.Key == key {
			msg.Headers[i].Value = value
			return
		}
	}

	msg.Headers = append(msg.Headers, MessageHeader{Key: key, Value: value})
}

type MessageHeader struct {
	Key   string
	Value []byte
}

type Client interface {
	Receive(ctx context.Context) ([]Message, error)
	Commit(ctx context.Context, msg Message) error
}

type Handler interface {
	Handle(ctx context.Context, msg Message) error
}

type ConsumerLoop struct {
	client  Client
	handler Handler
}

func NewConsumerLoop(client Client, handler Handler) *ConsumerLoop {
	return &ConsumerLoop{
		client:  client,
		handler: handler,
	}
}

func (cl *ConsumerLoop) Start(ctx context.Context) error {
	for {
		msgs, err := cl.client.Receive(ctx)
		if err != nil {
			if errors.Is(err, context.Canceled) {
				break
			}
			return fmt.Errorf("receive: %w", err)
		}

		if len(msgs) == 0 {
			continue
		}

		for _, msg := range msgs {
			if err := cl.handler.Handle(ctx, msg); err != nil {
				return fmt.Errorf("handle: %w", err)
			}

			if err := cl.client.Commit(ctx, msg); err != nil {
				return fmt.Errorf("commit: %w", err)
			}
		}
	}

	return nil
}
