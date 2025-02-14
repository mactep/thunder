package consumer

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"

	"github.com/gothunder/thunder/pkg/events"
	"github.com/rabbitmq/amqp091-go"
	amqp "github.com/rabbitmq/amqp091-go"
	"github.com/rotisserie/eris"
	"github.com/rs/zerolog/log"
	"github.com/vmihailenco/msgpack/v5"
)

const (
	topicHeaderKey = "x-thunder-topic"
)

func (r *rabbitmqConsumer) handler(msgs <-chan amqp.Delivery, handler events.Handler) {
	for msg := range msgs {
		// in case of requeue backoff, we want to make sure we have the correct topic
		topic := extractTopic(msg)
		// we always inject the correct topic so the requeue backoff can work
		injectTopic(&msg, topic)

		logger := r.logger.With().Str("topic", topic).Logger()
		ctx := logger.WithContext(context.Background())

		decoder := newDecoder(msg)
		res := r.handleWithRecoverer(ctx, handler, topic, decoder)

		switch res {
		case events.Success:
			// Message was successfully processed
			err := msg.Ack(false)
			if err != nil {
				logger.Error().Err(err).Msg("failed to ack message")
			}
		case events.DeadLetter:
			// We should retry to process the message on a different worker
			err := msg.Nack(false, false)
			if err != nil {
				logger.Error().Err(err).Msg("failed to requeue message")
			}
		case events.RetryBackoff:
			// We should send to a go routine that will requeue the message after a backoff time
			go r.retryBackoff(msg, &logger)
		default:
			// We should stop processing the message
			err := msg.Nack(false, true)
			if err != nil {
				logger.Error().Err(err).Msg("failed to discard message")
			}
		}
	}
}

func (r *rabbitmqConsumer) handleWithRecoverer(ctx context.Context, handler events.Handler, topic string, decoder events.EventDecoder) (res events.HandlerResponse) {
	logger := log.Ctx(ctx).With().Stack().Logger()
	logger.Info().Msg("consuming message")

	// Catch panics
	defer func() {
		if r := recover(); r != nil {
			err, ok := r.(error)
			if !ok {
				err = eris.New(fmt.Sprintf("%v", r))
			}

			err = eris.Wrap(err, "panic")
			logger.Error().Err(err).Msg("panic while consuming message")

			// If there's a panic, we should stop processing the message
			res = events.DeadLetter
		}
	}()

	return handler.Handle(ctx, topic, decoder)
}

// extractTopic extracts the topic from the message.
// It looks at the headers first, then the routing key.
func extractTopic(msg amqp091.Delivery) string {
	if headerTopic, ok := msg.Headers[topicHeaderKey]; ok {
		return headerTopic.(string)
	}

	return msg.RoutingKey
}

// injectTopic injects the topic into the message headers.
func injectTopic(msg *amqp091.Delivery, topic string) {
	if msg.Headers == nil {
		msg.Headers = make(amqp.Table)
	}

	msg.Headers[topicHeaderKey] = topic
}

// newDecoder creates a new decoder given the message.
// It looks at the content type to determine the decoder with fallback to json.
func newDecoder(msg amqp091.Delivery) events.EventDecoder {
	if msg.ContentType == "application/msgpack" {
		return msgpack.NewDecoder(bytes.NewReader(msg.Body))
	}

	return json.NewDecoder(bytes.NewReader(msg.Body))
}
