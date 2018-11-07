package catch

import (
	"context"
	"log"

	"github.com/Shopify/sarama"
	"github.com/TerrexTech/go-kafkautils/kafka"
	"github.com/pkg/errors"
)

// consumer is a convenience wrapper over kafka.Consumer that allows
// fetching messages asynchronously.
type consumer struct {
	consumer    *kafka.Consumer
	topic       string
	consumerCtx context.Context

	msgChan     chan *sarama.ConsumerMessage
	isConsuming bool
}

func newConsumer(ctx context.Context, config *kafka.ConsumerConfig) (*consumer, error) {
	c, err := kafka.NewConsumer(config)
	if err != nil {
		err = errors.Wrap(err, "Error while creating Consumer")
		log.Println(err)
		return nil, err
	}
	return &consumer{
		consumer:    c,
		topic:       config.Topics[0],
		consumerCtx: ctx,

		msgChan:     make(chan *sarama.ConsumerMessage),
		isConsuming: false,
	}, nil
}

func (*consumer) Setup(sarama.ConsumerGroupSession) error {
	return nil
}

func (*consumer) Cleanup(sarama.ConsumerGroupSession) error {
	return nil
}

func (r *consumer) ConsumeClaim(
	session sarama.ConsumerGroupSession,
	claim sarama.ConsumerGroupClaim,
) error {
	for {
		select {
		case <-r.consumerCtx.Done():
			log.Printf("Closing Consumer on Topic: %s", r.topic)
			close(r.msgChan)
			return errors.New("service-context closed")
		case msg := <-claim.Messages():
			r.msgChan <- msg
		}
	}
}

func (r *consumer) Close() error {
	return r.consumer.Close()
}

func (r *consumer) Errors() <-chan error {
	return r.consumer.Errors()
}

func (r *consumer) Messages() <-chan *sarama.ConsumerMessage {
	if !r.isConsuming {
		go func() {
			r.isConsuming = true
			err := r.consumer.Consume(r.consumerCtx, r)
			if err != nil {
				err = errors.Wrap(err, "Consumer: Error while consuming messages")
				log.Println(err)
			}
		}()
	}
	return r.msgChan
}
