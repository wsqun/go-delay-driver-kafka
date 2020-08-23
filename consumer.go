package go_delay_driver_kafka

import (
	"github.com/Shopify/sarama"
	"log"
)

// Consumer represents a Sarama consumer group consumer
type consumer struct {
	ready chan bool
	fn func([]byte)(err error)
}

// Setup is run at the beginning of a new session, before ConsumeClaim
func (consumer *consumer) Setup(sarama.ConsumerGroupSession) error {
	// Mark the consumer as ready
	close(consumer.ready)
	return nil
}

// Cleanup is run at the end of a session, once all ConsumeClaim goroutines have exited
func (consumer *consumer) Cleanup(sarama.ConsumerGroupSession) error {
	log.Println("kafka消费结束")
	return nil
}

// ConsumeClaim must start a consumer loop of ConsumerGroupClaim's Messages().
func (consumer *consumer) ConsumeClaim(session sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) error {
	for message := range claim.Messages() {
		if err := consumer.fn(message.Value);err == nil {
			session.MarkMessage(message, "")
		}
	}

	return nil
}
