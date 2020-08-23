package go_delay_driver_kafka

import (
	"github.com/Shopify/sarama"
	"golang.org/x/net/context"
	"log"
)

type DKafka struct {
	consumer sarama.ConsumerGroup
	producer sarama.AsyncProducer
	cfg *sarama.Config
	addrs []string
	groupId string
	ctx context.Context
}

func NewDKafka(addrs []string, groupId string, ctx context.Context, opt... OptFn) (dk *DKafka, err error) {
	dk = new(DKafka)
	dk.addrs = addrs
	dk.groupId = groupId
	dk.ctx = ctx
	// 构造配置
	if err = dk.initCfg(opt...);err != nil {
		return nil, err
	}
	// 消费者
	if err = dk.initConsumer();err != nil {
		return nil, err
	}
	// 生产者
	if err = dk.initProducer(); err != nil {
		return nil, err
	}
	return dk,nil
}


func (dk *DKafka) SubscribeMsg(topic string, dealFn func([]byte)(err error)) (err error){
	consumer := consumer{
		ready: make(chan bool),
		fn: dealFn,
	}
	go func() {
		defer dk.consumer.Close()
		for {
			if err := dk.consumer.Consume(dk.ctx, []string{topic}, &consumer); err != nil {
				log.Panicf("Error from consumer: %v", err)
			}
			// check if context was cancelled, signaling that the consumer should stop
			if dk.ctx.Err() != nil {
				return
			}
			consumer.ready = make(chan bool)
		}
	}()

	<-consumer.ready // Await till the consumer has been set up
	log.Println("Sarama consumer up and running!...")

	return nil
}


func  (dk *DKafka) PublishMsg(topic string, msg []byte) (err error){
	message := &sarama.ProducerMessage{}
	message.Topic = topic
	message.Partition = int32(-1)

	message.Value = sarama.StringEncoder(string(msg))
	dk.producer.Input() <- message
	return
}

type OptFn func (cfg *sarama.Config)
