package go_delay_driver_kafka

import (
	"github.com/Shopify/sarama"
	"golang.org/x/net/context"
	"log"
	"sync"
)

type DKafka struct {
	consumerMap  map[string]*consumer
	lockConsumer sync.Mutex
	producer     sarama.AsyncProducer
	cfg          *sarama.Config
	addrs        []string
	groupId      string
	ctx          context.Context
	wg			 *sync.WaitGroup
}

func NewDKafka(addrs []string, groupId string, ctx context.Context, wg *sync.WaitGroup, opt ...OptFn) (dk *DKafka, err error) {
	dk = new(DKafka)
	dk.addrs = addrs
	dk.groupId = groupId
	dk.ctx = ctx
	dk.wg = wg
	// 构造配置
	if err = dk.initCfg(opt...); err != nil {
		return nil, err
	}
	dk.lockConsumer = sync.Mutex{}
	// 消费者
	dk.consumerMap = map[string]*consumer{}
	// 生产者
	if err = dk.initProducer(); err != nil {
		return nil, err
	}
	return dk, nil
}

func (dk *DKafka) SubscribeMsg(topic string, dealFn func([]byte) (err error)) (err error) {
	serve,err := dk.getConsumer(topic)
	if err != nil {
		log.Println("get consumer err:", err)
		return err
	}
	serve.fn = dealFn
	go func() {
		dk.wg.Add(1)
		defer func() {
			serve.cil.Close()
			log.Println("kafka停止消费", topic)
			dk.wg.Done()
		}()
		for {
			if err := serve.cil.Consume(dk.ctx, []string{topic}, serve); err != nil {
				log.Panicf("Error from consumer: %v", err)
			}
			// check if context was cancelled, signaling that the consumer should stop
			if dk.ctx.Err() != nil {
				return
			}
			serve.ready = make(chan bool)
		}
	}()

	<-serve.ready // Await till the consumer has been set up
	log.Println("kafka消费开始: ", topic)

	return nil
}

func (dk *DKafka) PublishMsg(topic string, msg []byte) (err error) {
	message := &sarama.ProducerMessage{}
	message.Topic = topic
	message.Partition = int32(-1)

	message.Value = sarama.StringEncoder(string(msg))
	dk.producer.Input() <- message
	return
}

type OptFn func(cfg *sarama.Config)
