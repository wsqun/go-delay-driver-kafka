package go_delay_driver_kafka

import (
	"github.com/Shopify/sarama"
	"log"
)

// 初始化配置
func (dk *DKafka) initCfg(opt... OptFn) (err error) {
	dk.cfg = sarama.NewConfig()
	dk.cfg.Version, _ = sarama.ParseKafkaVersion("2.1.1")
	// 消费者
	dk.cfg.Consumer.Offsets.Initial = sarama.OffsetNewest
	// 生产者
	dk.cfg.Producer.RequiredAcks = sarama.WaitForAll
	dk.cfg.Producer.Partitioner = sarama.NewRandomPartitioner
	dk.cfg.Producer.Return.Successes = true
	if len(opt) > 0 {
		for _,fn := range opt {
			fn(dk.cfg)
		}
	}
	return nil
}

// 初始化消费者
func (dk *DKafka) getConsumer(topic string) (*consumer, error) {
	defer dk.lockConsumer.Unlock()
	dk.lockConsumer.Lock()
	if consumer,exist := dk.consumerMap[topic];exist {
		return consumer,nil
	}
	cli,err := sarama.NewConsumerGroup(dk.addrs, dk.groupId, dk.cfg)
	if err != nil {
		return nil, err
	}
	dk.consumerMap[topic] = &consumer{
		cil:   cli,
		ready: make(chan bool),
	}
	//dk.consumerMap[topic], err = sarama.NewConsumerGroup(dk.addrs, dk.groupId, dk.cfg)
	return dk.consumerMap[topic],nil
}

// 初始化生产者
func (dk *DKafka) initProducer() (err error) {
	dk.producer, err = sarama.NewAsyncProducer(dk.addrs, dk.cfg)
	if err != nil {
		return err
	}
	go func(p sarama.AsyncProducer) {
		errors := p.Errors()
		success := p.Successes()
		for {
			select {
			case err := <-errors:
				if err != nil {
					log.Println(err)
				}
			case <-success:
			}
		}
	}(dk.producer)
	return nil
}
