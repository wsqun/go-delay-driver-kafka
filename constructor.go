package go_delay_driver_kafka

import (
	"github.com/Shopify/sarama"
	"log"
)

// 初始化配置
func (dk *DKafka) initCfg(opt... OptFn) (err error) {
	dk.cfg = sarama.NewConfig()
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
func (dk *DKafka) initConsumer() (err error) {
	dk.consumer, err = sarama.NewConsumerGroup(dk.addrs, dk.groupId, dk.cfg)
	return err
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
