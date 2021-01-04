package go_delay_driver_kafka


// 当只需要生产消息可只调用该方法，初始化生产者
func NewOnlyProduct(addrs []string, opt ...OptFn) (dk *DKafka, err error) {
	dk = new(DKafka)
	dk.addrs = addrs
	// 构造配置
	if err = dk.initCfg(opt...); err != nil {
		return nil, err
	}
	// 生产者
	if err = dk.initProducer(); err != nil {
		return nil, err
	}
	return dk, nil
}
