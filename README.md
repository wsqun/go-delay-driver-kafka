## go-driver-kafka

### how to use
https://github.com/wsqun/go-delay-queue


### docker测试环境
> 将$IP替换为本地IP，通过ipconfig或者ifconfig获取，如192.168.1.37
- 安装Zookeeper
```
docker pull zookeeper
docker run -d --name zookeeper -p 2181:2181 zookeeper
```


- 安装Kafka
```
docker pull wurstmeister/kafka
docker run  -d --name kafka -p 9092:9092 -e KAFKA_BROKER_ID=0 -e KAFKA_ZOOKEEPER_CONNECT=$IP:2181 -e KAFKA_ADVERTISED_LISTENERS=PLAINTEXT://$IP:9092 -e KAFKA_LISTENERS=PLAINTEXT://0.0.0.0:9092 -t wurstmeister/kafka
```

- 安装Kafka管理后台
```
docker pull kafkamanager/kafka-manager
docker run -p 9000:9000 -d -eZK_HOSTS=$IP kafkamanager/kafka-manager

访问：
http://127.0.0.1:9000/
```