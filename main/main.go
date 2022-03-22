package main

import (
	"context"
	"fmt"
	"github.com/apache/rocketmq-client-go/v2"
	"github.com/apache/rocketmq-client-go/v2/consumer"
	"github.com/apache/rocketmq-client-go/v2/primitive"
	"github.com/apache/rocketmq-client-go/v2/producer"
	"strconv"
	"sync"
)

var topic = "tiger_topic_01"
var nameSrv, _ = primitive.NewNamesrvAddr("127.0.0.1:9876")
var producerGroupName = "tiger_producer_group_01"

var consumerGroupName = "tiger_consumer_group_02"
var ctx = context.Background()

var maxMessageCount = 10
var locker = sync.WaitGroup{}

func main() {
	locker.Add(maxMessageCount)

	startOneProducer()

	startOneConsumer()

	locker.Wait()
}

func startOneConsumer() {
	c, _ := rocketmq.NewPushConsumer(
		consumer.WithGroupName(consumerGroupName),
		consumer.WithNameServer(nameSrv),
	)

	c.Subscribe(topic, consumer.MessageSelector{}, func(ctx context.Context, msgs ...*primitive.MessageExt) (consumer.ConsumeResult, error) {
		for _, msg := range msgs {
			locker.Done()
			fmt.Println(fmt.Sprintf("[消费消息] %s , %s", msg.MsgId, string(msg.Body)))
		}
		return consumer.ConsumeSuccess, nil
	})

	c.Start()
}

func startOneProducer() {
	p, _ := rocketmq.NewProducer(
		producer.WithNameServer(nameSrv),
		producer.WithGroupName(producerGroupName),
	)
	p.Start()

	for i := 1; i <= maxMessageCount; i++ {
		msg := &primitive.Message{
			Topic: topic,
			Body:  []byte("Hello RocketMQ Go Client! " + strconv.Itoa(i)),
		}
		res, _ := p.SendSync(ctx, msg)
		fmt.Println(res.String())
	}
}
