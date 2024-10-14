package main

import (
	"fmt"
	"log"
	"time"

	"github.com/confluentinc/confluent-kafka-go/kafka"
)

type OrderPlacer struct {
	producer *kafka.Producer
	topic    string
    deliveryCh chan kafka.Event
}

func NewOrderPlacer(p *kafka.Producer, topic string) *OrderPlacer {
	return &OrderPlacer{
		producer: p,
		topic:    topic,
        deliveryCh: make(chan kafka.Event, 10000),
	}
}

func (op *OrderPlacer) placeOrder(orderType string, size int) error { 
    // FIXME: Возможно size должен быть не интом а float64

    var (
        format  = fmt.Sprintf("%s - %d", orderType, size)
        payload = []byte(format)
    )
    
    err := op.producer.Produce(&kafka.Message{
            TopicPartition: kafka.TopicPartition{
                Topic: &op.topic, 
                Partition: kafka.PartitionAny,
            },
            Value:          payload,
        },
            op.deliveryCh,
        )
        if err != nil {
            log.Fatal(err)
            return err
        }
        <-op.deliveryCh

        return nil
}

func main() {
    ///////////////////////////////////////////////////////
    // Producer
    ///////////////////////////////////////////////////////

    topic := "HVSE"
	p, err := kafka.NewProducer(&kafka.ConfigMap{
		"bootstrap.servers": "localhost:9092",
		"client.id":         "myProducer",
		"acks":              "all",
    })
	if err != nil {
		fmt.Printf("Failed to create producer: %s\n", err)
	}

    op := NewOrderPlacer(p, topic)
    for i := 0; i < 1000; i++ {
        if err := op.placeOrder("market order", i + 1); err != nil {
            log.Fatal(err)
        }
        time.Sleep(3 * time.Second)
    }
}