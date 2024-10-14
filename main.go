package main

import (
	"fmt"
	"log"
	"os"
	"time"

	"github.com/confluentinc/confluent-kafka-go/kafka"
)

func main() {
    topic := "HVSE"
    // FIXME: убрать ошибки
	p, err := kafka.NewProducer(&kafka.ConfigMap{
		"bootstrap.servers": "localhost:9092",
		"client.id":         "myProducer",
		"acks":              "all",
    })
	if err != nil {
		fmt.Printf("Failed to create producer: %s\n", err)
	}

    ///////////////////////////////////////////////////////

    go func(){
        consumer, err := kafka.NewConsumer(&kafka.ConfigMap{
        "bootstrap.servers":    "localhost:9092",
        "group.id":             "foo",
        "auto.offset.reset":    "smallest"},
        )
        if err != nil {
            log.Fatal(err)
        }

        err = consumer.Subscribe(topic, nil)
        if err != nil {
            log.Fatal(err)
        }

        for {
            ev := consumer.Poll(100)
            switch e := ev.(type) {
            case *kafka.Message:
                fmt.Printf("consumed message from the que: %s\n", string(e.Value))
            case kafka.Error:
                fmt.Fprintf(os.Stderr, "%% Error: %v\n", e)
        }
    }
    }()

    ///////////////////////////////////////////////////////

    deliverch := make(chan kafka.Event, 10000)

    for {
        err = p.Produce(&kafka.Message{
            TopicPartition: kafka.TopicPartition{Topic: &topic, Partition: kafka.PartitionAny},
            Value: []byte("FULL"),
        },
            deliverch,
        )
        if err != nil {
            log.Fatal(err)
        }
        <-deliverch

        time.Sleep(time.Second * 3)
    }

    // fmt.Println(e)
    ///////////////////////////////////////////////////////

}