package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"github.com/Shopify/sarama"
)

func TestConsumer() {
	defer func() {
		if r := recover(); r != nil {
			if err, ok := r.(error); ok {
				fmt.Println("panic 1 =  " + err.Error())
			} else {
				fmt.Printf("Panic happened with %v", r)
				fmt.Println()
			}
			go TestConsumer()
		} else {
			fmt.Println("Consumer Completely Closed!")
		}
	}()
	sarama.Logger = log.New(os.Stdout, "[sarama] ", log.LstdFlags)

	// Consumer Config!!!
	version, err := sarama.ParseKafkaVersion("3.0.0")

	consumerConfig := sarama.NewConfig()
	consumerConfig.Net.TLS.Enable = false
	consumerConfig.ClientID = "togomi-test"
	consumerConfig.Version = version
	consumerGroupId := "test-group"

	consumer := Consumer{
		ready: make(chan bool),
	}

	ctx, cancel := context.WithCancel(context.Background())

	kafkaBootStrapServer := os.Args[1]

	client, err := sarama.NewConsumerGroup([]string{kafkaBootStrapServer}, consumerGroupId, consumerConfig)
	if err != nil {
		fmt.Println(err)
		time.Sleep(time.Minute * 1)
		panic("Try Reconnection to Kafka...")
	}

	wg := &sync.WaitGroup{}
	wg.Add(1)
	go func() {
		topic := "tmax"
		defer wg.Done()
		for {
			// `Consume` should be called inside an infinite loop, when a
			// server-side rebalance happens, the consumer session will need to be
			// recreated to get the new claims
			if err := client.Consume(ctx, []string{topic}, &consumer); err != nil {
				fmt.Println(err)
			}
			// check if context was cancelled, signaling that the consumer should stop
			if ctx.Err() != nil {
				return
			}
			consumer.ready = make(chan bool)
		}
	}()

	<-consumer.ready // Await till the consumer has been set up
	fmt.Println("Test consumer up and running!...")

	sigterm := make(chan os.Signal, 1)
	signal.Notify(sigterm, syscall.SIGINT, syscall.SIGTERM)
	select {
	case <-ctx.Done():
		fmt.Println("terminating: context cancelled")
	case <-sigterm:
		fmt.Println("terminating: via signal")
	}
	cancel()
	wg.Wait()
	if err = client.Close(); err != nil {
		fmt.Println(err)
	}
}

// Consumer represents a Sarama consumer group consumer
type Consumer struct {
	ready chan bool
}

// Setup is run at the beginning of a new session, before ConsumeClaim
func (consumer *Consumer) Setup(sarama.ConsumerGroupSession) error {
	// Mark the consumer as ready
	close(consumer.ready)
	return nil
}

// Cleanup is run at the end of a session, once all ConsumeClaim goroutines have exited
func (consumer *Consumer) Cleanup(sarama.ConsumerGroupSession) error {
	return nil
}

// ConsumeClaim must start a consumer loop of ConsumerGroupClaim's Messages().
func (consumer *Consumer) ConsumeClaim(session sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) error {

	for message := range claim.Messages() {
		fmt.Println(string(message.Value))
		session.MarkMessage(message, "")
	}

	return nil
}
