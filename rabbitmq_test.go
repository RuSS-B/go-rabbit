package go_rabbit

import (
	"fmt"
	amqp "github.com/rabbitmq/amqp091-go"
	"log"
	"testing"
)

func TestConnect(t *testing.T) {
	conn, err := NewConnection("amqp://guest:guest@127.0.0.1", "text_conn", 2)
	if err != nil {
		fmt.Println("Unable to connect.", err)
	}
	defer conn.Close()
}

func TestMessageHandling(t *testing.T) {
	conn, err := NewConnection("amqp://guest:guest@127.0.0.1", "text_conn", 2)
	if err != nil {
		fmt.Println("Unable to connect.", err)
	}
	defer conn.Close()

	tex, err := NewTopicExchange("app_txn", "test_queue", []string{"Test.Topic"}, conn)
	if err != nil {
		log.Fatalln(err)
	}
	err = tex.Listen(func(message amqp.Delivery) {
		log.Println(string(message.Body))
	})
	if err != nil {
		log.Println("Unable to read message", err)
	}
	defer tex.Close()
}

func TestWaitForSignal(t *testing.T) {
	conn, err := NewConnection("amqp://guest:guest@127.0.0.1", "text_conn", 2)
	if err != nil {
		fmt.Println("Unable to connect.", err)
	}
	defer conn.Close()

	<-conn.Closed
}

func TestNewQueue(t *testing.T) {
	conn, err := NewConnection("amqp://guest:guest@127.0.0.1", "text_conn", 2)
	if err != nil {
		fmt.Println("Unable to connect.", err)
	}
	defer conn.Close()

	q, err := NewQueue("TestQueueName", conn)
	if err != nil {
		log.Fatalln(err)
	}
	defer q.Close()
}
