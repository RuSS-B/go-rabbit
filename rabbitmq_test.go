package go_rabbit

import (
	"fmt"
	"github.com/fatih/color"
	amqp "github.com/rabbitmq/amqp091-go"
	"log"
	"runtime"
	"testing"
	"time"
)

func TestConnect(t *testing.T) {
	conn, err := CreateNewAndConnect("amqp://guest:guest@127.0.0.1", Config{})
	if err != nil {
		fmt.Println("Unable to connect.", err)
	}
	defer conn.Close()
}

func TestMessageHandling(t *testing.T) {
	conn, err := CreateNewAndConnect("amqp://guest:guest@127.0.0.1", Config{})
	if err != nil {
		log.Fatalln("Unable to connect.", err)
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
	conn := NewConnection("amqp://guest:guest@127.0.0.1", Config{
		ConnectionName: "test_conn",
	}).WhenConnected(func(conn *Connection) {
		q, _ := NewQueue("test_input", conn)

		//Sending values to queue every 2 seconds
		go func() {
			i := 0
			for !q.IsClosed() {
				i++
				err := q.Publish([]byte(fmt.Sprintf("Tick %d", i)))
				if err != nil {
					log.Println("Publishing error", err)
				}
				time.Sleep(2 * time.Second)
			}
		}()

		//Reading data from queue
		go func() {
			err := q.Listen(func(message amqp.Delivery) {
				log.Println(string(message.Body))
			})
			if err != nil {
				log.Println("Listener error", err)
			}
		}()

		//Just to make sure that the number stays the same after reconnect
		color.Green("Number of GORUTINES: %d", runtime.NumGoroutine())
	})

	err := conn.Connect()
	if err != nil {
		fmt.Println("Unable to connect.", err)
	}
	defer conn.Close()

	<-conn.Closed
}

func TestGracefulClose(t *testing.T) {
	conn, err := CreateNewAndConnect("amqp://guest:guest@127.0.0.1", Config{
		ConnectionName: "test_conn",
		MaxAttempts:    2,
	})
	if err != nil {
		log.Fatalln("Unable to connect.", err)
	}

	defer conn.Close()

	go func() {
		q, err := NewQueue("TestQueueName", conn)
		if err != nil {
			log.Fatalln(err)
		}
		defer q.Close()

		conn.Close()
	}()

	<-conn.Closed
}
