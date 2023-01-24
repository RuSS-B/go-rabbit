package go_rabbit

import (
	"context"
	"errors"
	"github.com/fatih/color"
	amqp "github.com/rabbitmq/amqp091-go"
	"log"
	"math"
	"time"
)

var (
	queueNameMissingErr    = errors.New("the queueName name should not be empty")
	topicMissingErr        = errors.New("there should be at least one topic provided")
	exchangeNameMissingErr = errors.New("the exchange name should not be empty")
)

type Connection struct {
	Closed chan bool
	config config
	mqConn *amqp.Connection
	mqChan *amqp.Channel
}

func NewConnection(serverUrl string, connectionName string, maxAttempts uint) (*Connection, error) {
	cfg := config{
		serverURL:      serverUrl,
		connectionName: connectionName,
		maxAttempts:    maxAttempts,
	}

	conn := Connection{
		Closed: make(chan bool),
		config: cfg,
	}
	err := conn.Connect()
	if err != nil {
		return &conn, err
	}

	log.Println("Successfully connected to RabbitMQ")

	return &conn, nil
}

type config struct {
	connectionName string
	maxAttempts    uint
	serverURL      string
}

func (conn *Connection) Connect() error {
	var err error

	cfg := amqp.Config{
		Properties: amqp.Table{
			"connection_name": conn.config.connectionName,
		},
	}

	var connAttempts = 0
	for {
		connAttempts++
		log.Printf("Attempt %d of %d: Connecting to RabbitMQ...\n", connAttempts, conn.config.maxAttempts)

		conn.mqConn, err = amqp.DialConfig(conn.config.serverURL, cfg)
		if err != nil {
			log.Println("RabbitMQ not ready yet...")
		} else {
			break
		}

		if connAttempts >= int(conn.config.maxAttempts) {
			go conn.terminate()
			return err
		}

		waitTime := time.Duration(math.Pow(float64(connAttempts), 2)) * time.Second
		time.Sleep(waitTime)
	}

	conn.mqChan, err = conn.mqConn.Channel()
	if err != nil {
		conn.Close()
		go conn.terminate()
		return err
	}

	conn.observe()

	return nil
}

func (conn *Connection) terminate() {
	conn.Closed <- true
}

func (conn *Connection) observe() {
	go func() {
		log.Printf("Connection closed: %s\n", <-conn.mqConn.NotifyClose(make(chan *amqp.Error)))

		conn.Close()
		err := conn.Connect()
		if err != nil {
			color.Red("Unable to connect")
		}
	}()
}

func (conn *Connection) Close() {
	if conn.mqChan != nil && !conn.mqChan.IsClosed() {
		if err := conn.mqChan.Close(); err != nil {
			log.Println("Unable to close channel", err)
		}
	}

	if conn.mqConn != nil && !conn.mqConn.IsClosed() {
		if err := conn.mqConn.Close(); err != nil {
			log.Println("Unable to close connection", err)
		}
	}

	log.Println("Connection closed")
}

type MessageHandler func(message amqp.Delivery)

func handle(messages <-chan amqp.Delivery, handler MessageHandler) {
	for message := range messages {
		handler(message)
	}
}

func declareQueue(mqChan *amqp.Channel, queueName string) (amqp.Queue, error) {
	return mqChan.QueueDeclare(queueName, true, false, false, false, nil)
}

func consume(mqChan *amqp.Channel, q *amqp.Queue) (<-chan amqp.Delivery, error) {
	messages, err := mqChan.Consume(
		q.Name,
		"",
		true,
		false,
		false,
		false,
		nil,
	)

	if err != nil {
		log.Println("Consume error", err)
	}

	return messages, err
}

func publish(mqChan *amqp.Channel, q *amqp.Queue, body []byte) error {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	err := mqChan.PublishWithContext(
		ctx,
		"",
		q.Name,
		false,
		false,
		amqp.Publishing{
			ContentType: "application/json",
			Body:        body,
		},
	)
	if err != nil {
		return err
	}

	return nil
}
