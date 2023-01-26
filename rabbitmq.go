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
	serverUrlMissingErr    = errors.New("the ServerURL cannot be empty")
	queueNameMissingErr    = errors.New("the queueName name should not be empty")
	topicMissingErr        = errors.New("there should be at least one topic provided")
	exchangeNameMissingErr = errors.New("the exchange name should not be empty")
)

type Config struct {
	serverURL      string
	ConnectionName string
	MaxAttempts    uint
}

type onSuccessConn func(conn *Connection)

type Connection struct {
	Closed    chan bool
	config    Config
	mqConn    *amqp.Connection
	onRecover *onSuccessConn
}

func NewConnection(serverURL string, cfg Config) (*Connection, error) {
	cfg.serverURL = serverURL
	if cfg.serverURL == "" {
		return nil, serverUrlMissingErr
	}

	if cfg.MaxAttempts == 0 {
		cfg.MaxAttempts = 10
	}

	conn := &Connection{
		Closed: make(chan bool),
		config: cfg,
	}
	err := conn.Connect()
	if err != nil {
		return conn, err
	}

	log.Println("Successfully connected to RabbitMQ")

	return conn, nil
}

func (conn *Connection) Connect() error {
	var err error

	cfg := amqp.Config{
		Properties: amqp.Table{
			"connection_name": conn.config.ConnectionName,
		},
	}

	var connAttempts = 0
	for {
		connAttempts++
		log.Printf("Attempt %d of %d: Connecting to RabbitMQ...\n", connAttempts, conn.config.MaxAttempts)

		conn.mqConn, err = amqp.DialConfig(conn.config.serverURL, cfg)
		if err != nil {
			log.Println("RabbitMQ not ready yet...")
		} else {
			break
		}

		if connAttempts >= int(conn.config.MaxAttempts) {
			go conn.terminate()
			return err
		}

		waitTime := time.Duration(math.Pow(float64(connAttempts), 2)) * time.Second
		time.Sleep(waitTime)
	}

	conn.observe()

	return nil
}

func (conn *Connection) OnRecover(handler onSuccessConn) {
	conn.onRecover = &handler
}

func (conn *Connection) recover() {
	if conn.onRecover != nil {
		h := *conn.onRecover
		h(conn)
	}
}

func (conn *Connection) newChannel() (*amqp.Channel, error) {
	return conn.mqConn.Channel()
}

func (conn *Connection) terminate() {
	conn.Closed <- true
}

func (conn *Connection) observe() {
	go func() {
		if conn.onRecover != nil {
			conn.recover()
		}
		log.Printf("Connection closed: %s\n", <-conn.mqConn.NotifyClose(make(chan *amqp.Error)))

		conn.Close()
		err := conn.Connect()
		if err != nil {
			color.Red("Unable to connect")
		}
	}()
}

func closeChannel(mqChan *amqp.Channel) {
	if mqChan != nil && !mqChan.IsClosed() {
		if err := mqChan.Close(); err != nil {
			log.Println("Unable to close channel", err)
		}
	}
}

func (conn *Connection) Close() {
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
