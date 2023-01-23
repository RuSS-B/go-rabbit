package go_rabbit

import (
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
		config: cfg,
	}
	err := conn.Connect()
	if err != nil {
		return nil, err
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
			return err
		}

		waitTime := time.Duration(math.Pow(float64(connAttempts), 2)) * time.Second
		time.Sleep(waitTime)
	}

	conn.mqChan, err = conn.mqConn.Channel()
	if err != nil {
		conn.Close()
		return err
	}

	conn.observe()

	return nil
}

func (conn *Connection) observe() {
	go func() {
		log.Printf("Connection closed: %s\n", <-conn.mqConn.NotifyClose(make(chan *amqp.Error)))

		conn.Close()
		err := conn.Connect()
		if err != nil {
			color.Red("Unable to reconnect")
			return
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

type TopicExchange struct {
	conn      *Connection
	name      string
	queueName string
	topics    []string
}

func NewTopicExchange(exchangeName string, queueName string, topics []string, conn *Connection) TopicExchange {
	return TopicExchange{
		conn:      conn,
		name:      exchangeName,
		queueName: queueName,
		topics:    topics,
	}
}

func (ex *TopicExchange) declareExchange() error {
	if ex.name == "" {
		return exchangeNameMissingErr
	}

	return ex.conn.mqChan.ExchangeDeclare(ex.name, "topic", true, false, false, false, nil)
}

func (ex *TopicExchange) Listen(handler MessageHandler) error {
	err := ex.declareExchange()
	if err != nil {
		return err
	}

	if ex.queueName == "" {
		return queueNameMissingErr
	}

	q, err := declareQueue(ex.conn.mqChan, ex.queueName)
	if err != nil {
		return err
	}

	if len(ex.topics) == 0 {
		return topicMissingErr
	}

	for _, s := range ex.topics {
		err = ex.conn.mqChan.QueueBind(q.Name, s, ex.name, false, nil)
		if err != nil {
			return err
		}
	}

	messages, err := consume(ex.conn.mqChan, &q)
	if err != nil {
		return err
	}

	color.Green("Waiting for messages in [Queue] [%s]\n", q.Name)
	handle(messages, handler)

	return nil
}

type Queue struct {
	conn *Connection
	name string
}

func NewQueue(queueName string, conn *Connection) Queue {
	return Queue{
		conn: conn,
		name: queueName,
	}
}

func (queue *Queue) Listen(handler MessageHandler) error {
	if queue.name == "" {
		return queueNameMissingErr
	}

	q, err := declareQueue(queue.conn.mqChan, queue.name)

	messages, err := consume(queue.conn.mqChan, &q)
	if err != nil {
		return err
	}

	color.Green("Waiting for messages in [Queue] [%s]\n", q.Name)
	handle(messages, handler)

	return nil
}
