package go_rabbit

import (
	"errors"
	"fmt"
	"github.com/fatih/color"
	amqp "github.com/rabbitmq/amqp091-go"
	"log"
	"math"
	"time"
)

var mqConn *amqp.Connection
var mqChan *amqp.Channel

var (
	queueNameMissingErr    = errors.New("the queueName name should not be empty")
	topicMissingErr        = errors.New("there should be at least one topic provided")
	exchangeNameMissingErr = errors.New("the exchange name should not be empty")
)

type MQConfig struct {
	ConnectionName string
	MaxAttempts    uint
	ServerURL      string
}

func ConnectToMQ(config MQConfig) error {
	var err error

	cfg := amqp.Config{
		Properties: amqp.Table{
			"connection_name": config.ConnectionName,
		},
	}

	var connAttempts = 0
	for {
		connAttempts++
		log.Printf("Attempt %d of %d: Connecting to RabbitMQ...\n", connAttempts, config.MaxAttempts)

		mqConn, err = amqp.DialConfig(config.ServerURL, cfg)
		if err != nil {
			log.Println("RabbitMQ not ready yet...")
		} else {
			break
		}

		if connAttempts >= int(config.MaxAttempts) {
			fmt.Println(err)
			return err
		}

		waitTime := time.Duration(math.Pow(float64(connAttempts), 2)) * time.Second
		time.Sleep(waitTime)
	}

	mqChan, err = mqConn.Channel()
	if err != nil {
		return err
	}

	log.Println("Successfully connected to RabbitMQ")
	log.Println("Waiting for messages")

	observeMQConnection(config)

	return nil
}

func observeMQConnection(config MQConfig) {
	go func() {
		log.Printf("Connection closed: %s\n", <-mqConn.NotifyClose(make(chan *amqp.Error)))
		log.Printf("Trying to reconnect to MQ\n")

		CloseActiveConnections()
		err := ConnectToMQ(config)
		if err != nil {
			color.Red("Unable to reconnect")
			return
		}
	}()
}

func CloseActiveConnections() {
	if !mqChan.IsClosed() {
		if err := mqChan.Close(); err != nil {
			log.Println(err)
		}
	}

	if mqConn != nil && !mqConn.IsClosed() {
		if err := mqConn.Close(); err != nil {
			log.Println(err)
		}
	}
}

type MessageHandler func(message amqp.Delivery)

func handle(messages <-chan amqp.Delivery, handler MessageHandler) {
	for message := range messages {
		handler(message)
	}
}

func declareQueue(queueName string) (amqp.Queue, error) {
	return mqChan.QueueDeclare(queueName, true, false, false, false, nil)
}

func consume(q amqp.Queue) (<-chan amqp.Delivery, error) {
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
	name      string
	queueName string
	topics    []string
}

func (ex *TopicExchange) declareExchange() error {
	if ex.name == "" {
		return exchangeNameMissingErr
	}

	return mqChan.ExchangeDeclare(ex.name, "topic", true, false, false, false, nil)
}

func (ex *TopicExchange) Listen(handler MessageHandler) error {
	err := ex.declareExchange()
	if err != nil {
		return err
	}

	if ex.queueName != "" {
		return queueNameMissingErr
	}

	q, err := declareQueue(ex.queueName)
	if err != nil {
		return err
	}

	if len(ex.topics) == 0 {
		return topicMissingErr
	}

	for _, s := range ex.topics {
		err = mqChan.QueueBind(q.Name, s, ex.name, false, nil)
		if err != nil {
			return err
		}
	}

	messages, err := consume(q)
	if err != nil {
		return err
	}

	color.Green("Waiting for messages in [Queue] [%s]\n", q.Name)
	handle(messages, handler)

	return nil
}

type Queue struct {
	name string
}

func (queue *Queue) Listen(handler MessageHandler) error {
	if queue.name == "" {
		return queueNameMissingErr
	}

	q, err := declareQueue(queue.name)

	messages, err := consume(q)
	if err != nil {
		return err
	}

	color.Green("Waiting for messages in [Queue] [%s]\n", q.Name)
	handle(messages, handler)

	return nil
}
