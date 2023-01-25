package go_rabbit

import (
	"github.com/fatih/color"
	amqp "github.com/rabbitmq/amqp091-go"
)

type Queue struct {
	conn   *Connection
	name   string
	mqChan *amqp.Channel
}

func NewQueue(queueName string, conn *Connection) (Queue, error) {
	if queueName == "" {
		return Queue{}, queueNameMissingErr
	}

	ch, err := conn.newChannel()
	if err != nil {
		return Queue{}, err
	}

	return Queue{
		conn:   conn,
		name:   queueName,
		mqChan: ch,
	}, nil
}

func (queue *Queue) Listen(handler MessageHandler) error {
	q, err := declareQueue(queue.mqChan, queue.name)
	if err != nil {
		return err
	}

	messages, err := consume(queue.mqChan, &q)
	if err != nil {
		return err
	}

	color.Green("Waiting for messages in [Queue] [%s]\n", q.Name)
	handle(messages, handler)

	return nil
}

func (queue *Queue) Publish(body []byte) error {
	q, err := declareQueue(queue.mqChan, queue.name)
	if err != nil {
		return err
	}

	err = publish(queue.mqChan, &q, body)
	if err != nil {
		return err
	}

	return nil
}

func (queue *Queue) Close() {
	closeChannel(queue.mqChan)
}
