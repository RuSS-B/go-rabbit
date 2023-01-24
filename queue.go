package go_rabbit

import "github.com/fatih/color"

type Queue struct {
	conn *Connection
	name string
}

func NewQueue(queueName string, conn *Connection) (Queue, error) {
	if queueName == "" {
		return Queue{}, queueNameMissingErr
	}

	return Queue{
		conn: conn,
		name: queueName,
	}, nil
}

func (queue *Queue) Listen(handler MessageHandler) error {
	q, err := declareQueue(queue.conn.mqChan, queue.name)
	if err != nil {
		return err
	}

	messages, err := consume(queue.conn.mqChan, &q)
	if err != nil {
		return err
	}

	color.Green("Waiting for messages in [Queue] [%s]\n", q.Name)
	handle(messages, handler)

	return nil
}

func (queue *Queue) Publish(body []byte) error {
	q, err := declareQueue(queue.conn.mqChan, queue.name)
	if err != nil {
		return err
	}

	err = publish(queue.conn.mqChan, &q, body)
	if err != nil {
		return err
	}

	return nil
}
