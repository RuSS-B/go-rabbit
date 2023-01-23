package go_rabbit

import "github.com/fatih/color"

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
