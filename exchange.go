package go_rabbit

import "github.com/fatih/color"

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
