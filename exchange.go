package go_rabbit

import "github.com/fatih/color"

type TopicExchange struct {
	conn      *Connection
	name      string
	queueName string
	topics    []string
}

func NewTopicExchange(exchangeName string, queueName string, topics []string, conn *Connection) (TopicExchange, error) {
	if exchangeName == "" {
		return TopicExchange{}, exchangeNameMissingErr
	}

	if queueName == "" {
		return TopicExchange{}, queueNameMissingErr
	}

	if len(topics) == 0 {
		return TopicExchange{}, topicMissingErr
	}

	return TopicExchange{
		conn:      conn,
		name:      exchangeName,
		queueName: queueName,
		topics:    topics,
	}, nil
}

func (ex *TopicExchange) declareExchange() error {
	return ex.conn.mqChan.ExchangeDeclare(ex.name, "topic", true, false, false, false, nil)
}

func (ex *TopicExchange) Listen(handler MessageHandler) error {
	err := ex.declareExchange()
	if err != nil {
		return err
	}

	q, err := declareQueue(ex.conn.mqChan, ex.queueName)
	if err != nil {
		return err
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
