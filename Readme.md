## Installation
`go get github.com/RuSS-B/go-rabbit`

## Example Usage
```
mqConfig := rabbitmq.MQConfig{
    ConnectionName: "my_awesome_connection_name",
    MaxAttempts:    5,
    ServerURL:      os.Getenv("AMQP_SERVER_URL"),
}

err = rabbitmq.ConnectToMQ(mqConfig)
if err != nil {
	rabbitmq.CloseActiveConnections()
	log.Fatalln(err)
}
```