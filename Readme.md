## Installation
`go get github.com/RuSS-B/go-rabbit`

## Example Usage
```
import (
	"fmt"
	rabbitmq "github.com/RuSS-B/go-rabbit"
)

conn, err := rabbitmq.NewConnection("amqp://guest:guest@127.0.0.1", rabbitmq.Config{ConnectionName: "listener", MaxAttempts: 10})
if err != nil {
	fmt.Println("Unable to connect.", err)
}
defer conn.Close()

//The methods inside callback will be executed after successful connection to MQ. 
mqConn.WhenConnected(func(conn *rabbitmq.Connection) {
    go handleS3()
    go handleMetrics()
})

<- conn.Closed
```