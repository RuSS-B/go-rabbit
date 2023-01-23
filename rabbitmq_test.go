package go_rabbit

import (
	"log"
	"os"
	"testing"
)

func TestConnect(t *testing.T) {
	mqConfig := MQConfig{
		ConnectionName: "listener",
		MaxAttempts:    2,
		ServerURL:      os.Getenv("127.0.0.1"),
	}

	err := ConnectToMQ(mqConfig)
	if err != nil {
		CloseActiveConnections()
		log.Println("Closed with error", err)
	}
	defer CloseActiveConnections()
}
