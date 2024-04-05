package main

import (
	"encoding/json"
	"fmt"
	"github.com/confluentinc/confluent-kafka-go/kafka"
	//"time"
)

type AirQualitySensorData struct {
	IDSensor      string    `json:"idSensor"`
	Timestamp     string    `json:"timestamp"`
	TipoPoluente   string    `json:"tipoPoluente"`
	Nivel         float64   `json:"nivel"`
}

func main() {
	c, err := kafka.NewConsumer(&kafka.ConfigMap{
		"bootstrap.servers": "localhost:29092,localhost:39092",
		"group.id":          "go-consumer-group",
		"auto.offset.reset": "earliest",
	})
	if err!= nil {
		panic(err)
	}
	defer c.Close()

	topic := "qualidadeAr"

	c.SubscribeTopics([]string{topic}, nil)

	for {
		msg, err := c.ReadMessage(-1)
		if err == nil {
			sensorData := AirQualitySensorData{}
			err := json.Unmarshal(msg.Value, &sensorData)
			if err!= nil {
				fmt.Println("Error unmarshalling message:", err)
				continue
			}

			fmt.Printf("Consumed message: %+v\n", sensorData)
		} else {
			fmt.Printf("Consumer error: %v (%v)\n", err, msg)
			break
		}
	}
}