package main

import (
	"encoding/json"
	"fmt"
	"github.com/confluentinc/confluent-kafka-go/kafka"
	"math/rand"
	"time"
)

type AirQualitySensorData struct {
	IDSensor      string    `json:"idSensor"`
	Timestamp     string    `json:"timestamp"`
	TipoPoluente   string    `json:"tipoPoluente"`
	Nivel         float64   `json:"nivel"`
}

func main() {
	p, err := kafka.NewProducer(&kafka.ConfigMap{
		"bootstrap.servers": "localhost:29092,localhost:39092",
		"client.id":         "go-producer",
	})
	if err != nil {
		panic(err)
	}
	defer p.Close()

	topic := "qualidadeAr"

	pollutants := []string{"PM2.5", "PM10", "NO2", "CO", "O3"}

	for i := 0; i < 100; i++ {
		sensorData := generateSensorData(pollutants[rand.Intn(len(pollutants))])
		jsonData, err := json.Marshal(sensorData)
		if err != nil {
			fmt.Println("Error converting data to JSON", err)
			return
		}

		msg := string(jsonData)

		err = p.Produce(&kafka.Message{
			TopicPartition: kafka.TopicPartition{Topic: &topic, Partition: kafka.PartitionAny},
			Value:          []byte(msg),
		}, nil)

		if err != nil {
			fmt.Println("Producer error:", err)
		}

		fmt.Println("Produced message:", msg)
		time.Sleep(2 * time.Second)
	}
}

func generateSensorData(tipoPoluente string) AirQualitySensorData {
	rand.Seed(time.Now().UnixNano())
	return AirQualitySensorData{
		IDSensor:      fmt.Sprintf("sensor_%03d", rand.Intn(1000)),
		Timestamp:     time.Now().Format(time.RFC3339),
		TipoPoluente:   tipoPoluente,
		Nivel:         rand.Float64() * 100,
	}
}