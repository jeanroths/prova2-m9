package main

import (
	"encoding/json"
	"fmt"
	"github.com/confluentinc/confluent-kafka-go/kafka"
	"math/rand"
	"testing"
	"time"
)

type AirQualitySensorData struct {
	IDSensor      string    `json:"idSensor"`
	Timestamp     string    `json:"timestamp"`
	TipoPoluente   string    `json:"tipoPoluente"`
	Nivel         float64   `json:"nivel"`
}

func TestProducer(t *testing.T) {
	p, err := kafka.NewProducer(&kafka.ConfigMap{
		"bootstrap.servers": "localhost:29092,localhost:39092",
		"client.id":         "go-producer",
	})
	if err != nil {
		t.Fatalf("Failed to create producer: %v", err)
	}
	defer p.Close()

	topic := "qualidadeAr"

	pollutants := []string{"PM2.5", "PM10", "NO2", "CO", "O3"}

	for i := 0; i < 100; i++ {
		sensorData := generateSensorData(pollutants[rand.Intn(len(pollutants))])
		jsonData, err := json.Marshal(sensorData)
		if err != nil {
			t.Fatalf("Error converting data to JSON: %v", err)
		}

		msg := string(jsonData)

		err = p.Produce(&kafka.Message{
			TopicPartition: kafka.TopicPartition{Topic: &topic, Partition: kafka.PartitionAny},
			Value:          []byte(msg),
		}, nil)

		if err != nil {
			t.Fatalf("Producer error: %v", err)
		}

		fmt.Println("Produced message:", msg)
		time.Sleep(2 * time.Second)
	}

	// Wait for all messages to be delivered
	time.Sleep(10 * time.Second)
}

func TestConsumer(t *testing.T) {
	c, err := kafka.NewConsumer(&kafka.ConfigMap{
		"bootstrap.servers": "localhost:29092,localhost:39092",
		"group.id":          "go-consumer-group",
		"auto.offset.reset": "earliest",
	})
	if err != nil {
		t.Fatalf("Failed to create consumer: %v", err)
	}
	defer c.Close()

	topic := "qualidadeAr"

	c.SubscribeTopics([]string{topic}, nil)

	receivedData := make([]AirQualitySensorData, 0)

	for i := 0; i < 100; i++ {
		msg, err := c.ReadMessage(-1)
		if err == nil {
			sensorData := AirQualitySensorData{}
			err := json.Unmarshal(msg.Value, &sensorData)
			if err != nil {
				t.Fatalf("Error unmarshalling message: %v", err)
			}

			fmt.Printf("Consumed message: %+v\n", sensorData)

			receivedData = append(receivedData, sensorData)
		} else {
			t.Fatalf("Consumer error: %v (%v)", err, msg)
		}
	}

}

func TestPersistence(t *testing.T) {
	// Implement a test to verify that the data is persisted and accessible for future consultation
	// For now, we just print a message to indicate that the test is running
	fmt.Println("Running persistence test...")

	// Consume the messages again to verify that they are still available
	c, err := kafka.NewConsumer(&kafka.ConfigMap{
		"bootstrap.servers": "localhost:29092,localhost:39092",
		"group.id":          "go-consumer-group-persistence",
		"auto.offset.reset": "earliest",
	})
	if err != nil {
		t.Fatalf("Failed to create consumer: %v", err)
	}
	defer c.Close()

	topic := "qualidadeAr"

	c.SubscribeTopics([]string{topic}, nil)

	receivedData := make([]AirQualitySensorData, 0)

	for i := 0; i < 100; i++ {
		msg, err := c.ReadMessage(-1)
		if err == nil {
			sensorData := AirQualitySensorData{}
			err := json.Unmarshal(msg.Value, &sensorData)
			if err != nil {
				t.Fatalf("Error unmarshalling message: %v", err)
			}

			fmt.Printf("Consumed message: %+v\n", sensorData)

			receivedData = append(receivedData, sensorData)
		} else {
			t.Fatalf("Consumer error: %v (%v)", err, msg)
		}
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