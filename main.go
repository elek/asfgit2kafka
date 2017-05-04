package main

import (
	"flag"
	"fmt"
	"github.com/confluentinc/confluent-kafka-go/kafka"
	"log"
	"net/http"
	"os"
	"bufio"
	"encoding/json"
	"time"


)

type KafkaClient struct {
	live     bool
	broker   string
	topic    string
	producer *kafka.Producer
}

func (kafkaClient *KafkaClient) open() {
	var err error
	kafkaClient.producer, err = kafka.NewProducer(&kafka.ConfigMap{"bootstrap.servers": kafkaClient.broker})

	if err != nil {
		fmt.Printf("Failed to create producer: %s\n", err)
	} else {
		fmt.Printf("Created Producer %v to %s\n", kafkaClient.producer, kafkaClient.broker)
		kafkaClient.live = true
		go func() {
			for e := range kafkaClient.producer.Events() {
				switch ev := e.(type) {
				case *kafka.Message:
					m := ev
					if m.TopicPartition.Error != nil {
						fmt.Printf("Delivery failed: %v\n", m.TopicPartition.Error)
						kafkaClient.live = false
					} else {
						fmt.Printf("Delivered message to topic %s [%d] at offset %v\n",
							*m.TopicPartition.Topic, m.TopicPartition.Partition, m.TopicPartition.Offset)
					}

				default:
					fmt.Printf("Ignored event: %s\n", ev)
				}
			}
		}()
	}

}

func (kafkaClient *KafkaClient) close() {
	kafkaClient.producer.Close()
}

func (kafkaClient *KafkaClient) sendToKafka(message []byte) {
	println("Pushing event to kafka....")
	if ! kafkaClient.live {
		kafkaClient.close()
		kafkaClient.open()
	}
	kafkaClient.producer.ProduceChannel() <- &kafka.Message{
		TopicPartition: kafka.TopicPartition{Topic: &kafkaClient.topic, Partition: kafka.PartitionAny},
		Value:          message}
	kafkaClient.producer.Flush(100)

}

func main() {
	topic := flag.String("topic", "asfgit", "Kafka topic to use")
	file := flag.String("file", "/tmp/asfgit.log", "Logfile to save data locally")
	broker := flag.String("broker", "localhost", "Address of the kafka broker")
	url := "http://gitpubsub-wip.apache.org:2069/json/*"
	flag.Parse()

	client := &http.Client{}
	producer := KafkaClient{broker: *broker, topic: *topic}
	producer.open()
	defer producer.close()
	req, err := http.NewRequest("GET", url, nil)
	if err != nil {
		log.Fatal("Can't create request", err)
	}

	for {
		println("Connecting to the stream: " + url)
		resp, err := client.Do(req)

		if err != nil || resp.StatusCode > 400 {
			print(resp.StatusCode)
			log.Printf("Can't get the pubsub feed (Status code: %d): %s\f", resp.StatusCode, err)
			time.Sleep(time.Second)
		}
		defer resp.Body.Close()

		for {
			buffered_reader := bufio.NewReader(resp.Body)
			line, err := buffered_reader.ReadBytes('\n')
			if err != nil {
				log.Printf("Body is not readable %s\n", err)
				resp.Body.Close()
				break
			} else {
				var result map[string]interface{}
				err = json.Unmarshal(line, &result)
				if err != nil {
					log.Printf("Can't parse json: %s\n", err)
				} else {
					if _, ok := result["stillalive"]; ok {
						println("stillalive")
					} else {
						result["timestamp"] = time.Now().UnixNano()
						content, err := json.Marshal(result)
						if err != nil {
							println("Can't parse line to json: " + err.Error())
						} else {
							println(string(content))

							producer.sendToKafka(content)
							err = appendTo(*file, content)

						}
					}
				}
			}
		}

	}
	producer.close()

}
func appendTo(filename string, bytes []byte) error {
	f, err := os.OpenFile(filename, os.O_WRONLY | os.O_CREATE | os.O_APPEND, 0644)
	if err != nil {
		return err
	}
	_, err = f.Write(bytes)
	if err != nil {
		return err
	}

	_, err = f.Write([]byte("\n"))
	if err != nil {
		return err
	}
	err = f.Close()
	if err != nil {
		return err
	}
	return nil
}
