package algolia

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"retl/outputs/types"
	"time"

	"github.com/algolia/algoliasearch-client-go/v3/algolia/search"
	"github.com/segmentio/kafka-go"
)

type Algolia struct {
	Conf *types.ConfigType
}

var LastRunTime time.Time

func (a *Algolia) Run() error {

	fmt.Println("ENTERED RUN FUNC")
	keypair, err := tls.LoadX509KeyPair("service.cert", "service.key")
	if err != nil {
		log.Fatalf("Failed to load Access Key and/or Access Certificate: %s", err)
	}

	caCert, err := os.ReadFile("ca.pem")
	if err != nil {
		log.Fatalf("Failed to read CA Certificate file: %s", err)
	}

	caCertPool := x509.NewCertPool()
	ok := caCertPool.AppendCertsFromPEM(caCert)
	if !ok {
		log.Fatalf("Failed to parse CA Certificate file: %s", err)
	}

	dialer := &kafka.Dialer{
		Timeout:   10 * time.Second,
		DualStack: true,
		TLS: &tls.Config{
			Certificates: []tls.Certificate{keypair},
			RootCAs:      caCertPool,
		},
	}

	brokerAddress := "kafka-297becac-sjsu-f7b6.k.aivencloud.com:11921"
	topic := "aneesh_2"
	partition := 0

	reader := kafka.NewReader(kafka.ReaderConfig{
		Brokers:        []string{brokerAddress},
		Topic:          topic,
		Partition:      partition,
		Dialer:         dialer,
		StartOffset:    kafka.LastOffset, 
		GroupID:        "algolia-consumer-group", 
		MinBytes:       10e3, 
		MaxBytes:       10e6, 
		CommitInterval: time.Second, 
	})

	client := search.NewClient(os.Getenv("ALGOLIA_APP_ID"), os.Getenv("ALGOLIA_API_KEY"))
	index := client.InitIndex(os.Getenv("ALGOLIA_INDEX"))

	fmt.Println(client)
	fmt.Println(index)
	fmt.Println(os.Getenv("ALGOLIA_APP_ID"))
	fmt.Println(os.Getenv("ALGOLIA_INDEX"))

	for {
		msg, err := reader.ReadMessage(context.TODO())
		if err != nil {
			log.Fatalf("Failed to read message: %s", err)
		}

		fmt.Println(string(msg.Value))
		var document map[string]interface{}
		err = json.Unmarshal(msg.Value, &document)
		if err != nil {
			log.Printf("Failed to unmarshal Kafka message: %s", err)
			continue
		}

		if _, exists := document["objectID"]; string(msg.Key) == "algolia" && !exists {
			document["objectID"] = fmt.Sprintf("%s-%d", msg.Key, msg.Offset)
		}

		_, err = index.SaveObject(document)
		if err != nil {
			log.Printf("Failed to index document in Algolia: %s", err)
			return err
		}

		log.Printf("Successfully indexed document in Algolia: %v", document)
	}
}
