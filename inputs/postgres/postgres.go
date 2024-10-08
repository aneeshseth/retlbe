package postgres

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"database/sql"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"retl/inputs/types"
	"time"

	_ "github.com/lib/pq"
	"github.com/segmentio/kafka-go"
)

type Postgres struct {
	Conf *types.ConfigType
}

func (d *Postgres) Run() error {
	fmt.Println("STARTED POSTGRES TO KAFKA")

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

	producer := kafka.NewWriter(kafka.WriterConfig{
		Brokers: []string{brokerAddress},
		Topic:   topic,
		Dialer:  dialer,
	})

	dbURL := os.Getenv("POSTGRES_URL")
	table := os.Getenv("POSTGRES_TABLE")
	filter := os.Getenv("POSTGRES_FILTER")
	fmt.Println(dbURL)
	db, err := sql.Open("postgres", dbURL)
	if err != nil {
        log.Fatalf("Unable to connect to database: %v\n", err)
    }
    defer db.Close()

	query := fmt.Sprintf("SELECT * FROM %s WHERE %s", table, filter)
	rows, err := db.Query(query)
	if err != nil {
		log.Fatalf("Error fetching data: %v\n", err)
	}
	defer rows.Close()

	columns, err := rows.Columns()
	if err != nil {
		log.Fatalf("Error getting columns: %v\n", err)
	}
	fmt.Println("Columns:", columns)

	values := make([]interface{}, len(columns))
	valuePtrs := make([]interface{}, len(columns))

	for rows.Next() {
		for i := range values {
			valuePtrs[i] = &values[i] 
		}

		err := rows.Scan(valuePtrs...)
		if err != nil {
			log.Fatalf("Error scanning row: %v\n", err)
		}

		rowData := make(map[string]interface{})
		for i, col := range columns {
			var v interface{}
			val := values[i]

			if b, ok := val.([]byte); ok {
				v = string(b)
			} else {
				v = val
			}
			rowData[col] = v
		}
		fmt.Println("MARSHALLING HJSON")
		jsonMarshalled, err := json.Marshal(rowData)
		fmt.Println(err)
		fmt.Println(string(jsonMarshalled))
		if err != nil {
			return err
		}
		msg := kafka.Message{
			Key:   []byte(os.Getenv("PIPELINE_NAME")),
			Value: []byte(jsonMarshalled),
		}
		err = producer.WriteMessages(context.TODO(), msg)
		if err != nil {
			log.Fatalf("Failed to write message to Kafka: %s", err)
		}

	}

	if rows.Err() != nil {
		log.Fatalf("Error during row iteration: %v", rows.Err())
	}
	fmt.Println("messages sent")
	return nil
}
