package snowflake

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

	"github.com/segmentio/kafka-go"
	_ "github.com/snowflakedb/gosnowflake"
)

type Snowflake struct {
    Conf *types.ConfigType
}

var LastRunTime time.Time

func (s *Snowflake) Run() error {
    fmt.Println("IN RUN FUNCTIONNNNN")
    fmt.Println("cd ls")
    dir, err := os.Getwd()
    if err != nil {
        log.Fatal(err)
    }

    entries, err := os.ReadDir(dir)
    if err != nil {
        log.Fatal(err)
    }

    for _, entry := range entries {
        fmt.Println(entry.Name())
    }
    keypair, err := tls.LoadX509KeyPair("service.cert", "service.key")
    if err != nil {
        log.Fatalf("Failed to load Access Key and/or Access Certificate: %s", err)
    }
    fmt.Println("KEYPAIR RESOLVED")
    caCert, err := os.ReadFile("ca.pem")
    if err != nil {
        log.Fatalf("Failed to read CA Certificate file: %s", err)
    }
    fmt.Println("CA CERT RESOLVED")
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
    fmt.Println("KAFKA PRODUCER OK")
    fmt.Println(os.Getenv("SNOWFLAKE_USERNAME"))
    snowflakeURL := fmt.Sprintf("%s:%s@%s-%s/%s?warehouse=%s", os.Getenv("SNOWFLAKE_USERNAME"), os.Getenv("SNOWFLAKE_PASSWORD"), os.Getenv("SNOWFLAKE_ORG"), os.Getenv("SNOWFLAKE_ACC"), os.Getenv("SNOWFLAKE_DB"), os.Getenv("SNOWFLAKE_WH"))
    fmt.Println(snowflakeURL)
    db, err := sql.Open("snowflake", snowflakeURL)
    if err != nil {
        log.Fatal(err)
    }
    fmt.Println("DB")
    fmt.Println(db)
    rows, err := db.Query("SELECT * FROM DB_1.PUBLIC.TABLE_1 LIMIT 10")
    if err != nil {
        log.Fatal(err)
    }
    defer rows.Close()
    fmt.Println("ROWS")
    fmt.Println(rows)
    columns, err := rows.Columns()
    if err != nil {
        log.Fatal(err)
    }

    values := make([]interface{}, len(columns))
    valuePtrs := make([]interface{}, len(columns))
    rowMap := make(map[string]interface{})
    for rows.Next() {
        for i := range values {
            valuePtrs[i] = &values[i]
        }

        err := rows.Scan(valuePtrs...)
        if err != nil {
            log.Fatal(err)
        }
        for i := range columns {
            var v interface{}
            val := values[i]

            b, ok := val.([]byte)
            if ok {
                v = string(b)
            } else {
                v = val
            }
            rowMap[fmt.Sprintf("column_%d", i)] = v
        }
        rowJSON, err := json.Marshal(rowMap)
        fmt.Println(string(rowJSON))
        if err != nil {
            log.Fatalf("Failed to marshal row to JSON: %s", err)
        }

        msg := kafka.Message{
            Key:   []byte(os.Getenv("PIPELINE_NAME")),
            Value: rowJSON, 
        }
        err = producer.WriteMessages(context.TODO(), msg)
        if err != nil {
                log.Fatalf("Failed to write message: %s", err)
        }
    }

    fmt.Println("Message sent successfully")
    return nil
}
