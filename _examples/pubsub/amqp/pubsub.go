// Sources for https://watermill.io/docs/getting-started/
package PublisherRMQ

import (
	stdSQL "database/sql"

	"encoding/binary"
	"encoding/csv"
	"encoding/json"

	"fmt"
	"io"
	"log"
	"math"
	"os"

	driver "github.com/go-sql-driver/mysql"

	"github.com/ThreeDotsLabs/watermill"
	"github.com/ThreeDotsLabs/watermill-amqp/pkg/amqp"
	"github.com/ThreeDotsLabs/watermill-sql/pkg/sql"
	"github.com/ThreeDotsLabs/watermill/message"
)

const connString_parsTime = "testdb:testdb@tcp(127.0.0.1:3306)/dhl2019demo"

var (
	logger     = watermill.NewStdLogger(false, false)
	RMQTopic   = "eventsRMQ"
	mysqlTable = "eventsSqlRMQ"
)

type event struct {
	Message message.Message
}

// struct data ,the file is just an example, can be adapted as needed

type MyData struct {
	Data string //col 1

}

// connectBD For the connection to the MySQL database,connString_parsTime represented the path to the DB

func connectDB() *stdSQL.DB {
	conf := driver.NewConfig()
	conf.DBName = "watermill"
	db, err := stdSQL.Open("mysql", connString_parsTime)
	//defer db.Close()
	if err != nil {
	}
	err = db.Ping()
	if err != nil {
	}
	return db
}

func createSubscriber(db *stdSQL.DB) message.Subscriber {
	pub, err := sql.NewSubscriber(
		db,
		sql.SubscriberConfig{
			SchemaAdapter:    sql.DefaultMySQLSchema{},
			OffsetsAdapter:   sql.DefaultMySQLOffsetsAdapter{},
			InitializeSchema: true,
		},
		logger,
	)
	if err != nil {
		panic(err)
	}
	//pub.Close()
	return pub
}

// amqpURI the path and port for the RabbitMQ connection or brocker
var amqpURI = "amqp://guest:guest@127.0.0.1:5672/"

func createPublisher() message.Publisher {
	amqpConfig := amqp.NewDurableQueueConfig(amqpURI)
	pub, err := amqp.NewPublisher(
		amqpConfig,
		logger,
	)
	if err != nil {
		panic(err)
	}
	//pub.Close()

	return pub
}

func simulateEvents(db *stdSQL.DB) {

	tx, err := db.Begin()
	if err != nil {
		panic(err)
	}

	err = monPublishEvent(tx, "export-data.csv", "int")
	if err != nil {
		rollbackErr := tx.Rollback()
		if rollbackErr != nil {
			panic(rollbackErr)
		}
		panic(err)
	}

	err = tx.Commit()
	if err != nil {
		panic(err)
	}
	//sql.Publisher.Close()
}

//publishEvent retrieves data from a file in CSV format, and sends them to a DBsql

func monPublishEvent(tx *stdSQL.Tx, filename string, data_type string) error {

	pub, err := sql.NewPublisher(tx, sql.PublisherConfig{
		SchemaAdapter: sql.DefaultMySQLSchema{},
	}, logger)
	if err != nil {
		return err
	}
	csvFile, err := os.Open(filename)
	if err == nil {
		fmt.Println(err)
		defer csvFile.Close()
		reader := csv.NewReader(csvFile)
		reader.Comma = ';'
		reader.LazyQuotes = true
		_, _ = reader.Read()
		for {
			line, error := reader.Read()
			if error == io.EOF {
				break
			} else if error != nil {
				log.Fatal(error)
			}
			var sig MyData
			switch data_type {
			case "int":
				sig = MyData{
					Data: line[0],
				}
				msg := message.Message{
					Payload: []byte(sig.Data),
				}
				e := event{
					Message: msg,
				}
				payload, err := json.Marshal(e)
				if err != nil {
					return err
				}
				pub.Publish(mysqlTable, message.NewMessage(
					watermill.NewUUID(),
					payload,
				))
			default:
				fmt.Println("int does not exist.")
			}
		}
	}
	return nil
}
