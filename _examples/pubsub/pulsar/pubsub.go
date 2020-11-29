package pulsar

import (
	stdSQL "database/sql"
	"encoding/binary"
	"encoding/csv"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"math"
	PW "watermill-pulsar/pkg/pulsar"

	"os"

	"github.com/ThreeDotsLabs/watermill"
	"github.com/ThreeDotsLabs/watermill-sql/pkg/sql"
	"github.com/ThreeDotsLabs/watermill/message"
	driver "github.com/go-sql-driver/mysql"
)

const connString_parsTime = "testdb:testdb@tcp(127.0.0.1:3306)/dhl2019demo"

var (
	logger      = watermill.NewStdLogger(false, false)
	pulsarTopic = "eventspulsar"
	mysqlTable  = "eventsSQLpulsar"
)

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

	return pub
}

func createPublisher() message.Publisher {

	pub, err := PW.NewPublisher(
		PW.PublisherConfig{

			Brokers: []string{"pulsar://127.0.0.1:6650"},

			Marshaler: PW.DefaultMarshaler{},
		},
		logger,
	)

	if err != nil {
		panic(err)
	}
	return pub
}

type event struct {
	Message message.Message
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

}

type MyData struct {
	Data string //col 5

}

// publishEvent retrieves data from a file in CSV format, and sends them to a DBsql
func monPublishEvent(tx *stdSQL.Tx, filename string, typeData string) error {

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

			switch typeData {

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
				fmt.Println("intdoes not exist.")
			}
		}
	}
	return nil
}
