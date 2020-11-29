// Sources for https://watermill.io/docs/getting-started/
package Publisherkafka

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"testing"
	"time"

	messageW "github.com/ThreeDotsLabs/watermill/message"
	"github.com/ThreeDotsLabs/watermill/message/router/middleware"
	"github.com/ThreeDotsLabs/watermill/message/router/plugin"
)

func TestSimulateEvents(t *testing.T) {
	db := connectDB()
	simulateEvents(db)

	t.Logf("simulateEvents Ok\n")

}
func TestConnectionDB(t *testing.T) {

	connectDB()

	t.Logf("TestConnectionDB Ok\n")

}

func TestCreatePublisher(t *testing.T) {
	createPublisher()

	t.Logf("CreatePublisher Ok\n")

}
func TestCreateSubscriber(t *testing.T) {
	db := connectDB()
	createSubscriber(db)

	t.Logf("createSubscriber Ok\n")

}

func TestCloseSubscriber(t *testing.T) {
	db := connectDB()
	subscriber := createSubscriber(db)

	if err := subscriber.Close(); err != nil {
		t.Error(err)
	}
	t.Logf("Close Subscriber Ok\n")

}
func TestClosPublisher(t *testing.T) {

	publisher := createPublisher()

	if err := publisher.Close(); err != nil {
		t.Error(err)
	}
	t.Logf("Close Publisher  Ok\n")

}

func TestPublishSubscribe(t *testing.T) {
	start := time.Now()
	router, err := messageW.NewRouter(messageW.RouterConfig{}, logger)
	if err != nil {
		panic(err)
	}

	router.AddPlugin(plugin.SignalsHandler)
	router.AddMiddleware(middleware.Recoverer)
	//router.Close()

	db := connectDB()

	subscriber := createSubscriber(db)
	publisher := createPublisher()
	//publisher.Close()

	router.AddHandler(
		"mysql-to-kafka",
		mysqlTable,
		subscriber,
		kafkaTopic,
		publisher,
		func(msg *messageW.Message) ([]*messageW.Message, error) {

			consumedEvent := event{}

			err := json.Unmarshal(msg.Payload, &consumedEvent)
			if err != nil {
				return nil, err
			}

			log.Printf("received event %+v with UUID %s", consumedEvent.Message.Payload, msg.UUID)

			return []*messageW.Message{msg}, nil
		},
	)

	go func() {

		<-router.Running()
		simulateEvents(db)

	}()

	if err := router.Run(context.Background()); err != nil {
		panic(err)
	} else {
		router.Close()
		fmt.Printf("close router \n")
	}

	t.Logf("TestSQLPublishSubscribe Ok\n")

	temps_ex := time.Since(start)
	log.Printf("temps d'execution %s", temps_ex)
}
