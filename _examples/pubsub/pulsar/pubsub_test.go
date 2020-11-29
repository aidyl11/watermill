package pulsar

import (
	"context"

	"encoding/json"
	"log"
	"testing"

	messageW "github.com/ThreeDotsLabs/watermill/message"
	"github.com/ThreeDotsLabs/watermill/message/router/middleware"
	"github.com/ThreeDotsLabs/watermill/message/router/plugin"
)

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
func TestSimulateEvents(t *testing.T) {
	db := connectDB()
	simulateEvents(db)

	t.Logf("simulateEvents Ok\n")

}

func TestSQLPublishSubscribe(t *testing.T) {
	router, err := messageW.NewRouter(messageW.RouterConfig{}, logger)
	if err != nil {
		panic(err)
	}

	router.AddPlugin(plugin.SignalsHandler)
	router.AddMiddleware(middleware.Recoverer)

	db := connectDB()

	subscriber := createSubscriber(db)

	publisher := createPublisher()

	router.AddHandler(
		"mysql-to-pulsar",
		mysqlTable,
		subscriber,
		pulsarTopic,
		publisher,
		func(msg *messageW.Message) ([]*messageW.Message, error) {

			consumedEvent := event{}

			err := json.Unmarshal(msg.Payload, &consumedEvent)
			if err != nil {
				return nil, err
			}

			log.Printf("received event %+v with UUID %s", consumedEvent, msg.UUID)

			return []*messageW.Message{msg}, nil
		},
	)

	go func() {

		<-router.Running()
		simulateEvents(db)

	}()

	if err := router.Run(context.Background()); err != nil {
		panic(err)
	}

}
