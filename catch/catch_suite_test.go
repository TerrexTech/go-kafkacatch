package catch

import (
	"context"
	"encoding/json"
	"log"
	"os"
	"testing"
	"time"

	"github.com/TerrexTech/go-eventstore-models/model"

	"github.com/TerrexTech/go-kafkautils/kafka"

	"github.com/TerrexTech/uuuid"

	"github.com/TerrexTech/go-commonutils/commonutil"
	"github.com/joho/godotenv"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	"github.com/pkg/errors"
)

func TestCatch(t *testing.T) {
	log.Println("Reading environment file")
	err := godotenv.Load("../test.env")
	if err != nil {
		err = errors.Wrap(err,
			".env file not found, env-vars will be read as set in environment",
		)
		log.Println(err)
	}

	missingVar, err := commonutil.ValidateEnv(
		"KAFKA_BROKERS",
		"KAFKA_CONSUMER_GROUP",
		"KAFKA_CONSUMER_TOPIC",
	)

	if err != nil {
		err = errors.Wrapf(
			err,
			"Env-var %s is required for testing, but is not set", missingVar,
		)
		log.Fatalln(err)
	}

	RegisterFailHandler(Fail)
	RunSpecs(t, "KafkaCatch Suite")
}

var _ = Describe("KafkaCatch", func() {
	var (
		kafkaBrokers []string
		group        string
		topic        string

		producer *kafka.Producer
	)

	BeforeSuite(func() {
		kafkaBrokersStr := os.Getenv("KAFKA_BROKERS")
		kafkaBrokers = *commonutil.ParseHosts(kafkaBrokersStr)
		group = os.Getenv("KAFKA_CONSUMER_GROUP")
		topic = os.Getenv("KAFKA_CONSUMER_TOPIC")

		var err error
		producer, err = kafka.NewProducer(&kafka.ProducerConfig{
			KafkaBrokers: kafkaBrokers,
		})
		Expect(err).ToNot(HaveOccurred())
	})

	Describe("test pre-request enabled", func() {
		It("should cache the response and provide it when requested", func(done Done) {
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			f, err := NewFactory(ctx, Config{
				PreRequest:   true,
				KafkaBrokers: kafkaBrokers,
				TimeoutSec:   5,
			})
			Expect(err).ToNot(HaveOccurred())

			uuid1, err := uuuid.NewV4()
			Expect(err).ToNot(HaveOccurred())
			uuid2, err := uuuid.NewV4()
			Expect(err).ToNot(HaveOccurred())
			uuid3, err := uuuid.NewV4()
			Expect(err).ToNot(HaveOccurred())

			kr1 := model.KafkaResponse{UUID: uuid1}
			kr2 := model.KafkaResponse{UUID: uuid2}
			kr3 := model.KafkaResponse{UUID: uuid3}

			mkr1, err := json.Marshal(kr1)
			Expect(err).ToNot(HaveOccurred())
			mkr2, err := json.Marshal(kr2)
			Expect(err).ToNot(HaveOccurred())
			mkr3, err := json.Marshal(kr3)
			Expect(err).ToNot(HaveOccurred())

			producer.Input() <- kafka.CreateMessage(topic, mkr1)
			producer.Input() <- kafka.CreateMessage(topic, mkr2)
			producer.Input() <- kafka.CreateMessage(topic, mkr3)

			krChan1, err := f.Catch(group, topic, uuid1)
			Expect(err).ToNot(HaveOccurred())
			krChan2, err := f.Catch(group, topic, uuid2)
			Expect(err).ToNot(HaveOccurred())
			krChan3, err := f.Catch(group, topic, uuid3)
			Expect(err).ToNot(HaveOccurred())

			<-krChan1
			<-krChan2
			<-krChan3
			close(done)
		}, 15)
	})

	Describe("test pre-request disabled", func() {
		It("should return nil if response is not already present", func() {
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			f, err := NewFactory(ctx, Config{
				PreRequest:   false,
				KafkaBrokers: kafkaBrokers,
				TimeoutSec:   5,
			})
			Expect(err).ToNot(HaveOccurred())

			uuid1, err := uuuid.NewV4()
			Expect(err).ToNot(HaveOccurred())
			uuid2, err := uuuid.NewV4()
			Expect(err).ToNot(HaveOccurred())
			uuid3, err := uuuid.NewV4()
			Expect(err).ToNot(HaveOccurred())

			krChan1, err := f.Catch(group, topic, uuid1)
			Expect(err).ToNot(HaveOccurred())
			krChan2, err := f.Catch(group, topic, uuid2)
			Expect(err).ToNot(HaveOccurred())
			krChan3, err := f.Catch(group, topic, uuid3)
			Expect(err).ToNot(HaveOccurred())

			Expect(<-krChan1).To(BeNil())
			Expect(<-krChan2).To(BeNil())
			Expect(<-krChan3).To(BeNil())
		})

		It("should return response if its present", func(done Done) {
			ctx, cancel := context.WithCancel(context.Background())
			f, err := NewFactory(ctx, Config{
				PreRequest:   false,
				KafkaBrokers: kafkaBrokers,
				TimeoutSec:   10,
			})
			Expect(err).ToNot(HaveOccurred())

			uuid1, err := uuuid.NewV4()
			Expect(err).ToNot(HaveOccurred())
			uuid2, err := uuuid.NewV4()
			Expect(err).ToNot(HaveOccurred())
			uuid3, err := uuuid.NewV4()
			Expect(err).ToNot(HaveOccurred())

			kr1 := model.KafkaResponse{UUID: uuid1}
			kr2 := model.KafkaResponse{UUID: uuid2}
			kr3 := model.KafkaResponse{UUID: uuid3}

			mkr1, err := json.Marshal(kr1)
			Expect(err).ToNot(HaveOccurred())
			mkr2, err := json.Marshal(kr2)
			Expect(err).ToNot(HaveOccurred())
			mkr3, err := json.Marshal(kr3)
			Expect(err).ToNot(HaveOccurred())

			krChan1, err := f.Catch(group, topic, uuid1)
			Expect(err).ToNot(HaveOccurred())
			krChan2, err := f.Catch(group, topic, uuid2)
			Expect(err).ToNot(HaveOccurred())
			krChan3, err := f.Catch(group, topic, uuid3)
			Expect(err).ToNot(HaveOccurred())

			go func() {
				defer GinkgoRecover()
				kr1 := <-krChan1
				kr2 := <-krChan2
				kr3 := <-krChan3
				cancel()
				Expect(kr1).ToNot(BeNil())
				Expect(kr2).ToNot(BeNil())
				Expect(kr3).ToNot(BeNil())
				Expect(kr1.UUID).To(Equal(uuid1))
				Expect(kr2.UUID).To(Equal(uuid2))
				Expect(kr3.UUID).To(Equal(uuid3))
				close(done)
			}()

			<-time.After(3 * time.Second)
			producer.Input() <- kafka.CreateMessage(topic, mkr1)
			producer.Input() <- kafka.CreateMessage(topic, mkr2)
			producer.Input() <- kafka.CreateMessage(topic, mkr3)
		}, 15)
	})

	Describe("testValidation", func() {
		It("should return error when empty arguments are passed to NewFactory", func() {
			_, err := NewFactory(nil, Config{})
			Expect(err).To(HaveOccurred())

			_, err = NewFactory(context.Background(), Config{})
			Expect(err).To(HaveOccurred())
		})

		It("should return error when empty arguments are passed to Catch", func() {
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			f, err := NewFactory(ctx, Config{
				PreRequest:   false,
				KafkaBrokers: kafkaBrokers,
				TimeoutSec:   10,
			})
			Expect(err).ToNot(HaveOccurred())

			_, err = f.Catch("", "test", uuuid.UUID{})
			Expect(err).To(HaveOccurred())

			_, err = f.Catch("test", "", uuuid.UUID{})
			Expect(err).To(HaveOccurred())

			_, err = f.Catch("test", "test", uuuid.UUID{})
			Expect(err).To(HaveOccurred())

		})
	})
})
