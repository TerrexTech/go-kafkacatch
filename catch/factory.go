package catch

import (
	"context"
	"encoding/json"
	"log"
	"sync"
	"time"

	"github.com/TerrexTech/go-kafkautils/kafka"

	"github.com/Shopify/sarama"
	"github.com/TerrexTech/go-eventstore-models/model"
	"github.com/TerrexTech/uuuid"
	"github.com/pkg/errors"
)

// Config defines the configuration for Catch-Factory.
type Config struct {
	PreRequest   bool
	KafkaBrokers []string
	TimeoutSec   int
}

type consCache struct {
	store         map[string]*consumer
	consWriteLock map[string]*sync.RWMutex
	consWriteCtrl map[string]bool
}

// Factory handles creating appropriate Kafka-Consumers
// and creating a result-channel for every request.
// Check Factory.Catch function.
type Factory struct {
	ctx         context.Context
	catchConfig Config
	cache       *consCache

	uuidMap       map[string]map[string]chan *model.KafkaResponse
	uuidMapLock   *sync.RWMutex
	topicMapLocks map[string]*sync.RWMutex
}

// NewFactory returns a new Factory instance.
func NewFactory(ctx context.Context, config Config) (*Factory, error) {
	if ctx == nil {
		return nil, errors.New("ctx is required")
	}
	if config.KafkaBrokers == nil || len(config.KafkaBrokers) == 0 {
		err := errors.New("KafkaBrokers in Config are required")
		return nil, err
	}
	if config.TimeoutSec == 0 {
		err := errors.New("TimeoutSec in Config are required")
		return nil, err
	}

	cc := &consCache{
		store:         map[string]*consumer{},
		consWriteLock: map[string]*sync.RWMutex{},
		consWriteCtrl: map[string]bool{},
	}
	return &Factory{
		ctx:         ctx,
		catchConfig: config,
		cache:       cc,

		uuidMap:       map[string]map[string]chan *model.KafkaResponse{},
		uuidMapLock:   &sync.RWMutex{},
		topicMapLocks: map[string]*sync.RWMutex{},
	}, nil
}

// Catch creates a new Kafka-Consumer if one does not exist for that specific ID.
// Otherwise an existing cached Consumer is returned.
// ID here refers to "Group+Topic" for the Consumer.
// This is not analogous to traditional Consumers that simply forward any Kafka Messages.
// This returns a new channel for every request. The request's response is then sent on that
// channel when the response arrives. It determines the correct response for request by using
// the response-UUID. If a response arrives before the request for it is registered, it
// caches the response until the request for it is received, and the response is sent out
// as soon request arrives.
func (f *Factory) Catch(
	group string,
	topic string,
	uuid uuuid.UUID,
) (<-chan *model.KafkaResponse, error) {
	if group == "" {
		return nil, errors.New("group is required")
	}
	if topic == "" {
		return nil, errors.New("topic is required")
	}
	if uuid == (uuuid.UUID{}) {
		return nil, errors.New("uuid is required")
	}

	// Update UUIDs for that specific topic
	f.uuidMapLock.RLock()
	topicCIDMap := f.uuidMap[topic]
	f.uuidMapLock.RUnlock()

	if f.topicMapLocks[topic] == nil {
		f.topicMapLocks[topic] = &sync.RWMutex{}
	}

	if topicCIDMap == nil {
		f.uuidMapLock.Lock()
		f.uuidMap[topic] = map[string]chan *model.KafkaResponse{}
		f.uuidMapLock.Unlock()

		f.uuidMapLock.RLock()
		topicCIDMap = f.uuidMap[topic]
		f.uuidMapLock.RUnlock()
	}

	err := f.createConsumer(group, topic, uuid)
	if err != nil {
		err = errors.Wrap(err, "Error creating ConsumerGroup")
		return nil, err
	}

	tl := f.topicMapLocks[topic]

	uuidStr := uuid.String()
	tl.RLock()
	topicCID := topicCIDMap[uuidStr]
	tl.RUnlock()
	if topicCID == nil {
		// 1 buffer because channel might be read later after its written
		readChan := make(chan *model.KafkaResponse, 1)
		tl.Lock()
		topicCIDMap[uuidStr] = readChan
		tl.Unlock()

		tl.RLock()
		topicCID = topicCIDMap[uuidStr]
		tl.RUnlock()

		if !f.catchConfig.PreRequest {
			go func(uuidStr string) {
				seconds := time.Duration(f.catchConfig.TimeoutSec)
				<-time.After(seconds * time.Second)
				tl.RLock()
				krChan := topicCIDMap[uuidStr]
				tl.RUnlock()

				if krChan != nil {
					krChan <- nil
					tl.Lock()
					delete(topicCIDMap, uuidStr)
					tl.Unlock()
				}
			}(uuidStr)
		}
	}

	return (<-chan *model.KafkaResponse)(topicCID), nil
}

func (f *Factory) createConsumer(
	group string,
	topic string,
	uuid uuuid.UUID,
) error {
	store := f.cache.store
	id := group + topic

	cacheLock := f.cache.consWriteLock[id]
	if cacheLock == nil {
		cacheLock = &sync.RWMutex{}
	}
	cacheLock.Lock()
	consWriteCtrl := f.cache.consWriteCtrl
	if !consWriteCtrl[id] {
		consWriteCtrl[id] = true
	}
	cacheLock.Unlock()

	if store[id] == nil {
		// Create Kafka Aggregate-Response Consumer
		cons, err := newConsumer(f.ctx, &kafka.ConsumerConfig{
			GroupName:    group,
			KafkaBrokers: f.catchConfig.KafkaBrokers,
			Topics:       []string{topic},
		})
		if err != nil {
			err = errors.Wrap(err, "CreateConsumer")
			return err
		}
		store[id] = cons

		// Handle Messages
		go func() {
		msgLoop:
			for {
				select {
				case <-f.ctx.Done():
					log.Printf("KafkaCatch: Closing Consumer-loop on Topic: %s", topic)
					err := cons.Close()
					if err != nil {
						err = errors.Wrap(err, "KafkaCatch: Error closing Consumer")
						log.Println(err)
					}
					break msgLoop
				case msg := <-cons.Messages():
					go f.handleKafkaConsumerMsg(msg)
				}
			}
		}()

		// Handle Errors
		go func() {
		errLoop:
			for {
				select {
				case <-f.ctx.Done():
					log.Printf("KafkaCatch: Closing Consumer Error-loop on Topic: %s", topic)
					break errLoop

				case err := <-cons.Errors():
					if err != nil {
						err = errors.Wrap(err, "KafkaCatch: Consumer Error")
						log.Println(err)
						// Allow consumer to re-initialize
						cacheLock.Lock()
						if !consWriteCtrl[id] {
							store[id] = nil
						}
						cacheLock.Unlock()
					}
				}
			}
		}()
	}

	return nil
}

// handleKafkaConsumerMsg handles the messages from consumer. It checks if any
// corresponding UUID exists for that response, and then passes the response to
// the channel associated with that UUID.
// If no UUID exists at time of message-arrival, and PreRequest is enabled,
// the message is cached until the matching request is received, and is then sent.
// Once the response is sent, the corresponding UUID-entry is deleted from map.
func (f *Factory) handleKafkaConsumerMsg(msg *sarama.ConsumerMessage) {
	if msg == nil {
		return
	}
	kr := &model.KafkaResponse{}
	err := json.Unmarshal(msg.Value, kr)
	if err != nil {
		err = errors.Wrap(err, "Error Unmarshalling Kafka-Message into Kafka-Response")
		log.Println(err)
		return
	}
	uuid := kr.UUID.String()

	f.uuidMapLock.RLock()
	topicCIDMap := f.uuidMap[msg.Topic]
	f.uuidMapLock.RUnlock()

	// Create topic-entry in CID map since it doesn't exist
	if topicCIDMap == nil {
		f.uuidMapLock.Lock()
		f.uuidMap[msg.Topic] = make(map[string]chan *model.KafkaResponse)
		f.uuidMapLock.Unlock()

		f.uuidMapLock.RLock()
		topicCIDMap = f.uuidMap[msg.Topic]
		f.uuidMapLock.RUnlock()
	}

	topicMapLock := f.topicMapLocks[msg.Topic]
	// Get the associated channel from TopicCIDMap
	topicMapLock.RLock()
	exists := topicCIDMap[uuid] != nil
	topicMapLock.RUnlock()

	// Send the response to the channel from above and delete the entry from map
	if exists {
		topicMapLock.RLock()
		krChan := topicCIDMap[uuid]
		topicMapLock.RUnlock()

		krChan <- kr
		topicMapLock.Lock()
		delete(topicCIDMap, uuid)
		topicMapLock.Unlock()
		return
	}

	// If no corresponding UUID was found, and PreRequest is enabled,
	// we cache the response until a CID requesting that response is available.
	if f.catchConfig.PreRequest {
		readChan := make(chan *model.KafkaResponse, 1)
		topicMapLock.Lock()
		topicCIDMap[uuid] = readChan
		topicMapLock.Unlock()
		readChan <- kr
	}
}
