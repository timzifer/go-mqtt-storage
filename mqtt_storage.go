package storage

import (
	"context"
	"fmt"
	"net/url"
	"reflect"
	"strings"
	"sync"
	"time"

	autopaho "github.com/eclipse/paho.golang/autopaho"
	"github.com/eclipse/paho.golang/paho"
	"github.com/rs/zerolog"
)

// Config configures the MQTT storage instance.
type Config struct {
	BrokerURLs        []string
	ClientID          string
	SubscribeQoS      byte
	PublishQoS        byte
	KeepAlive         time.Duration
	ConnectRetryDelay time.Duration
	Logger            zerolog.Logger
}

// MQTTStorage implements Storage using an autopaho connection manager.
type MQTTStorage struct {
	cm     *autopaho.ConnectionManager
	log    zerolog.Logger
	subQoS byte
	pubQoS byte

	mu          sync.RWMutex
	serializers map[reflect.Type]any
	receivers   map[string][]Receiver[any]
	ctx         context.Context
	cancel      context.CancelFunc
}

// NewStorage creates a new MQTTStorage and starts the connection handling.
func NewStorage(ctx context.Context, cfg Config) (*MQTTStorage, error) {
	if len(cfg.BrokerURLs) == 0 {
		return nil, fmt.Errorf("no broker URLs configured")
	}
	if cfg.ClientID == "" {
		cfg.ClientID = "go-mqtt-storage"
	}
	if cfg.KeepAlive == 0 {
		cfg.KeepAlive = 30 * time.Second
	}
	if cfg.ConnectRetryDelay == 0 {
		cfg.ConnectRetryDelay = 5 * time.Second
	}
	if cfg.SubscribeQoS == 0 {
		cfg.SubscribeQoS = 1
	}
	if cfg.PublishQoS == 0 {
		cfg.PublishQoS = 1
	}
	log := cfg.Logger
	if log.GetLevel() == zerolog.NoLevel {
		log = zerolog.Nop()
	}

	u := make([]*url.URL, 0, len(cfg.BrokerURLs))
	for _, raw := range cfg.BrokerURLs {
		parsed, err := url.Parse(raw)
		if err != nil {
			return nil, fmt.Errorf("parse broker url %q: %w", raw, err)
		}
		u = append(u, parsed)
	}

	baseCtx, cancel := context.WithCancel(ctx)
	s := &MQTTStorage{
		log:         log,
		subQoS:      cfg.SubscribeQoS,
		pubQoS:      cfg.PublishQoS,
		serializers: make(map[reflect.Type]any),
		receivers:   make(map[string][]Receiver[any]),
		ctx:         baseCtx,
		cancel:      cancel,
	}

	clientCfg := autopaho.ClientConfig{
		BrokerUrls:        u,
		KeepAlive:         uint16(cfg.KeepAlive / time.Second),
		ConnectRetryDelay: cfg.ConnectRetryDelay,
		OnConnectionUp: func(cm *autopaho.ConnectionManager, _ *paho.Connack) {
			s.log.Info().Msg("mqtt connection established")
			if err := s.resubscribe(baseCtx); err != nil {
				s.log.Error().Err(err).Msg("resubscribe failed")
			}
		},
		OnConnectError: func(err error) {
			s.log.Error().Err(err).Msg("mqtt connection failed")
		},
		ClientConfig: paho.ClientConfig{
			ClientID: cfg.ClientID,
			OnPublishReceived: []func(paho.PublishReceived) (bool, error){
				func(pr paho.PublishReceived) (bool, error) {
					s.handleMessage(pr)
					return true, nil
				},
			},
		},
	}

	cm, err := autopaho.NewConnection(baseCtx, clientCfg)
	if err != nil {
		cancel()
		return nil, fmt.Errorf("create autopaho connection: %w", err)
	}
	s.cm = cm

	waitCtx, cancelWait := context.WithTimeout(baseCtx, 10*time.Second)
	defer cancelWait()
	if err := cm.AwaitConnection(waitCtx); err != nil {
		cancel()
		return nil, fmt.Errorf("initial mqtt connection: %w", err)
	}

	return s, nil
}

// Close closes the MQTT connection.
func (s *MQTTStorage) Close(ctx context.Context) error {
	s.cancel()
	if ctx == nil {
		ctx = context.Background()
	}
	return s.cm.Disconnect(ctx)
}

// RegisterSerializerForType stores the serializer for the given reflect.Type.
func (s *MQTTStorage) RegisterSerializerForType(t reflect.Type, serializer any) error {
	if t == nil {
		return fmt.Errorf("type cannot be nil")
	}
	if serializer == nil {
		return fmt.Errorf("serializer cannot be nil")
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.serializers[t] = serializer
	return nil
}

// LookupSerializer retrieves a serializer for the given reflect.Type.
func (s *MQTTStorage) LookupSerializer(t reflect.Type) (any, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	v, ok := s.serializers[t]
	return v, ok
}

// RegisterRawReceiver registers a raw receiver for the given topic and subscribes at the broker.
func (s *MQTTStorage) RegisterRawReceiver(topic string, receiver Receiver[any]) error {
	if receiver == nil {
		return fmt.Errorf("receiver cannot be nil")
	}

	s.mu.Lock()
	s.receivers[topic] = append(s.receivers[topic], receiver)
	s.mu.Unlock()

	return s.subscribe(s.ctx, normalizeTopicFilter(topic))
}

// Publish sends a payload to the broker.
func (s *MQTTStorage) Publish(ctx context.Context, topic string, payload []byte) error {
	if ctx == nil {
		ctx = s.ctx
	}
	_, err := s.cm.Publish(ctx, &paho.Publish{
		Topic:   topic,
		Payload: payload,
		QoS:     s.pubQoS,
	})
	return err
}

func (s *MQTTStorage) resubscribe(ctx context.Context) error {
	s.mu.RLock()
	defer s.mu.RUnlock()
	for topic := range s.receivers {
		if err := s.subscribe(ctx, normalizeTopicFilter(topic)); err != nil {
			return err
		}
	}
	return nil
}

func (s *MQTTStorage) subscribe(ctx context.Context, topic string) error {
	if ctx == nil {
		ctx = s.ctx
	}
	if s.cm == nil {
		return fmt.Errorf("connection manager not initialised")
	}
	_, err := s.cm.Subscribe(ctx, &paho.Subscribe{
		Subscriptions: []paho.SubscribeOptions{{Topic: topic, QoS: s.subQoS}},
	})
	return err
}

func (s *MQTTStorage) handleMessage(pr paho.PublishReceived) {
	msg := pr.Packet
	if msg == nil {
		return
	}
	topic := msg.Topic
	payload := msg.Payload

	s.mu.RLock()
	matched := make([]struct {
		pattern  string
		receiver Receiver[any]
	}, 0)
	for pattern, subs := range s.receivers {
		if matchTopic(pattern, topic) {
			for _, sub := range subs {
				matched = append(matched, struct {
					pattern  string
					receiver Receiver[any]
				}{pattern: pattern, receiver: sub})
			}
		}
	}
	s.mu.RUnlock()

	if len(matched) == 0 {
		return
	}

	for _, entry := range matched {
		rawVal := &mqttValue[any]{
			key:        topic,
			keys:       extractKeys(topic, entry.pattern),
			serializer: nil,
			storage:    s,
			value:      payload,
		}

		if err := entry.receiver.Receive(rawVal); err != nil {
			s.log.Error().Err(err).Str("topic", topic).Msg("receiver error")
		}
	}
}

// mqttValue is a concrete implementation of Value.
type mqttValue[T any] struct {
	key        string
	keys       map[string]string
	value      T
	serializer Serializer[T]
	storage    Storage
}

func (v *mqttValue[T]) Key() string { return v.key }

func (v *mqttValue[T]) Get() T { return v.value }

func (v *mqttValue[T]) Keys() map[string]string {
	out := make(map[string]string, len(v.keys))
	for k, val := range v.keys {
		out[k] = val
	}
	return out
}

func (v *mqttValue[T]) Set(val T) error {
	if v.serializer == nil {
		return fmt.Errorf("no serializer available for %s", v.key)
	}
	data, err := v.serializer.Marshal(val)
	if err != nil {
		return err
	}
	return v.storage.Publish(context.Background(), v.key, data)
}

func matchTopic(pattern, topic string) bool {
	pParts := strings.Split(pattern, "/")
	tParts := strings.Split(topic, "/")

	for i, p := range pParts {
		if p == "#" {
			return true
		}
		if i >= len(tParts) {
			return false
		}
		switch {
		case p == "+":
			continue
		default:
			if p != tParts[i] && !isPlaceholder(p) {
				return false
			}
		}
	}
	return len(pParts) == len(tParts) || (len(pParts) > 0 && pParts[len(pParts)-1] == "#")
}

func normalizeTopicFilter(topic string) string {
	parts := strings.Split(topic, "/")
	for i, part := range parts {
		if isPlaceholder(part) {
			parts[i] = "+"
		}
	}
	return strings.Join(parts, "/")
}

func extractKeys(topic, pattern string) map[string]string {
	keys := make(map[string]string)
	pParts := strings.Split(pattern, "/")
	tParts := strings.Split(topic, "/")

	for i, p := range pParts {
		if i >= len(tParts) {
			break
		}
		switch {
		case p == "#":
			keys[fmt.Sprintf("level%d", i)] = strings.Join(tParts[i:], "/")
			return keys
		case p == "+":
			keys[fmt.Sprintf("level%d", i)] = tParts[i]
		case isPlaceholder(p):
			name := strings.Trim(p, "{}")
			keys[name] = tParts[i]
		}
	}
	return keys
}

func isPlaceholder(segment string) bool {
	return strings.HasPrefix(segment, "{") && strings.HasSuffix(segment, "}") && len(segment) > 2
}
