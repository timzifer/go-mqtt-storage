package storage

import (
	"context"
	"testing"
	"time"

	mqtt "github.com/mochi-mqtt/server/v2"
	"github.com/mochi-mqtt/server/v2/hooks/auth"
	"github.com/mochi-mqtt/server/v2/listeners"
	"github.com/rs/zerolog"
	"github.com/stretchr/testify/require"
	"github.com/timzifer/go-mqtt-storage/serializer"
)

func startBroker(t *testing.T) (string, func()) {
	t.Helper()

	srv := mqtt.New(nil)
	require.NoError(t, srv.AddHook(new(auth.AllowHook), nil))
	tcp := listeners.NewTCP(listeners.Config{ID: "t1", Address: "127.0.0.1:0"})
	require.NoError(t, srv.AddListener(tcp))

	go func() {
		_ = srv.Serve()
	}()

	// Give the listener a moment to bind to an address
	time.Sleep(100 * time.Millisecond)

	addr := "tcp://" + tcp.Address()
	return addr, func() {
		_ = srv.Close()
	}
}

func TestObserveAndSet(t *testing.T) {
	brokerAddr, shutdown := startBroker(t)
	defer shutdown()

	store, err := NewStorage(context.Background(), Config{BrokerURLs: []string{brokerAddr}, Logger: zerolog.Nop()})
	require.NoError(t, err)
	defer store.Close(context.Background())

	require.NoError(t, RegisterSerializer[string](store, &serializer.StringSerializer{}))

	messages := make(chan string, 1)

	err = Observe[string](store, "test/topic", ReceiveFn(func(v Value[string]) error {
		if v.Get() == "hello" {
			return v.Set("world")
		}
		messages <- v.Get()
		return nil
	}))
	require.NoError(t, err)

	// Ensure the subscription is active before publishing.
	time.Sleep(100 * time.Millisecond)

	require.NoError(t, store.Publish(context.Background(), "test/topic", []byte("hello")))

	select {
	case msg := <-messages:
		require.Equal(t, "world", msg)
	case <-time.After(5 * time.Second):
		t.Fatal("did not receive republished value")
	}
}

func TestObserveWildcardKeys(t *testing.T) {
	brokerAddr, shutdown := startBroker(t)
	defer shutdown()

	store, err := NewStorage(context.Background(), Config{BrokerURLs: []string{brokerAddr}, Logger: zerolog.Nop()})
	require.NoError(t, err)
	defer store.Close(context.Background())

	require.NoError(t, RegisterSerializer[string](store, &serializer.StringSerializer{}))

	keysCh := make(chan map[string]string, 1)
	err = Observe[string](store, "home/{room}/temp/+", ReceiveFn(func(v Value[string]) error {
		keysCh <- v.Keys()
		return nil
	}))
	require.NoError(t, err)

	// Ensure the subscription is active before publishing.
	time.Sleep(100 * time.Millisecond)

	require.NoError(t, store.Publish(context.Background(), "home/kitchen/temp/current", []byte("22.5")))

	select {
	case keys := <-keysCh:
		require.Equal(t, "kitchen", keys["room"])
		require.Equal(t, "current", keys["level3"])
	case <-time.After(5 * time.Second):
		t.Fatal("did not receive value with wildcard keys")
	}
}
