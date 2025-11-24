package storage

import (
	"context"
	"errors"
	"fmt"
	"reflect"

	"github.com/timzifer/go-mqtt-storage/serializer"
)

// Serializer is a thin wrapper around the serializer.Serializer interface.
type Serializer[T any] interface {
	serializer.Serializer[T]
}

// Storage defines the behaviour required by the generic helper functions.
type Storage interface {
	RegisterSerializerForType(reflect.Type, any) error
	LookupSerializer(reflect.Type) (any, bool)
	RegisterRawReceiver(topic string, receiver Receiver[any]) error
	Publish(ctx context.Context, topic string, payload []byte) error
	Close(ctx context.Context) error
}

// Value represents a typed message received from the broker.
type Value[T any] interface {
	Key() string
	Get() T
	Set(T) error
	Keys() map[string]string
}

// Receiver describes a callback for typed values.
type Receiver[T any] interface {
	Receive(Value[T]) error
}

type receiver[T any] struct {
	fn func(Value[T]) error
}

// ReceiveFn creates a receiver from a simple function.
func ReceiveFn[T any](fn func(Value[T]) error) Receiver[T] {
	return &receiver[T]{fn: fn}
}

func (r *receiver[T]) Receive(v Value[T]) error {
	return r.fn(v)
}

// RegisterSerializer registers a typed serializer for T on the provided Storage.
func RegisterSerializer[T any](s Storage, ser Serializer[T]) error {
	if s == nil {
		return errors.New("storage is nil")
	}
	if ser == nil {
		return errors.New("serializer is nil")
	}
	typ := reflect.TypeFor[T]()
	return s.RegisterSerializerForType(typ, ser)
}

// Observe installs a typed observer on the given topic.
func Observe[T any](s Storage, topic string, r Receiver[T]) error {
	if s == nil {
		return errors.New("storage is nil")
	}
	if r == nil {
		return errors.New("receiver is nil")
	}

	typ := reflect.TypeFor[T]()
	serAny, ok := s.LookupSerializer(typ)
	if !ok {
		return fmt.Errorf("serializer for %s not registered", typ)
	}
	ser, ok := serAny.(Serializer[T])
	if !ok {
		return fmt.Errorf("serializer for %s has unexpected type %T", typ, serAny)
	}

	rawReceiver := ReceiveFn(func(v Value[any]) error {
		payload, ok := v.Get().([]byte)
		if !ok {
			return fmt.Errorf("expected []byte payload for topic %s", v.Key())
		}

		var typed T
		if err := ser.Unmarshal(payload, &typed); err != nil {
			return fmt.Errorf("unmarshal payload for %s: %w", v.Key(), err)
		}

		tv := &mqttValue[T]{
			key:        v.Key(),
			keys:       v.Keys(),
			serializer: ser,
			storage:    s,
			value:      typed,
		}
		return r.Receive(tv)
	})

	return s.RegisterRawReceiver(topic, rawReceiver)
}
