package storage

import "reflect"

type Storage interface {
	// ToDo
}

type Value[T any] interface {
	Key() string
	Get() T
	Set(T) error
	Keys() map[string]string
}

// Serializer definiert Methoden zur Konvertierung von Daten des Typs T.
type Serializer[T any] interface {
	Marshal(v T) ([]byte, error)
	Unmarshal(data []byte, target *T) error // Nimmt Pointer auf T
	Name() string                           // Gibt einen Namen zurück (z.B. für Logging)
}

type Receiver[T any] interface {
	Receive(Value[T]) error
}

type receiver[T any] struct {
	fn func(Value[T]) error
}

func ReceiveFn[T any](fn func(Value[T]) error) Receiver[T] {
	return &receiver[T]{fn}
}

func (r *receiver[T]) Receive(v Value[T]) error {
	return r.fn(v)
}

// ---------------------------------------------------------------------------
// GENERISCHE API-FUNKTIONEN
//
// Dies sind die einzigen Funktionen, die du garantierst.
// Der Implementierer kann ein beliebiges Storage-Objekt bauen,
// solange es diese Anforderungen erfüllt.
// ---------------------------------------------------------------------------

// RegisterSerializer registers a typed serializer for T.
//
// The provided `s` must support:
//   - storing the serializer under the reflect.TypeFor[T]()
//   - providing a way for Observe[T] to retrieve this serializer later
//
// It must be possible for the implementer to call something like:
//
//	typ := reflect.TypeFor[T]()
//	s.SetSerializer(typ, serializer)
//
// but the concrete mechanism is fully up to the implementer.
func RegisterSerializer[T any](s Storage, serializer Serializer[T]) error {
	// implementer decides how `s` handles this
	_ = reflect.TypeFor[T]() // hint: type-key needed
	return nil
}

// Observe installs a typed observer on the given topic.
//
// The provided storage instance `s` must support:
//
//   - registering a raw (untyped) receiver: Receiver[any]
//   - invoking that raw receiver when a message arrives
//   - providing the correct Value[any] to the raw receiver
//   - publishing via Value.Set()
//
// Observe[T] will:
//   - look up the serializer for T (the implementer must provide a lookup mechanism)
//   - adapt the typed Receiver[T] into an untyped Receiver[any]
//
// The concrete storage implementation defines HOW these operations work.
func Observe[T any](s Storage, topic string, r Receiver[T]) error {
	// implementer must provide:
	// - a way to lookup Serializer[T]
	// - a way to register a raw Observer
	return nil
}
