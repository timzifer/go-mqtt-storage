package serializer

import (
	"encoding/json"
	"fmt"
)

type JsonSerializer[T any] struct{}

// Marshal implementiert Serializer[T] für JSON.
func (js *JsonSerializer[T]) Marshal(v T) ([]byte, error) {
	return json.Marshal(v)
}

// Unmarshal implementiert Serializer[T] für JSON.
func (js *JsonSerializer[T]) Unmarshal(data []byte, target *T) error {
	// Prüfe auf nil-Pointer, um Panics in json.Unmarshal zu vermeiden
	if target == nil {
		return fmt.Errorf("json unmarshal target cannot be nil")
	}
	return json.Unmarshal(data, target)
}

// Name implementiert Serializer[T].
func (js *JsonSerializer[T]) Name() string {
	// Optional: Typnamen hinzufügen für besseres Logging (Go 1.18+)
	// var zero T
	// return fmt.Sprintf("JSON[%s]", reflect.TypeOf(zero).Name())
	return "JSON"
}

// NewJsonSerializer erstellt einen neuen JSON Serializer für den Typ T.
func NewJsonSerializer[T any]() Serializer[T] {
	return &JsonSerializer[T]{}
}
