package serializer

// Serializer definiert Methoden zur Konvertierung von Daten des Typs T.
type Serializer[T any] interface {
	Marshal(v T) ([]byte, error)
	Unmarshal(data []byte, target *T) error // Nimmt Pointer auf T
	Name() string                           // Gibt einen Namen zurück (z.B. für Logging)
}
