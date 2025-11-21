package serializer

import (
	"fmt"
	"strconv"
	"strings"

	"github.com/pkg/errors"
	"golang.org/x/exp/constraints"
)

type FloatSerializer[T constraints.Float] struct{}

func (f *FloatSerializer[T]) Marshal(v T) ([]byte, error) {

	switch f := any(v).(type) {
	case float64:
		return []byte(strconv.FormatFloat(f, 'f', -1, 64)), nil
	case float32:
		return []byte(strconv.FormatFloat(float64(f), 'f', -1, 32)), nil
	}

	return nil, errors.New("not an float")
}
func (f *FloatSerializer[T]) Unmarshal(data []byte, v *T) error {

	switch f := any(v).(type) {
	case *float64:
		i, err := strconv.ParseFloat(strings.TrimSpace(string(data)), 64)
		if err != nil {
			return err
		}
		*f = i
		return nil
	case *float32:
		i, err := strconv.ParseFloat(strings.TrimSpace(string(data)), 32)
		if err != nil {
			return err
		}
		*f = float32(i)
		return nil
	}

	return errors.New("not a float pointer")
}
func (f *FloatSerializer[T]) Name() string {
	var t T
	return fmt.Sprintf("float[%T]", t)
}

var _ Serializer[float64] = (*FloatSerializer[float64])(nil)
