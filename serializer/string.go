package serializer

type StringSerializer struct{}

func (f *StringSerializer) Marshal(v string) ([]byte, error) {
	return []byte(v), nil
}
func (f *StringSerializer) Unmarshal(data []byte, v *string) error {
	*v = string(data)
	return nil
}
func (f *StringSerializer) Name() string {
	return "string"
}

var _ Serializer[string] = (*StringSerializer)(nil)
