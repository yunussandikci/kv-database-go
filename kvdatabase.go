package kvdatabase

import (
	"encoding/gob"
	"os"
)

type kvDatabase[K comparable, V any] struct {
	cache   *map[K]V
	decoder *gob.Decoder
	encoder *gob.Encoder
}

type KVDatabase[K comparable, V any] interface {
	Get(key K) (V, bool)
	Set(key K, value V)
	Read() error
	Flush()
	Persist() error
}

func New[K comparable, V any](filepath string) (KVDatabase[K, V], error) {
	file, openErr := os.OpenFile(filepath, os.O_RDWR|os.O_CREATE, 0666)
	if openErr != nil {
		panic(openErr)
	}
	instance := &kvDatabase[K, V]{
		decoder: gob.NewDecoder(file),
		encoder: gob.NewEncoder(file),
		cache:   &map[K]V{},
	}
	if readErr := instance.Read(); readErr != nil {
		return nil, readErr
	}
	return instance, nil
}

func (l *kvDatabase[K, V]) Get(key K) (V, bool) {
	value, exist := (*l.cache)[key]
	return value, exist
}

func (l *kvDatabase[K, V]) Set(key K, value V) {
	(*l.cache)[key] = value
}

func (l *kvDatabase[K, V]) Read() error {
	if decodeErr := l.decoder.Decode(&l.cache); decodeErr != nil && decodeErr.Error() != "EOF" {
		return decodeErr
	}
	return nil
}

func (l *kvDatabase[K, V]) Persist() error {
	return l.encoder.Encode(l.cache)
}

func (l *kvDatabase[K, V]) Flush() {
	*l.cache = make(map[K]V)
}
